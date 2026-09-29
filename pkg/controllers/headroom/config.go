/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package headroom

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

const (
	// defaultPodCPU / defaultPodMemory size one headroom pod when the pool entry does not
	// set podCPU / podMemory. The buffer is denominated in these absolute per-pod slices,
	// so preempting one pod frees a predictable amount of capacity.
	defaultPodCPU    = "500m"
	defaultPodMemory = "512Mi"

	// maxBufferPercent bounds every buffer fraction. The buffer is a fraction f of the pool's
	// TOTAL allocatable, and every node also carries a DaemonSet/system share d, so the pool
	// only settles once N >= workload / (C × (1 - f - d)): with f >= 1 - d there is no fixed
	// point at all (each new node adds as much pause-pod demand as capacity, one pod is always
	// Pending, and Karpenter provisions until the NodePool limits stop it), and the effective
	// buffer relative to the real workload is f/(1-f-d) in any case. 50% keeps a fixed point for
	// any realistic overhead and still allows a buffer larger than the workload itself.
	maxBufferPercent = 50.0

	// minPodCPU / minPodMemory are the smallest per-pod slices we accept. The per-pod size is
	// the divisor of the replica math, so a typo that makes it absurdly small (podCPU: 100u)
	// would blow the replica count up by orders of magnitude; reject it at parse time rather
	// than let the reconciler clamp it.
	minPodCPU    = "1m"
	minPodMemory = "1Mi"
)

var (
	// wellKnownKeys are the ConfigMap data keys scanned first, in this order.
	wellKnownKeys = []string{"policy", "config"}

	minPodCPUQuantity    = resource.MustParse(minPodCPU)
	minPodMemoryQuantity = resource.MustParse(minPodMemory)
)

// tolerationYAML is one toleration entry in the ConfigMap (corev1.Toleration-shaped).
type tolerationYAML struct {
	Key      string `json:"key"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
	Effect   string `json:"effect"`
}

// poolPolicyYAML is the raw YAML structure for one pool entry in the ConfigMap.
type poolPolicyYAML struct {
	Name      string `json:"name"`
	CPU       string `json:"cpu"`
	Memory    string `json:"memory"`
	PodCPU    string `json:"podCPU"`
	PodMemory string `json:"podMemory"`

	// GPU / PodGPU configure a GPU buffer. IMPORTANT LIMITATION: GPU headroom reserves
	// capacity on the pool's EXISTING GPU nodes only — it cannot cold-start a GPU pool.
	// The GPU resource name is derived from the GPU nodes the pool already has; on a pool
	// with no GPU nodes the podGPU request is dropped (see perPodRequests) rather than
	// guessed, which would only park a pause pod in Pending forever — and on an amd.com/gpu
	// pool the guess would be wrong outright.
	//
	// Instance types do now advertise a TEMPORARY hardcoded nvidia.com/gpu capacity
	// (v1alpha1.InstanceTypeSpec.GPU), so Karpenter can provision for a pending GPU pod; this
	// controller is deliberately unchanged. Once that field is replaced by real per-SKU
	// accelerator capacity, sourcing the key from the instance type instead of from existing
	// nodes would let a GPU buffer drive scale-out too.
	GPU    string `json:"gpu"`
	PodGPU string `json:"podGPU"`

	MinPods int `json:"minPods"`
	// MaxPods optionally caps the replica count. The buffer is sized from total pool
	// allocatable, which includes the nodes the buffer itself caused to be provisioned, so
	// without a ceiling a large pool carries a proportionally large buffer; maxPods bounds it
	// absolutely (0 / unset = no ceiling beyond maxHeadroomReplicas).
	MaxPods     int              `json:"maxPods"`
	Tolerations []tolerationYAML `json:"tolerations"`
}

// headroomPolicyYAML is the top-level structure expected in the ConfigMap data value.
// Other top-level keys (e.g. "name", "comfigmap") are ignored.
type headroomPolicyYAML struct {
	Pools []poolPolicyYAML `json:"pools"`
}

// parsedPoolPolicy holds one pool's policy after parsing: buffer fractions (0.0–1.0),
// absolute per-pod sizes, the cold-start replica floor, and scheduling tolerations.
type parsedPoolPolicy struct {
	Name   string
	CPU    float64
	Memory float64
	GPU    float64

	// PodCPU/PodMemory are always non-zero (defaults applied); PodGPU is zero unless set.
	PodCPU    resource.Quantity
	PodMemory resource.Quantity
	PodGPU    resource.Quantity

	// MinPods is the replica floor even when the pool has no ready nodes (cold start).
	MinPods int
	// MaxPods is the replica ceiling; 0 means none (beyond maxHeadroomReplicas). When set it
	// is >= MinPods (validated at parse time).
	MaxPods int

	Tolerations []corev1.Toleration
}

// parseHeadroomConfig extracts the pool policies from a headroom-policy ConfigMap.
// It scans the data keys in a deterministic order (candidateKeys) and returns the pools of
// the first key that yields a non-empty list.
//
// The error contract matters, because Reconcile garbage-collects the headroom Deployments of
// every pool that is NOT in the returned list:
//   - ConfigMap present, parses cleanly, declares no pools → (nil, nil). A legitimate "headroom
//     off" policy; the caller tears the buffer Deployments down.
//   - ConfigMap present but NO key yields pools AND at least one candidate key failed to parse
//     → (nil, error). A YAML typo must NOT read as "no pools configured", or a single bad
//     character would GC every headroom Deployment in the cluster (fail-open). Reconcile
//     short-circuits on this error before any cleanup runs.
//   - A key that is not one of the well-known ones and does not hold a YAML mapping (free text
//     such as a README or maintenance note, a bare bool or number) can never declare pools, so
//     it is not a candidate at all: it is skipped, never an error. Otherwise an unrelated key
//     sharing the ConfigMap would make the legitimate `pools: []` unreachable. The well-known
//     keys stay fail-closed whatever they hold.
//   - A fallback key that is not even well-formed YAML (a note with colons, `Owner: x, reason: y`)
//     cannot be told from a typo by the parser. It is fail-closed on its own, but once a
//     well-known key has parsed cleanly it cannot legitimately be the policy, so it is skipped
//     with a warning instead: failing on it would leave a real `pools: []` unreachable, and the
//     buffer Deployments up, for as long as the note exists. A parse error on a well-known key
//     itself always fails closed.
func parseHeadroomConfig(cm *corev1.ConfigMap) ([]parsedPoolPolicy, error) {
	var (
		pools   []parsedPoolPolicy
		fromKey string
		errs    []error
		// skipped are the fallback keys that failed to parse; cleanKey is the first well-known key
		// that parsed cleanly (pools or not) and wellKnownErr whether any well-known key failed.
		skipped      []string
		cleanKey     string
		wellKnownErr bool
	)
	for _, key := range candidateKeys(cm.Data) {
		wellKnown := isWellKnownKey(key)
		if !wellKnown && isNonMappingYAML(cm.Data[key]) {
			klog.V(2).Infof("headroom: configmap %s/%s key %q is not a YAML mapping; ignoring it", cm.Namespace, cm.Name, key)
			continue
		}
		got, err := unmarshalPools(cm.Data[key])
		if err != nil {
			klog.Warningf("headroom: configmap %s/%s key %q: %v", cm.Namespace, cm.Name, key, err)
			errs = append(errs, fmt.Errorf("key %q: %w", key, err))
			if wellKnown {
				wellKnownErr = true
			} else {
				skipped = append(skipped, key)
			}
			continue
		}
		if wellKnown && cleanKey == "" {
			cleanKey = key
		}
		if len(got) == 0 {
			continue
		}
		if pools == nil {
			pools, fromKey = got, key
			continue
		}
		// Two keys declaring pools is almost always a copy/paste accident; say which one won
		// so the silently-ignored one is visible.
		klog.Warningf("headroom: configmap %s/%s key %q also declares pools; ignoring it (key %q wins)",
			cm.Namespace, cm.Name, key, fromKey)
	}
	if pools != nil {
		return pools, nil
	}
	if len(errs) > 0 {
		if cleanKey != "" && !wellKnownErr {
			for _, key := range skipped {
				klog.Warningf("headroom: configmap %s/%s key %q is not valid YAML; ignoring it because %q parsed cleanly",
					cm.Namespace, cm.Name, key, cleanKey)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("configmap %s/%s: no data key yields a valid pools list: %w",
			cm.Namespace, cm.Name, errors.Join(errs...))
	}
	klog.Warningf("headroom: configmap %s/%s exists but no data key yields a non-empty pools list", cm.Namespace, cm.Name)
	return nil, nil
}

// candidateKeys returns the ConfigMap data keys in the order they should be scanned: the
// well-known keys first ("policy", then "config"), then every other key sorted by name.
// The sort is load-bearing — Go map iteration order is randomized, so an unsorted fallback
// scan would let the applied policy flap between reconciles (churning Deployments) whenever
// two keys both declare a pools list.
func candidateKeys(data map[string]string) []string {
	keys := make([]string, 0, len(data))
	for _, key := range wellKnownKeys {
		if _, ok := data[key]; ok {
			keys = append(keys, key)
		}
	}
	rest := make([]string, 0, len(data))
	for key := range data {
		if !isWellKnownKey(key) {
			rest = append(rest, key)
		}
	}
	sort.Strings(rest)
	return append(keys, rest...)
}

func isWellKnownKey(key string) bool {
	for _, k := range wellKnownKeys {
		if k == key {
			return true
		}
	}
	return false
}

// isNonMappingYAML reports whether data is well-formed YAML whose top level is a scalar or a
// list — a document that can never carry a `pools` key. Malformed YAML returns false so that
// it still reaches unmarshalPools and fails closed (a typo in a fallback policy key must not
// silently read as "no pools").
func isNonMappingYAML(data string) bool {
	var v any
	if err := yaml.Unmarshal([]byte(data), &v); err != nil {
		return false
	}
	switch v.(type) {
	case nil, map[string]any:
		return false
	}
	return true
}

func unmarshalPools(data string) ([]parsedPoolPolicy, error) {
	var raw headroomPolicyYAML
	if err := yaml.Unmarshal([]byte(data), &raw); err != nil {
		return nil, err
	}
	if len(raw.Pools) == 0 {
		return nil, nil
	}
	out := make([]parsedPoolPolicy, 0, len(raw.Pools))
	seen := make(map[string]bool, len(raw.Pools))
	for _, p := range raw.Pools {
		if p.Name == "" {
			continue
		}
		// Duplicates would apply two conflicting specs to the same Deployment on every sync.
		if seen[p.Name] {
			return nil, fmt.Errorf("pool %q: duplicate pool name", p.Name)
		}
		seen[p.Name] = true
		// The name becomes the Deployment name suffix and a label value; anything that is not
		// a DNS label is rejected by the API server on every apply, so refuse it here.
		if msgs := validation.IsDNS1123Label(p.Name); len(msgs) > 0 {
			return nil, fmt.Errorf("pool %q name: %s", p.Name, strings.Join(msgs, "; "))
		}
		parsed := parsedPoolPolicy{Name: p.Name, MinPods: p.MinPods, MaxPods: p.MaxPods}
		var err error
		if parsed.CPU, err = parsePercent(p.CPU); err != nil {
			return nil, fmt.Errorf("pool %q cpu: %w", p.Name, err)
		}
		if parsed.Memory, err = parsePercent(p.Memory); err != nil {
			return nil, fmt.Errorf("pool %q memory: %w", p.Name, err)
		}
		if p.GPU != "" {
			if parsed.GPU, err = parsePercent(p.GPU); err != nil {
				return nil, fmt.Errorf("pool %q gpu: %w", p.Name, err)
			}
		}
		if parsed.PodCPU, err = parseQuantity(p.PodCPU, defaultPodCPU); err != nil {
			return nil, fmt.Errorf("pool %q podCPU: %w", p.Name, err)
		}
		if parsed.PodCPU.Cmp(minPodCPUQuantity) < 0 {
			return nil, fmt.Errorf("pool %q podCPU: must be >= %s, got %q", p.Name, minPodCPU, parsed.PodCPU.String())
		}
		if parsed.PodMemory, err = parseQuantity(p.PodMemory, defaultPodMemory); err != nil {
			return nil, fmt.Errorf("pool %q podMemory: %w", p.Name, err)
		}
		if parsed.PodMemory.Cmp(minPodMemoryQuantity) < 0 {
			return nil, fmt.Errorf("pool %q podMemory: must be >= %s, got %q", p.Name, minPodMemory, parsed.PodMemory.String())
		}
		if parsed.PodGPU, err = parseQuantity(p.PodGPU, ""); err != nil {
			return nil, fmt.Errorf("pool %q podGPU: %w", p.Name, err)
		}
		if parsed.PodGPU.Sign() < 0 {
			return nil, fmt.Errorf("pool %q podGPU: must be >= 0, got %q", p.Name, parsed.PodGPU.String())
		}
		// Extended resources are integral: the API server rejects a fractional GPU request.
		if parsed.PodGPU.MilliValue()%1000 != 0 {
			return nil, fmt.Errorf("pool %q podGPU: must be a whole number of devices, got %q", p.Name, parsed.PodGPU.String())
		}
		if p.MinPods < 0 {
			return nil, fmt.Errorf("pool %q minPods: must be >= 0, got %d", p.Name, p.MinPods)
		}
		if p.MaxPods < 0 {
			return nil, fmt.Errorf("pool %q maxPods: must be >= 0, got %d", p.Name, p.MaxPods)
		}
		if p.MaxPods > 0 && p.MaxPods < p.MinPods {
			return nil, fmt.Errorf("pool %q maxPods: must be >= minPods (%d), got %d", p.Name, p.MinPods, p.MaxPods)
		}
		if parsed.Tolerations, err = parseTolerations(p.Tolerations); err != nil {
			return nil, fmt.Errorf("pool %q tolerations: %w", p.Name, err)
		}
		out = append(out, parsed)
	}
	return out, nil
}

func parsePercent(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	s = strings.TrimSuffix(s, "%")
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid percent %q: %w", s, err)
	}
	// ParseFloat accepts "NaN", which fails both range comparisons and would silently size
	// the buffer to zero.
	if math.IsNaN(v) {
		return 0, fmt.Errorf("invalid percent %q: not a number", s)
	}
	if v < 0 || v > maxBufferPercent {
		return 0, fmt.Errorf("percent %q out of range [0, %g]: the buffer is a fraction of total pool allocatable, "+
			"so a larger fraction cannot converge once DaemonSet overhead is counted (effective buffer is f/(1-f-d))",
			s, maxBufferPercent)
	}
	return v / 100.0, nil
}

// parseQuantity parses a resource quantity string, substituting def when s is empty.
// An empty def with an empty s yields the zero Quantity (resource not configured).
func parseQuantity(s, def string) (resource.Quantity, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		s = def
	}
	if s == "" {
		return resource.Quantity{}, nil
	}
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("invalid quantity %q: %w", s, err)
	}
	return q, nil
}

// parseTolerations converts ConfigMap toleration entries to corev1.Tolerations,
// validating that operator and effect carry only values the scheduler understands, that a
// non-empty key is a qualified name (the API server runs IsQualifiedName on every toleration
// key: a trailing space from a YAML edit or an embedded space is a 422), and the cross-field
// combinations the API server enforces (an Exists toleration carries no value; an Equal one —
// the default when operator is empty — needs a key). A combination the API server would reject
// must fail here, or the pool's Deployment fails on every apply.
func parseTolerations(in []tolerationYAML) ([]corev1.Toleration, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]corev1.Toleration, 0, len(in))
	for i, t := range in {
		switch corev1.TolerationOperator(t.Operator) {
		case corev1.TolerationOpExists:
			if t.Value != "" {
				return nil, fmt.Errorf("entry %d: value must be empty when operator is Exists", i)
			}
		case "", corev1.TolerationOpEqual:
			if t.Key == "" {
				return nil, fmt.Errorf("entry %d: key must be set unless operator is Exists", i)
			}
		default:
			return nil, fmt.Errorf("entry %d: invalid operator %q", i, t.Operator)
		}
		if t.Key != "" {
			if msgs := validation.IsQualifiedName(t.Key); len(msgs) > 0 {
				return nil, fmt.Errorf("entry %d: key %q: %s", i, t.Key, strings.Join(msgs, "; "))
			}
		}
		switch corev1.TaintEffect(t.Effect) {
		case "", corev1.TaintEffectNoSchedule, corev1.TaintEffectPreferNoSchedule, corev1.TaintEffectNoExecute:
		default:
			return nil, fmt.Errorf("entry %d: invalid effect %q", i, t.Effect)
		}
		out = append(out, corev1.Toleration{
			Key:      t.Key,
			Operator: corev1.TolerationOperator(t.Operator),
			Value:    t.Value,
			Effect:   corev1.TaintEffect(t.Effect),
		})
	}
	return out, nil
}
