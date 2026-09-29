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
	"math"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// --- parseHeadroomConfig / candidateKeys ---

// TestHrParseHeadroomConfig_HeadroomOffIgnoresUnrelatedKey is the regression test for
// R1-prov-headroom-4: `policy: pools: []` is the documented "headroom off" policy and must
// yield (nil, nil) so Reconcile can GC the buffer Deployments. A free-text key sharing the
// ConfigMap (a README, a maintenance note) can never declare pools, so it must not turn the
// intended "off" into a parse error that keeps the Deployments up forever.
func TestHrParseHeadroomConfig_HeadroomOffIgnoresUnrelatedKey(t *testing.T) {
	cases := map[string]map[string]string{
		"free text note": {
			"policy": "pools: []\n",
			"notes":  "disabled during maintenance",
		},
		"free text note that is malformed YAML": {
			"policy": "pools: []\n",
			"notes":  "Disabled: see ticket: RC-123",
		},
		"scalar bool": {
			"policy":  "pools: []\n",
			"enabled": "false",
		},
		"scalar number": {
			"policy":  "pools: []\n",
			"version": "3",
		},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			pools, err := parseHeadroomConfig(newConfigMap(data))
			if err != nil {
				t.Fatalf("headroom off with an unrelated key must not error, got: %v", err)
			}
			if len(pools) != 0 {
				t.Fatalf("expected no pools, got %+v", pools)
			}
		})
	}
}

// TestHrParseHeadroomConfig_UnrelatedMappingKeyIsHarmless locks in the part of the
// fallback scan that does work today: an extra key that IS a YAML mapping (but declares no
// pools) is ignored, so `pools: []` still reads as headroom off.
func TestHrParseHeadroomConfig_UnrelatedMappingKeyIsHarmless(t *testing.T) {
	pools, err := parseHeadroomConfig(newConfigMap(map[string]string{
		"policy": "pools: []\n",
		"notes":  "owner: platform-team\nreason: maintenance\n",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 0 {
		t.Fatalf("expected no pools, got %+v", pools)
	}
}

// TestHrParseHeadroomConfig_PoolsWinOverBrokenSiblingKey locks in the other half of the
// error contract: once a key yields pools, parse failures in other keys are only warned
// about — the returned policy is usable and there is no error.
func TestHrParseHeadroomConfig_PoolsWinOverBrokenSiblingKey(t *testing.T) {
	pools, err := parseHeadroomConfig(newConfigMap(map[string]string{
		"policy": validPoolYAML,
		"notes":  "free text that is not a mapping",
		"config": "pools: [garbage",
	}))
	if err != nil {
		t.Fatalf("a valid policy key must win over broken siblings, got error: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "worker-pool-amd" {
		t.Fatalf("expected worker-pool-amd from the policy key, got %+v", pools)
	}
}

// TestHrParseHeadroomConfig_EmptyPoolsWithExtraTopLevelKeys: other top-level keys inside
// the policy document are ignored, so `pools: []` next to them is still headroom off.
func TestHrParseHeadroomConfig_EmptyPoolsWithExtraTopLevelKeys(t *testing.T) {
	cases := map[string]string{
		"pools empty list with siblings": "name: headroom\npools: []\ncomfigmap: yes\n",
		"pools null":                     "pools: null\n",
		"pools absent, only siblings":    "name: headroom\nversion: 2\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			pools, err := parseHeadroomConfig(newConfigMap(map[string]string{"policy": data}))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(pools) != 0 {
				t.Fatalf("expected no pools, got %+v", pools)
			}
		})
	}
}

func TestHrUnmarshalPools_PoolsNotAList(t *testing.T) {
	cases := map[string]string{
		"pools is a scalar":  "pools: worker-pool-amd\n",
		"pools is a mapping": "pools:\n  name: worker-pool-amd\n",
		"document is a list": "- name: worker-pool-amd\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			pools, err := unmarshalPools(data)
			if err == nil {
				t.Fatalf("expected an error for a non-list pools value, got %+v", pools)
			}
		})
	}
}

func TestHrCandidateKeys_Edges(t *testing.T) {
	cases := []struct {
		name string
		data map[string]string
		want []string
	}{
		{name: "nil map", data: nil, want: []string{}},
		{name: "empty map", data: map[string]string{}, want: []string{}},
		{name: "only config", data: map[string]string{"config": ""}, want: []string{"config"}},
		{name: "only policy", data: map[string]string{"policy": ""}, want: []string{"policy"}},
		{name: "config before policy in map still yields policy first", data: map[string]string{"config": "", "policy": ""}, want: []string{"policy", "config"}},
		{name: "no well-known keys sorted", data: map[string]string{"b": "", "a": "", "README": ""}, want: []string{"README", "a", "b"}},
		{name: "well-known keys are not repeated in the sorted tail", data: map[string]string{"policy": "", "a": ""}, want: []string{"policy", "a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := candidateKeys(tc.data)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("candidateKeys(%v) = %v, want %v", tc.data, got, tc.want)
			}
		})
	}
}

// --- fraction bounds ---

// TestHrParsePercent_RejectsNonConvergentFraction is the regression test for
// R1-prov-headroom-2: the buffer is a fraction f of TOTAL pool allocatable, and every node
// also carries DaemonSet overhead d, so any f >= 1 - d has no fixed point (each new node adds
// as much pause-pod demand as capacity). Fractions at or near 100% must be refused at parse
// time instead of being accepted and driving unbounded scale-out.
func TestHrParsePercent_RejectsNonConvergentFraction(t *testing.T) {
	for _, in := range []string{"100%", "100", "95%", "99.9%"} {
		t.Run(in, func(t *testing.T) {
			got, err := parsePercent(in)
			if err == nil {
				t.Fatalf("parsePercent(%q) = %v, want an error (non-convergent buffer fraction)", in, got)
			}
		})
	}
}

// TestHrParsePercent_Boundaries pins the accepted range [0, maxBufferPercent] and the exact
// boundary behaviour (tightened from [0, 100] for R1-prov-headroom-2: the fraction-of-total
// model has no fixed point once f + DaemonSet share >= 1).
func TestHrParsePercent_Boundaries(t *testing.T) {
	cases := []struct {
		in      string
		want    float64
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "0.0%", want: 0},
		{in: "50", want: 0.5},
		{in: "50.0%", want: 0.5},
		{in: "50.0001", wantErr: true},
		{in: "50.5%", wantErr: true},
		{in: "100.0", wantErr: true},
		{in: "100.0001", wantErr: true},
		{in: "-0.0001", wantErr: true},
		{in: "1e1", want: 0.1}, // ParseFloat accepts exponent notation: 1e1 == 10
		{in: "%%", wantErr: true},
		{in: "30%%", wantErr: true},
		{in: "0x10", wantErr: true},
		{in: "Inf", wantErr: true},
		{in: "+Inf", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parsePercent(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parsePercent(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePercent(%q) unexpected error: %v", tc.in, err)
			}
			if math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("parsePercent(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// TestHrParsePercent_RejectsNaN: strconv.ParseFloat accepts "NaN", and NaN fails both range
// comparisons, so `cpu: NaN` currently parses to a NaN fraction that the replica math turns
// into 0 replicas — the pool silently has no buffer and no error is reported anywhere.
func TestHrParsePercent_RejectsNaN(t *testing.T) {
	for _, in := range []string{"NaN", "nan", "NaN%"} {
		t.Run(in, func(t *testing.T) {
			got, err := parsePercent(in)
			if err == nil {
				t.Fatalf("parsePercent(%q) = %v, want error", in, got)
			}
		})
	}
}

// --- tolerations / pool validation ---

// TestHrUnmarshalPools_APIServerRejectedCombinations is the regression test for
// R1-prov-headroom-3: combinations that parse today but that the API server rejects on
// every reconcile (so the pool never gets a buffer) must be refused at parse time, per the
// documented "bad policy is caught before apply" contract.
func TestHrUnmarshalPools_APIServerRejectedCombinations(t *testing.T) {
	cases := []struct {
		name    string
		data    string
		errPart string
	}{
		{
			name:    "Exists toleration with a value",
			data:    "pools:\n  - name: p\n    tolerations:\n      - key: nvidia.com/gpu\n        operator: Exists\n        value: \"true\"\n        effect: NoSchedule\n",
			errPart: "value",
		},
		{
			name:    "Equal toleration with an empty key",
			data:    "pools:\n  - name: p\n    tolerations:\n      - operator: Equal\n        value: x\n        effect: NoSchedule\n",
			errPart: "key",
		},
		{
			name:    "empty-operator toleration with an empty key",
			data:    "pools:\n  - name: p\n    tolerations:\n      - value: x\n        effect: NoSchedule\n",
			errPart: "key",
		},
		{
			name:    "fractional podGPU",
			data:    "pools:\n  - name: p\n    podGPU: \"0.5\"\n",
			errPart: "podGPU",
		},
		{
			name:    "millis podGPU",
			data:    "pools:\n  - name: p\n    podGPU: 500m\n",
			errPart: "podGPU",
		},
		{
			name:    "pool name with a space",
			data:    "pools:\n  - name: \"Worker Pool\"\n    cpu: 10%\n",
			errPart: "name",
		},
		{
			name:    "pool name with uppercase",
			data:    "pools:\n  - name: WorkerPool\n    cpu: 10%\n",
			errPart: "name",
		},
		{
			name:    "pool name longer than a DNS label",
			data:    "pools:\n  - name: " + strings.Repeat("a", 64) + "\n    cpu: 10%\n",
			errPart: "name",
		},
		{
			name:    "key with trailing space",
			data:    "pools:\n  - name: p\n    tolerations:\n      - key: \"nvidia.com/gpu \"\n        operator: Exists\n        effect: NoSchedule\n",
			errPart: "key",
		},
		{
			name:    "key with a space",
			data:    "pools:\n  - name: p\n    tolerations:\n      - key: \"gpu key\"\n        operator: Equal\n        value: \"true\"\n        effect: NoSchedule\n",
			errPart: "key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pools, err := unmarshalPools(tc.data)
			if err == nil {
				t.Fatalf("expected a parse-time error, got pools %+v", pools)
			}
			if !strings.Contains(err.Error(), tc.errPart) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.errPart)
			}
		})
	}
}

// TestHrParseTolerations_Combinations covers the toleration shapes the parser accepts and
// how they are carried through, beyond the single-field operator/effect checks.
func TestHrParseTolerations_Combinations(t *testing.T) {
	cases := []struct {
		name string
		in   []tolerationYAML
		want []corev1.Toleration
	}{
		{
			name: "empty operator is passed through unchanged (API server defaults it to Equal)",
			in:   []tolerationYAML{{Key: "k", Value: "v", Effect: "NoSchedule"}},
			want: []corev1.Toleration{{Key: "k", Operator: "", Value: "v", Effect: corev1.TaintEffectNoSchedule}},
		},
		{
			name: "Exists with key and no effect tolerates every effect of that taint",
			in:   []tolerationYAML{{Key: "k", Operator: "Exists"}},
			want: []corev1.Toleration{{Key: "k", Operator: corev1.TolerationOpExists}},
		},
		{
			name: "blanket Exists (no key, no effect) is accepted from config — the controller never adds one itself",
			in:   []tolerationYAML{{Operator: "Exists"}},
			want: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
		},
		{
			name: "every valid effect",
			in: []tolerationYAML{
				{Key: "a", Operator: "Exists", Effect: "NoSchedule"},
				{Key: "b", Operator: "Exists", Effect: "PreferNoSchedule"},
				{Key: "c", Operator: "Exists", Effect: "NoExecute"},
			},
			want: []corev1.Toleration{
				{Key: "a", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
				{Key: "b", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectPreferNoSchedule},
				{Key: "c", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
			},
		},
		{
			name: "order is preserved",
			in: []tolerationYAML{
				{Key: "z", Operator: "Equal", Value: "1"},
				{Key: "a", Operator: "Equal", Value: "2"},
			},
			want: []corev1.Toleration{
				{Key: "z", Operator: corev1.TolerationOpEqual, Value: "1"},
				{Key: "a", Operator: corev1.TolerationOpEqual, Value: "2"},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseTolerations(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("parseTolerations() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestHrParseTolerations_EmptyAndNil(t *testing.T) {
	for name, in := range map[string][]tolerationYAML{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			got, err := parseTolerations(in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != nil {
				t.Errorf("parseTolerations(%s) = %+v, want nil (so the pod template carries no tolerations field)", name, got)
			}
		})
	}
}

func TestHrParseTolerations_InvalidEntryIndexIsReported(t *testing.T) {
	// The error must name the offending entry so an operator can find it in a long list.
	_, err := parseTolerations([]tolerationYAML{
		{Key: "ok", Operator: "Exists"},
		{Key: "ok2", Operator: "Equal", Value: "v"},
		{Key: "bad", Operator: "Exists", Effect: "Evict"},
	})
	if err == nil {
		t.Fatal("expected an error for the invalid effect")
	}
	if !strings.Contains(err.Error(), "entry 2") {
		t.Errorf("error %q does not name entry 2", err.Error())
	}
}

// TestHrUnmarshalPools_ErrorNamesThePool: every per-pool validation error is wrapped with
// the pool name, and the first failing pool aborts the whole document (fail-closed: the
// caller must not apply the pools that happened to parse before it).
func TestHrUnmarshalPools_ErrorNamesThePool(t *testing.T) {
	data := "pools:\n  - name: good\n    cpu: 10%\n  - name: bad-pool\n    tolerations:\n      - key: k\n        operator: Maybe\n"
	pools, err := unmarshalPools(data)
	if err == nil {
		t.Fatalf("expected an error, got pools %+v", pools)
	}
	if pools != nil {
		t.Errorf("a document with one bad pool must yield no pools at all, got %+v", pools)
	}
	if !strings.Contains(err.Error(), `pool "bad-pool"`) || !strings.Contains(err.Error(), "tolerations") {
		t.Errorf("error %q should name the pool and the field", err.Error())
	}
}

func TestHrUnmarshalPools_ZeroFractionsAndMinPodsOnly(t *testing.T) {
	// A pool that only sets minPods (cold-start only, no fraction) is a valid policy.
	pools, err := unmarshalPools("pools:\n  - name: cold\n    minPods: 3\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %+v", pools)
	}
	p := pools[0]
	if p.CPU != 0 || p.Memory != 0 || p.GPU != 0 {
		t.Errorf("fractions = cpu %v mem %v gpu %v, want all zero", p.CPU, p.Memory, p.GPU)
	}
	if p.MinPods != 3 {
		t.Errorf("minPods = %d, want 3", p.MinPods)
	}
	// Defaults still apply so the pause pod always carries a cpu/memory request.
	if p.PodCPU.IsZero() || p.PodMemory.IsZero() {
		t.Errorf("podCPU/podMemory defaults not applied: %v / %v", p.PodCPU.String(), p.PodMemory.String())
	}
}

func TestHrUnmarshalPools_MinimumPodSizesAreAccepted(t *testing.T) {
	// Exactly the floor values must pass (the check is >=, not >).
	pools, err := unmarshalPools("pools:\n  - name: tiny\n    podCPU: 1m\n    podMemory: 1Mi\n")
	if err != nil {
		t.Fatalf("floor values must be accepted, got: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %+v", pools)
	}
	if pools[0].PodCPU.MilliValue() != 1 {
		t.Errorf("podCPU = %v, want 1m", pools[0].PodCPU.String())
	}
	if pools[0].PodMemory.Value() != 1<<20 {
		t.Errorf("podMemory = %v, want 1Mi", pools[0].PodMemory.String())
	}
}

func TestHrUnmarshalPools_GPUFractionWithoutPodGPU(t *testing.T) {
	// gpu: 10% without podGPU parses (the reconciler ignores the GPU term); podGPU stays zero.
	pools, err := unmarshalPools("pools:\n  - name: g\n    gpu: 10%\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].GPU != 0.10 || !pools[0].PodGPU.IsZero() {
		t.Fatalf("got %+v, want gpu=0.10 and zero podGPU", pools)
	}
}
