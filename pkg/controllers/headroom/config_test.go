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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newConfigMap(data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: "karpenter"},
		Data:       data,
	}
}

const validPoolYAML = `
pools:
  - name: worker-pool-amd
    cpu: 30%
    memory: 20%
`

func TestParseHeadroomConfig_PolicyKey(t *testing.T) {
	pools, err := parseHeadroomConfig(newConfigMap(map[string]string{"policy": validPoolYAML}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	p := pools[0]
	if p.Name != "worker-pool-amd" {
		t.Errorf("name = %q, want worker-pool-amd", p.Name)
	}
	if p.CPU != 0.30 {
		t.Errorf("cpu fraction = %v, want 0.30", p.CPU)
	}
	if p.Memory != 0.20 {
		t.Errorf("memory fraction = %v, want 0.20", p.Memory)
	}
	// Defaults apply when podCPU/podMemory are unset.
	if p.PodCPU.Cmp(resource.MustParse(defaultPodCPU)) != 0 {
		t.Errorf("podCPU = %v, want default %s", p.PodCPU.String(), defaultPodCPU)
	}
	if p.PodMemory.Cmp(resource.MustParse(defaultPodMemory)) != 0 {
		t.Errorf("podMemory = %v, want default %s", p.PodMemory.String(), defaultPodMemory)
	}
	if !p.PodGPU.IsZero() {
		t.Errorf("podGPU = %v, want zero (unset)", p.PodGPU.String())
	}
	if p.MinPods != 0 {
		t.Errorf("minPods = %d, want 0", p.MinPods)
	}
	if p.Tolerations != nil {
		t.Errorf("tolerations = %v, want nil", p.Tolerations)
	}
}

func TestParseHeadroomConfig_ConfigKey(t *testing.T) {
	pools, err := parseHeadroomConfig(newConfigMap(map[string]string{"config": validPoolYAML}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "worker-pool-amd" {
		t.Fatalf("expected pool worker-pool-amd under %q key, got %+v", "config", pools)
	}
}

func TestParseHeadroomConfig_PolicyKeyWinsOverConfigKey(t *testing.T) {
	cm := newConfigMap(map[string]string{
		"policy": "pools:\n  - name: from-policy\n    cpu: 10%\n",
		"config": "pools:\n  - name: from-config\n    cpu: 10%\n",
	})
	pools, err := parseHeadroomConfig(cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "from-policy" {
		t.Fatalf("expected pool from-policy (policy key tried first), got %+v", pools)
	}
}

func TestParseHeadroomConfig_BrokenPolicyFallsBackToConfigKey(t *testing.T) {
	cm := newConfigMap(map[string]string{
		"policy": "pools:\n  - name: bad\n    cpu: garbage%\n",
		"config": "pools:\n  - name: from-config\n    cpu: 10%\n",
	})
	pools, err := parseHeadroomConfig(cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "from-config" {
		t.Fatalf("expected fallback to config key, got %+v", pools)
	}
}

func TestParseHeadroomConfig_FallbackScansOtherKeys(t *testing.T) {
	cm := newConfigMap(map[string]string{
		"policy":     "pools: []\n",
		"custom-key": validPoolYAML,
	})
	pools, err := parseHeadroomConfig(cm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "worker-pool-amd" {
		t.Fatalf("expected pool from fallback key scan, got %+v", pools)
	}
}

func TestParseHeadroomConfig_EmptyOrMissingPools(t *testing.T) {
	// A ConfigMap that parses cleanly but declares no pools is a legitimate "headroom off"
	// policy: no pools, NO error (the caller is then allowed to GC the headroom Deployments).
	cases := map[string]*corev1.ConfigMap{
		"no data":                  newConfigMap(nil),
		"empty pools list":         newConfigMap(map[string]string{"policy": "pools: []\n"}),
		"missing pools key":        newConfigMap(map[string]string{"policy": "name: something-else\n"}),
		"pool entries all unnamed": newConfigMap(map[string]string{"policy": "pools:\n  - cpu: 10%\n"}),
	}
	for name, cm := range cases {
		t.Run(name, func(t *testing.T) {
			pools, err := parseHeadroomConfig(cm)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(pools) != 0 {
				t.Fatalf("expected no pools, got %+v", pools)
			}
		})
	}
}

func TestParseHeadroomConfig_BrokenConfigReturnsError(t *testing.T) {
	// A broken ConfigMap must NOT look like "no pools configured": Reconcile would then GC
	// every headroom Deployment. Every case here yields zero pools with at least one key
	// failing to parse ⇒ error.
	cases := map[string]*corev1.ConfigMap{
		"unparseable yaml":   newConfigMap(map[string]string{"policy": "pools: [garbage"}),
		"invalid pool field": newConfigMap(map[string]string{"policy": "pools:\n  - name: p\n    podCPU: notacpu\n"}),
		"every key broken": newConfigMap(map[string]string{
			"policy": "pools:\n  - name: p\n    cpu: garbage%\n",
			"config": "pools: [garbage",
			"extra":  "pools:\n  - name: p\n    minPods: -1\n",
		}),
		"broken key with an empty-but-valid key": newConfigMap(map[string]string{
			"policy": "pools: []\n",
			"config": "pools:\n  - name: p\n    cpu: 200%\n",
		}),
	}
	for name, cm := range cases {
		t.Run(name, func(t *testing.T) {
			pools, err := parseHeadroomConfig(cm)
			if err == nil {
				t.Fatalf("expected an error for a broken policy, got pools %+v", pools)
			}
			if len(pools) != 0 {
				t.Errorf("expected no pools alongside the error, got %+v", pools)
			}
		})
	}
}

func TestParseHeadroomConfig_DeterministicKeyOrder(t *testing.T) {
	// Two non-well-known keys both declaring pools: the winner must be the lexicographically
	// first one on every call, not whichever Go's randomized map iteration happens to hit —
	// otherwise the applied policy flaps between reconciles and churns Deployments.
	cm := newConfigMap(map[string]string{
		"a-key": "pools:\n  - name: from-a\n    cpu: 10%\n",
		"z-key": "pools:\n  - name: from-z\n    cpu: 10%\n",
	})
	for i := 0; i < 20; i++ {
		pools, err := parseHeadroomConfig(cm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pools) != 1 || pools[0].Name != "from-a" {
			t.Fatalf("iteration %d: expected the sorted-first key to win with from-a, got %+v", i, pools)
		}
	}
}

func TestCandidateKeys_WellKnownFirstThenSorted(t *testing.T) {
	got := candidateKeys(map[string]string{"z": "", "config": "", "a": "", "policy": "", "m": ""})
	want := []string{"policy", "config", "a", "m", "z"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("candidateKeys() = %v, want %v", got, want)
	}
}

func TestUnmarshalPools_SkipsUnnamedEntries(t *testing.T) {
	pools, err := unmarshalPools("pools:\n  - cpu: 10%\n  - name: named\n    cpu: 10%\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "named" {
		t.Fatalf("expected only the named pool, got %+v", pools)
	}
}

func TestParsePercent(t *testing.T) {
	cases := []struct {
		in      string
		want    float64
		wantErr bool
	}{
		{in: "30%", want: 0.30},
		{in: "30", want: 0.30},
		{in: " 30% ", want: 0.30},
		{in: " 30 % ", want: 0.30},
		{in: "0%", want: 0},
		{in: "100%", want: 1.0},
		{in: "2.5%", want: 0.025},
		{in: "", want: 0},
		{in: "   ", want: 0},
		{in: "101%", wantErr: true},
		{in: "150", wantErr: true},
		{in: "-1%", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "%", wantErr: true},
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

func TestUnmarshalPools_NewFieldsValid(t *testing.T) {
	data := `
pools:
  - name: gpu-pool
    cpu: 30%
    memory: 20%
    gpu: 10%
    podCPU: 250m
    podMemory: 1Gi
    podGPU: "1"
    minPods: 2
    tolerations:
      - key: example.com/dedicated
        operator: Equal
        value: headroom
        effect: NoSchedule
      - key: example.com/gpu
        operator: Exists
        effect: NoExecute
`
	pools, err := unmarshalPools(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}
	p := pools[0]
	if p.GPU != 0.10 {
		t.Errorf("gpu fraction = %v, want 0.10", p.GPU)
	}
	if p.PodCPU.Cmp(resource.MustParse("250m")) != 0 {
		t.Errorf("podCPU = %v, want 250m", p.PodCPU.String())
	}
	if p.PodMemory.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("podMemory = %v, want 1Gi", p.PodMemory.String())
	}
	if p.PodGPU.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("podGPU = %v, want 1", p.PodGPU.String())
	}
	if p.MinPods != 2 {
		t.Errorf("minPods = %d, want 2", p.MinPods)
	}
	wantTolerations := []corev1.Toleration{
		{Key: "example.com/dedicated", Operator: corev1.TolerationOpEqual, Value: "headroom", Effect: corev1.TaintEffectNoSchedule},
		{Key: "example.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute},
	}
	if !reflect.DeepEqual(p.Tolerations, wantTolerations) {
		t.Errorf("tolerations = %+v, want %+v", p.Tolerations, wantTolerations)
	}
}

func TestUnmarshalPools_InvalidValues(t *testing.T) {
	cases := []struct {
		name    string
		data    string
		errPart string
	}{
		{
			name:    "invalid podCPU",
			data:    "pools:\n  - name: p\n    podCPU: notacpu\n",
			errPart: "podCPU",
		},
		{
			name:    "invalid podMemory",
			data:    "pools:\n  - name: p\n    podMemory: 12XYZ\n",
			errPart: "podMemory",
		},
		{
			name:    "invalid podGPU",
			data:    "pools:\n  - name: p\n    podGPU: one\n",
			errPart: "podGPU",
		},
		{
			name:    "negative minPods",
			data:    "pools:\n  - name: p\n    minPods: -1\n",
			errPart: "minPods",
		},
		{
			name:    "invalid cpu percent",
			data:    "pools:\n  - name: p\n    cpu: 150%\n",
			errPart: "cpu",
		},
		{
			name:    "invalid memory percent",
			data:    "pools:\n  - name: p\n    memory: -5%\n",
			errPart: "memory",
		},
		{
			name:    "invalid gpu percent",
			data:    "pools:\n  - name: p\n    gpu: lots\n",
			errPart: "gpu",
		},
		{
			name:    "podCPU below the 1m floor",
			data:    "pools:\n  - name: p\n    podCPU: 100u\n",
			errPart: "podCPU",
		},
		{
			name:    "zero podCPU",
			data:    "pools:\n  - name: p\n    podCPU: \"0\"\n",
			errPart: "podCPU",
		},
		{
			name:    "podMemory below the 1Mi floor",
			data:    "pools:\n  - name: p\n    podMemory: 1Ki\n",
			errPart: "podMemory",
		},
		{
			name:    "negative podGPU",
			data:    "pools:\n  - name: p\n    podGPU: \"-1\"\n",
			errPart: "podGPU",
		},
		{
			name:    "duplicate pool names",
			data:    "pools:\n  - name: dup\n    cpu: 10%\n  - name: dup\n    cpu: 20%\n",
			errPart: "duplicate pool name",
		},
		{
			name:    "invalid toleration operator",
			data:    "pools:\n  - name: p\n    tolerations:\n      - key: k\n        operator: Sometimes\n",
			errPart: "invalid operator",
		},
		{
			name:    "invalid toleration effect",
			data:    "pools:\n  - name: p\n    tolerations:\n      - key: k\n        operator: Exists\n        effect: NoIdea\n",
			errPart: "invalid effect",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pools, err := unmarshalPools(tc.data)
			if err == nil {
				t.Fatalf("expected error, got pools %+v", pools)
			}
			if !strings.Contains(err.Error(), tc.errPart) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.errPart)
			}
		})
	}
}

func TestParseQuantity(t *testing.T) {
	q, err := parseQuantity("", "500m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Cmp(resource.MustParse("500m")) != 0 {
		t.Errorf("default not applied: got %v", q.String())
	}

	q, err = parseQuantity(" 1Gi ", "512Mi")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("explicit value ignored: got %v", q.String())
	}

	q, err = parseQuantity("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !q.IsZero() {
		t.Errorf("empty value with empty default should be zero, got %v", q.String())
	}

	if _, err = parseQuantity("bogus", ""); err == nil {
		t.Error("expected error for invalid quantity")
	}
}
