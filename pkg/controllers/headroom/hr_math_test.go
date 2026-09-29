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
	"context"
	"math"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
)

// --- ceilDiv boundaries ---

func TestHrCeilDiv(t *testing.T) {
	limit := maxHeadroomReplicas
	cases := []struct {
		name   string
		buffer float64
		perPod int64
		want   int64
	}{
		{name: "zero buffer", buffer: 0, perPod: 500, want: 0},
		{name: "negative buffer", buffer: -1, perPod: 500, want: 0},
		{name: "zero perPod", buffer: 1000, perPod: 0, want: 0},
		{name: "negative perPod", buffer: 1000, perPod: -500, want: 0},
		{name: "exact division", buffer: 1000, perPod: 500, want: 2},
		{name: "just above an integer rounds up", buffer: 1000.0001, perPod: 500, want: 3},
		{name: "just below an integer rounds up to it", buffer: 999.9999, perPod: 500, want: 2},
		{name: "buffer smaller than one pod ceils to 1", buffer: 1, perPod: 500, want: 1},
		{name: "tiny positive buffer ceils to 1", buffer: 1e-9, perPod: 500, want: 1},
		{name: "exactly the cap is not clamped", buffer: float64(limit) * 500, perPod: 500, want: limit},
		{name: "one unit above the cap is clamped", buffer: float64(limit)*500 + 1, perPod: 500, want: limit},
		{name: "huge buffer is clamped", buffer: 1e300, perPod: 1, want: limit},
		{name: "+Inf is clamped instead of an undefined float→int conversion", buffer: math.Inf(1), perPod: 1, want: limit},
		{name: "-Inf is treated as non-positive", buffer: math.Inf(-1), perPod: 1, want: 0},
		{name: "MaxInt64 perPod against a small buffer", buffer: 1, perPod: math.MaxInt64, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ceilDiv(tc.buffer, tc.perPod); got != tc.want {
				t.Errorf("ceilDiv(%v, %d) = %d, want %d", tc.buffer, tc.perPod, got, tc.want)
			}
		})
	}
}

// --- desiredReplicas gaps ---

func TestHrDesiredReplicas_Gaps(t *testing.T) {
	mustQ := resource.MustParse
	cases := []struct {
		name       string
		pool       parsedPoolPolicy
		total      poolAllocatable
		readyNodes int
		want       int32
	}{
		{
			name:       "gpu fraction + podGPU but no GPU node yields 0 from the GPU term",
			pool:       parsedPoolPolicy{GPU: 0.50, PodGPU: mustQ("1"), PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 8000, memBytes: 32 << 30}, // gpuKey empty, gpu 0
			readyNodes: 2,
			want:       0,
		},
		{
			name:       "gpu term is dropped but cpu term still counts",
			pool:       parsedPoolPolicy{CPU: 0.25, GPU: 0.50, PodGPU: mustQ("1"), PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 8000},
			readyNodes: 2,
			want:       4, // 2000m / 500m
		},
		{
			name:       "memory-only fraction with zero memBytes on ready nodes",
			pool:       parsedPoolPolicy{Memory: 0.50, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 8000},
			readyNodes: 2,
			want:       0,
		},
		{
			name:       "memory-only fraction",
			pool:       parsedPoolPolicy{Memory: 0.25, PodCPU: mustQ("500m"), PodMemory: mustQ("1Gi")},
			total:      poolAllocatable{cpuMilli: 8000, memBytes: 10 << 30},
			readyNodes: 2,
			want:       3, // ceil(2.5Gi / 1Gi)
		},
		{
			name:       "podCPU larger than the whole buffer ceils to 1",
			pool:       parsedPoolPolicy{CPU: 0.10, PodCPU: mustQ("4"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 4000}, // buffer 400m, pod 4000m
			readyNodes: 1,
			want:       1,
		},
		{
			name:       "podMemory larger than the whole buffer ceils to 1",
			pool:       parsedPoolPolicy{Memory: 0.01, PodCPU: mustQ("500m"), PodMemory: mustQ("64Gi")},
			total:      poolAllocatable{memBytes: 16 << 30},
			readyNodes: 1,
			want:       1,
		},
		{
			name:       "ready nodes with zero allocatable fall back to minPods",
			pool:       parsedPoolPolicy{CPU: 0.50, Memory: 0.50, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: 2},
			total:      poolAllocatable{},
			readyNodes: 3,
			want:       2,
		},
		{
			name:       "zero-valued pool policy is zero replicas",
			pool:       parsedPoolPolicy{},
			total:      poolAllocatable{cpuMilli: 8000, memBytes: 32 << 30, gpu: 8, gpuKey: "nvidia.com/gpu"},
			readyNodes: 2,
			want:       0,
		},
		{
			name:       "gpu term rounds up",
			pool:       parsedPoolPolicy{GPU: 0.10, PodGPU: mustQ("2"), PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{gpu: 3, gpuKey: "nvidia.com/gpu"},
			readyNodes: 1,
			want:       1, // ceil(0.3 / 2)
		},
		{
			name:       "cpu buffer rounding: 1 core at 33% with 333m pods",
			pool:       parsedPoolPolicy{CPU: 0.33, PodCPU: mustQ("333m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 1000},
			readyNodes: 1,
			want:       1, // 330 / 333 = 0.99 → 1
		},
		{
			name:       "minPods exactly at the cap is not warned past it",
			pool:       parsedPoolPolicy{PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: int(maxHeadroomReplicas)},
			total:      poolAllocatable{},
			readyNodes: 0,
			want:       int32(maxHeadroomReplicas),
		},
		{
			name:       "cpu term exactly at the cap",
			pool:       parsedPoolPolicy{CPU: 1.0, PodCPU: mustQ("1m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: maxHeadroomReplicas},
			readyNodes: 1,
			want:       int32(maxHeadroomReplicas),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := desiredReplicas(tc.pool, tc.total, tc.readyNodes); got != tc.want {
				t.Errorf("desiredReplicas() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestHrDesiredReplicas_FullFractionNeverFits documents the convergence property behind
// R1-prov-headroom-2: with cpu: 100% the buffer demand equals total allocatable, so once any
// per-node overhead exists (DaemonSets, system reserved) at least one pause pod can never be
// placed no matter how many nodes are added — there is no fixed point. This test passes
// against the current code because it describes the math, not the missing validation (which
// TestHrParsePercent_RejectsNonConvergentFraction covers).
func TestHrDesiredReplicas_FullFractionNeverFits(t *testing.T) {
	pool := parsedPoolPolicy{CPU: 1.0, PodCPU: resource.MustParse("500m"), PodMemory: resource.MustParse("512Mi")}
	const nodeCPU, overhead = int64(4000), int64(300)
	for n := 1; n <= 10; n++ {
		total := poolAllocatable{cpuMilli: int64(n) * nodeCPU}
		replicas := desiredReplicas(pool, total, n)
		demand := int64(replicas) * pool.PodCPU.MilliValue()
		free := total.cpuMilli - int64(n)*overhead
		if demand <= free {
			t.Fatalf("n=%d: buffer demand %dm fits in free %dm — a fixed point exists, which contradicts the finding's model", n, demand, free)
		}
	}
}

// TestHrDesiredReplicas_ConvergesBelowOverheadBound: the positive side of the same model —
// a fraction well below 1 - overhead share does converge, i.e. adding one node eventually
// makes the whole buffer fit.
func TestHrDesiredReplicas_ConvergesBelowOverheadBound(t *testing.T) {
	pool := parsedPoolPolicy{CPU: 0.30, PodCPU: resource.MustParse("500m"), PodMemory: resource.MustParse("512Mi")}
	const nodeCPU, overhead, workload = int64(4000), int64(300), int64(4000)
	for n := 1; n <= 10; n++ {
		total := poolAllocatable{cpuMilli: int64(n) * nodeCPU}
		replicas := desiredReplicas(pool, total, n)
		demand := workload + int64(replicas)*pool.PodCPU.MilliValue()
		free := total.cpuMilli - int64(n)*overhead
		if demand <= free {
			return // converged
		}
	}
	t.Fatal("a 30% buffer did not converge within 10 nodes")
}

// hrPackedNodesFor is the bin-packing part of the ratchet model: the number of nodes needed
// to host workloadMilli plus replicas pause pods when pods may be packed (no spread).
func hrPackedNodesFor(workloadMilli, replicas, podMilli, nodeMilli int64) int {
	demand := workloadMilli + replicas*podMilli
	return int(ceilDiv(float64(demand), nodeMilli))
}

// hrTemplateSpreadsOnHostname reports whether the pod template built by reconcilePool carries
// a kubernetes.io/hostname topology spread — the constraint that keeps every node non-empty.
func hrTemplateSpreadsOnHostname(t *testing.T, pool parsedPoolPolicy) bool {
	t.Helper()
	c := newTestController(livePool(pool.Name), makeNode("n1", pool.Name, standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	var dep appsv1.Deployment
	if err := c.kubeClient.Get(context.Background(), types.NamespacedName{Name: deploymentName(pool.Name), Namespace: testNamespace}, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	for _, tsc := range dep.Spec.Template.Spec.TopologySpreadConstraints {
		if tsc.TopologyKey == corev1.LabelHostname {
			return true
		}
	}
	return false
}

// TestHrDesiredReplicas_FixedPointWithSelfProvisionedNodes is the regression test for
// R1-prov-headroom-1 (the ratchet). Model: a pool of 4-CPU nodes, cpu: 30%, 500m pause pods,
// consolidationPolicy WhenEmpty (the only policy the broker renders). Before a burst the pool
// runs one node's worth of workload on 2 nodes. The burst preempts the buffer, Karpenter
// grows the pool to 5 nodes, the burst finishes and the workload returns to its base level.
//
// Iterate nodes → replicas (the real desiredReplicas) → nodes that WhenEmpty can keep. With a
// hostname spread every node hosts at least one pause pod while replicas >= nodes, so WhenEmpty
// can only remove nodes beyond min(nodes, replicas); without the spread the pods pack and
// WhenEmpty reclaims everything above the packed count. The pool must return to its pre-burst
// size; today it stays at the post-burst size forever (and grows on every further burst).
func TestHrDesiredReplicas_FixedPointWithSelfProvisionedNodes(t *testing.T) {
	pool := parsedPoolPolicy{
		Name:      "ratchet",
		CPU:       0.30,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
	}
	const (
		nodeMilli     = int64(4000)
		workloadMilli = int64(4000) // steady-state real demand: one node's worth
		preBurstNodes = 2
		postBurst     = 5
	)
	podMilli := pool.PodCPU.MilliValue()
	spread := hrTemplateSpreadsOnHostname(t, pool)

	// Sanity: the pre-burst state is itself a fixed point of the model.
	preReplicas := int64(desiredReplicas(pool, poolAllocatable{cpuMilli: preBurstNodes * nodeMilli}, preBurstNodes))
	if got := hrPackedNodesFor(workloadMilli, preReplicas, podMilli, nodeMilli); got != preBurstNodes {
		t.Fatalf("model sanity: pre-burst state needs %d nodes, want %d", got, preBurstNodes)
	}

	nodes := postBurst
	for i := 0; i < 20; i++ {
		replicas := int64(desiredReplicas(pool, poolAllocatable{cpuMilli: int64(nodes) * nodeMilli}, nodes))
		packed := hrPackedNodesFor(workloadMilli, replicas, podMilli, nodeMilli)
		next := packed
		if spread {
			// Every node with at least one pause pod is never "empty": WhenEmpty keeps it.
			next = max(packed, min(nodes, int(replicas)))
		}
		if next == nodes {
			break
		}
		nodes = next
	}
	if nodes != preBurstNodes {
		t.Fatalf("after the burst the pool settles at %d nodes, want %d (pre-burst); spreadOnHostname=%v", nodes, preBurstNodes, spread)
	}
}

// TestHrReconcilePool_NoHostnameSpread is the template half of R1-prov-headroom-1: a hostname
// topology spread keeps one pause pod on every node so WhenEmpty consolidation never fires.
// The template must not carry it (a pack-friendly preference is fine).
func TestHrReconcilePool_NoHostnameSpread(t *testing.T) {
	pool := parsedPoolPolicy{
		Name:      "worker-a",
		CPU:       0.30,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
	}
	if hrTemplateSpreadsOnHostname(t, pool) {
		t.Fatal("pause-pod template carries a kubernetes.io/hostname topologySpreadConstraint; it must not spread across nodes")
	}
}
