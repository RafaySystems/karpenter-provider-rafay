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
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const testNamespace = "karpenter"

func makeNode(name, pool string, alloc corev1.ResourceList, ready bool) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{karpenterNodePoolLabel: pool},
		},
		Status: corev1.NodeStatus{
			Allocatable: alloc,
			Conditions:  []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}},
		},
	}
}

func standardAlloc(cpu, memory string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
}

func newTestController(objs ...client.Object) *Controller {
	return &Controller{
		kubeClient:      fake.NewClientBuilder().WithObjects(objs...).Build(),
		podNamespace:    testNamespace,
		configNamespace: testNamespace,
	}
}

// --- replica math ---

func TestDesiredReplicas(t *testing.T) {
	mustQ := resource.MustParse
	cases := []struct {
		name       string
		pool       parsedPoolPolicy
		total      poolAllocatable
		readyNodes int
		want       int32
	}{
		{
			name:       "cpu only exact division",
			pool:       parsedPoolPolicy{CPU: 0.30, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 10000},
			readyNodes: 2,
			want:       6, // 3000m / 500m
		},
		{
			name:       "cpu ceil rounds up",
			pool:       parsedPoolPolicy{CPU: 1.0, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 1001},
			readyNodes: 1,
			want:       3, // ceil(1001 / 500) = ceil(2.002)
		},
		{
			name: "memory dominates cpu (max over resources)",
			pool: parsedPoolPolicy{CPU: 0.30, Memory: 0.20, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			// 12 cores, 48Gi: cpu → ceil(3600/500)=8, memory → ceil(0.2*48Gi/512Mi)=ceil(19.2)=20
			total:      poolAllocatable{cpuMilli: 12000, memBytes: 48 * 1024 * 1024 * 1024},
			readyNodes: 3,
			want:       20,
		},
		{
			name:       "gpu buffer with podGPU",
			pool:       parsedPoolPolicy{GPU: 0.50, PodGPU: mustQ("1"), PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{gpu: 16, gpuKey: "nvidia.com/gpu"},
			readyNodes: 2,
			want:       8, // 0.5 * 16 / 1
		},
		{
			name:       "gpu fraction without podGPU is ignored",
			pool:       parsedPoolPolicy{GPU: 0.50, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{gpu: 16, gpuKey: "nvidia.com/gpu"},
			readyNodes: 2,
			want:       0,
		},
		{
			name:       "minPods floors the computed count",
			pool:       parsedPoolPolicy{CPU: 0.10, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: 3},
			total:      poolAllocatable{cpuMilli: 4000},
			readyNodes: 1,
			want:       3, // cpu math gives ceil(400/500)=1, floored at 3
		},
		{
			name:       "minPods does not cap a larger computed count",
			pool:       parsedPoolPolicy{CPU: 0.50, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: 1},
			total:      poolAllocatable{cpuMilli: 10000},
			readyNodes: 2,
			want:       10, // 5000m / 500m
		},
		{
			name:       "zero ready nodes cold start uses minPods",
			pool:       parsedPoolPolicy{CPU: 0.30, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: 2},
			total:      poolAllocatable{},
			readyNodes: 0,
			want:       2,
		},
		{
			name:       "zero ready nodes without minPods is zero",
			pool:       parsedPoolPolicy{CPU: 0.30, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{},
			readyNodes: 0,
			want:       0,
		},
		{
			name: "ready nodes but allocatable still counted only when fraction set",
			pool: parsedPoolPolicy{PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")},
			// no fractions configured → nothing to buffer
			total:      poolAllocatable{cpuMilli: 64000, memBytes: 1 << 40},
			readyNodes: 4,
			want:       0,
		},
		{
			name: "absurd count is clamped to the cap, not overflowed into int32",
			// A 1m per-pod slice against a large pool asks for ~64M replicas; unclamped, the
			// int64→int32 cast would wrap into garbage (here: a negative count).
			pool:       parsedPoolPolicy{CPU: 1.0, PodCPU: mustQ("1m"), PodMemory: mustQ("512Mi")},
			total:      poolAllocatable{cpuMilli: 64_000_000},
			readyNodes: 1000,
			want:       int32(maxHeadroomReplicas),
		},
		{
			name:       "absurd minPods is clamped to the cap",
			pool:       parsedPoolPolicy{PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: math.MaxInt32},
			total:      poolAllocatable{},
			readyNodes: 0,
			want:       int32(maxHeadroomReplicas),
		},
		{
			name:       "negative minPods is clamped to zero",
			pool:       parsedPoolPolicy{PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi"), MinPods: -5},
			total:      poolAllocatable{},
			readyNodes: 0,
			want:       0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := desiredReplicas(tc.pool, tc.total, tc.readyNodes)
			if got != tc.want {
				t.Errorf("desiredReplicas() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestSumAllocatableAndFilterReady(t *testing.T) {
	nodes := []corev1.Node{
		*makeNode("n1", "p", standardAlloc("4", "16Gi"), true),
		*makeNode("n2", "p", standardAlloc("4", "16Gi"), true),
		*makeNode("n3", "p", standardAlloc("32", "256Gi"), false), // NotReady: excluded
		{ObjectMeta: metav1.ObjectMeta{Name: "n4"}},               // no Ready condition: excluded
	}
	ready := filterReady(nodes)
	if len(ready) != 2 {
		t.Fatalf("filterReady: got %d nodes, want 2", len(ready))
	}

	total := sumAllocatable(ready)
	if total.cpuMilli != 8000 {
		t.Errorf("cpuMilli = %d, want 8000", total.cpuMilli)
	}
	if want := int64(32) * 1024 * 1024 * 1024; total.memBytes != want {
		t.Errorf("memBytes = %d, want %d", total.memBytes, want)
	}
	if total.gpu != 0 {
		t.Errorf("gpu = %d, want 0", total.gpu)
	}
	// No GPU node ⇒ no GPU key. It must NOT default to nvidia.com/gpu: a guessed key would
	// stamp an unsatisfiable GPU request on the pause pods (Karpenter cannot provision GPU
	// capacity), and would simply be wrong on an amd.com/gpu pool.
	if total.gpuKey != "" {
		t.Errorf("gpuKey = %q, want empty (no GPU nodes in the pool)", total.gpuKey)
	}
}

func TestSumAllocatable_DetectsGPUKey(t *testing.T) {
	alloc := standardAlloc("8", "32Gi")
	alloc["amd.com/gpu"] = resource.MustParse("4")
	nodes := []corev1.Node{
		*makeNode("g1", "p", alloc, true),
		*makeNode("g2", "p", alloc, true),
	}
	total := sumAllocatable(nodes)
	if total.gpu != 8 {
		t.Errorf("gpu = %d, want 8", total.gpu)
	}
	if total.gpuKey != corev1.ResourceName("amd.com/gpu") {
		t.Errorf("gpuKey = %q, want amd.com/gpu", total.gpuKey)
	}
}

func TestPerPodRequests(t *testing.T) {
	pool := parsedPoolPolicy{
		PodCPU:    resource.MustParse("250m"),
		PodMemory: resource.MustParse("1Gi"),
	}
	reqs := perPodRequests(pool, "nvidia.com/gpu")
	if len(reqs) != 2 {
		t.Fatalf("expected cpu+memory only, got %v", reqs)
	}
	if q := reqs[corev1.ResourceCPU]; q.Cmp(resource.MustParse("250m")) != 0 {
		t.Errorf("cpu = %v, want 250m", q.String())
	}
	if q := reqs[corev1.ResourceMemory]; q.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Errorf("memory = %v, want 1Gi", q.String())
	}

	pool.PodGPU = resource.MustParse("2")
	reqs = perPodRequests(pool, "amd.com/gpu")
	if q, ok := reqs[corev1.ResourceName("amd.com/gpu")]; !ok || q.Cmp(resource.MustParse("2")) != 0 {
		t.Errorf("gpu request = %v (present=%v), want 2 under amd.com/gpu", q.String(), ok)
	}

	// podGPU set but the pool has no GPU node to name the resource: drop the GPU request
	// rather than guessing a key nothing can satisfy.
	reqs = perPodRequests(pool, "")
	if len(reqs) != 2 {
		t.Errorf("requests with an unknown GPU key = %v, want cpu+memory only", reqs)
	}
	for name := range reqs {
		if name != corev1.ResourceCPU && name != corev1.ResourceMemory {
			t.Errorf("unexpected request %q with an unknown GPU key", name)
		}
	}
}

// --- deployment shape ---

func TestReconcilePool_DeploymentShape(t *testing.T) {
	tolerations := []corev1.Toleration{{
		Key:      "example.com/dedicated",
		Operator: corev1.TolerationOpEqual,
		Value:    "headroom",
		Effect:   corev1.TaintEffectNoSchedule,
	}}
	pool := parsedPoolPolicy{
		Name:        "worker-a",
		CPU:         0.30,
		GPU:         0.50,
		PodCPU:      resource.MustParse("500m"),
		PodMemory:   resource.MustParse("512Mi"),
		PodGPU:      resource.MustParse("1"),
		Tolerations: tolerations,
	}
	gpuAlloc := standardAlloc("4", "16Gi")
	gpuAlloc["nvidia.com/gpu"] = resource.MustParse("2")
	c := newTestController(
		makeNode("n1", "worker-a", gpuAlloc, true),
		makeNode("other", "worker-b", standardAlloc("64", "256Gi"), true), // different pool: excluded
	)

	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}

	var dep appsv1.Deployment
	key := types.NamespacedName{Name: "headroom-worker-a", Namespace: testNamespace}
	if err := c.kubeClient.Get(context.Background(), key, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}

	wantLabels := map[string]string{headroomLabel: "true", headroomPoolLabel: "worker-a"}
	if !reflect.DeepEqual(dep.Labels, wantLabels) {
		t.Errorf("deployment labels = %v, want %v", dep.Labels, wantLabels)
	}
	if dep.Spec.Selector == nil || !reflect.DeepEqual(dep.Spec.Selector.MatchLabels, wantLabels) {
		t.Errorf("selector = %v, want matchLabels %v", dep.Spec.Selector, wantLabels)
	}
	// CPU: ceil(0.30*4000/500)=3; GPU: ceil(0.50*2/1)=1 → max is 3.
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		t.Errorf("replicas = %v, want 3", dep.Spec.Replicas)
	}

	podSpec := dep.Spec.Template.Spec
	if !reflect.DeepEqual(dep.Spec.Template.Labels, wantLabels) {
		t.Errorf("pod template labels = %v, want %v", dep.Spec.Template.Labels, wantLabels)
	}
	if podSpec.PriorityClassName != priorityClassName {
		t.Errorf("priorityClassName = %q, want %q", podSpec.PriorityClassName, priorityClassName)
	}
	if podSpec.PreemptionPolicy == nil || *podSpec.PreemptionPolicy != corev1.PreemptNever {
		t.Errorf("preemptionPolicy = %v, want Never", podSpec.PreemptionPolicy)
	}
	if podSpec.TerminationGracePeriodSeconds == nil || *podSpec.TerminationGracePeriodSeconds != 0 {
		t.Errorf("terminationGracePeriodSeconds = %v, want 0", podSpec.TerminationGracePeriodSeconds)
	}
	wantNodeSelector := map[string]string{karpenterNodePoolLabel: "worker-a"}
	if !reflect.DeepEqual(podSpec.NodeSelector, wantNodeSelector) {
		t.Errorf("nodeSelector = %v, want %v", podSpec.NodeSelector, wantNodeSelector)
	}

	if len(podSpec.TopologySpreadConstraints) != 1 {
		t.Fatalf("topologySpreadConstraints = %v, want exactly 1", podSpec.TopologySpreadConstraints)
	}
	tsc := podSpec.TopologySpreadConstraints[0]
	if tsc.MaxSkew != 1 || tsc.TopologyKey != corev1.LabelHostname || tsc.WhenUnsatisfiable != corev1.ScheduleAnyway {
		t.Errorf("topology spread = %+v, want maxSkew=1 key=%s whenUnsatisfiable=ScheduleAnyway", tsc, corev1.LabelHostname)
	}
	wantTSCSelector := map[string]string{headroomPoolLabel: "worker-a"}
	if tsc.LabelSelector == nil || !reflect.DeepEqual(tsc.LabelSelector.MatchLabels, wantTSCSelector) {
		t.Errorf("topology spread selector = %v, want matchLabels %v", tsc.LabelSelector, wantTSCSelector)
	}

	// Tolerations must be exactly the configured ones — no blanket Exists toleration.
	if !reflect.DeepEqual(podSpec.Tolerations, tolerations) {
		t.Errorf("tolerations = %+v, want exactly %+v", podSpec.Tolerations, tolerations)
	}
	for _, tol := range podSpec.Tolerations {
		if tol.Operator == corev1.TolerationOpExists && tol.Key == "" {
			t.Errorf("found blanket Exists toleration: %+v", tol)
		}
	}

	if len(podSpec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(podSpec.Containers))
	}
	ctr := podSpec.Containers[0]
	if ctr.Image != pauseImage {
		t.Errorf("image = %q, want %q", ctr.Image, pauseImage)
	}
	// Guaranteed QoS: requests == limits, carrying the per-pod sizes plus the GPU slice.
	if !reflect.DeepEqual(ctr.Resources.Requests, ctr.Resources.Limits) {
		t.Errorf("requests %v != limits %v (Guaranteed QoS broken)", ctr.Resources.Requests, ctr.Resources.Limits)
	}
	if q := ctr.Resources.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("500m")) != 0 {
		t.Errorf("cpu request = %v, want 500m", q.String())
	}
	if q := ctr.Resources.Requests[corev1.ResourceMemory]; q.Cmp(resource.MustParse("512Mi")) != 0 {
		t.Errorf("memory request = %v, want 512Mi", q.String())
	}
	if q := ctr.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")]; q.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("gpu request = %v, want 1", q.String())
	}
}

func TestReconcilePool_NoTolerationsNoGPU(t *testing.T) {
	pool := parsedPoolPolicy{
		Name:      "worker-b",
		CPU:       0.50,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
	}
	c := newTestController(makeNode("n1", "worker-b", standardAlloc("2", "8Gi"), true))

	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}

	var dep appsv1.Deployment
	key := types.NamespacedName{Name: "headroom-worker-b", Namespace: testNamespace}
	if err := c.kubeClient.Get(context.Background(), key, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 { // ceil(0.5*2000/500) = 2
		t.Errorf("replicas = %v, want 2", dep.Spec.Replicas)
	}
	if len(dep.Spec.Template.Spec.Tolerations) != 0 {
		t.Errorf("tolerations = %+v, want none", dep.Spec.Template.Spec.Tolerations)
	}
	reqs := dep.Spec.Template.Spec.Containers[0].Resources.Requests
	if len(reqs) != 2 {
		t.Errorf("requests = %v, want cpu+memory only (no GPU)", reqs)
	}
}

func TestReconcilePool_ColdStartMinPods(t *testing.T) {
	pool := parsedPoolPolicy{
		Name:      "empty-pool",
		CPU:       0.30,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
		MinPods:   2,
	}
	// One node exists for the pool but is NotReady → still cold.
	c := newTestController(makeNode("n1", "empty-pool", standardAlloc("4", "16Gi"), false))

	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}

	var dep appsv1.Deployment
	key := types.NamespacedName{Name: "headroom-empty-pool", Namespace: testNamespace}
	if err := c.kubeClient.Get(context.Background(), key, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Errorf("replicas = %v, want minPods floor of 2", dep.Spec.Replicas)
	}
}

// --- full sync ---

func TestReconcile_FullSync(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: testNamespace},
		Data: map[string]string{"policy": `
pools:
  - name: pool-a
    cpu: 50%
    minPods: 1
`},
	}
	staleDep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      "headroom-removed-pool",
		Namespace: testNamespace,
		Labels:    map[string]string{headroomLabel: "true", headroomPoolLabel: "removed-pool"},
	}}
	legacyPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "headroom-legacy-0",
		Namespace: testNamespace,
		Labels:    map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"},
	}}
	ownedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "headroom-owned-0",
		Namespace: testNamespace,
		Labels:    map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "headroom-pool-a-abc", UID: "uid-1",
		}},
	}}
	c := newTestController(
		cm,
		makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true),
		staleDep,
		legacyPod,
		ownedPod,
	)

	res, err := c.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: syncRequestName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}

	ctx := context.Background()
	var dep appsv1.Deployment
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: "headroom-pool-a", Namespace: testNamespace}, &dep); err != nil {
		t.Fatalf("expected deployment for pool-a: %v", err)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 4 { // ceil(0.5*4000/500m default) = 4
		t.Errorf("replicas = %v, want 4", dep.Spec.Replicas)
	}

	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(staleDep), &appsv1.Deployment{}); err == nil {
		t.Error("stale deployment for removed pool was not deleted")
	}
	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(legacyPod), &corev1.Pod{}); err == nil {
		t.Error("legacy bare pod was not deleted")
	}
	if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(ownedPod), &corev1.Pod{}); err != nil {
		t.Errorf("ReplicaSet-owned pod must be kept, got: %v", err)
	}
}

func TestReconcile_MissingConfigMap(t *testing.T) {
	c := newTestController()
	res, err := c.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: syncRequestName},
	})
	if err != nil {
		t.Fatalf("Reconcile without ConfigMap should not error, got: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}
}

// TestReconcile_BrokenPolicyKeepsDeployments is the fail-open GC regression test: a policy
// that no longer parses must NOT be read as "no pools configured", which would garbage-collect
// every headroom Deployment (all buffer capacity torn down by a YAML typo).
func TestReconcile_BrokenPolicyKeepsDeployments(t *testing.T) {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: testNamespace},
		Data:       map[string]string{"policy": "pools:\n  - name: pool-a\n    cpu: thirty percent\n"},
	}
	existingDep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      "headroom-pool-a",
		Namespace: testNamespace,
		Labels:    map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"},
	}}
	c := newTestController(cm, existingDep, makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true))

	_, err := c.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: syncRequestName},
	})
	if err == nil {
		t.Fatal("Reconcile with an unparseable policy must return an error, got nil")
	}
	if err := c.kubeClient.Get(context.Background(), client.ObjectKeyFromObject(existingDep), &appsv1.Deployment{}); err != nil {
		t.Fatalf("headroom Deployment was garbage-collected because of a broken policy: %v", err)
	}
}

// TestReconcilePool_NoSpuriousUpdate guards the "updated every reconcile" regression: the
// mutate fn must only touch controller-owned fields, leaving API-server defaults intact, so a
// steady-state reconcile is a no-op instead of an Update.
func TestReconcilePool_NoSpuriousUpdate(t *testing.T) {
	ctx := context.Background()
	pool := parsedPoolPolicy{
		Name:      "worker-a",
		CPU:       0.50,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
	}
	c := newTestController(makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (create): %v", err)
	}

	key := types.NamespacedName{Name: "headroom-worker-a", Namespace: testNamespace}
	var dep appsv1.Deployment
	if err := c.kubeClient.Get(ctx, key, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	// Stand in for the API server's defaulting webhook, which the fake client does not run.
	dep.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	dep.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	dep.Spec.Template.Spec.SchedulerName = "default-scheduler"
	dep.Spec.Template.Spec.Containers[0].TerminationMessagePath = "/dev/termination-log"
	dep.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	if err := c.kubeClient.Update(ctx, &dep); err != nil {
		t.Fatalf("apply server defaults: %v", err)
	}
	if err := c.kubeClient.Get(ctx, key, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	rvBefore := dep.ResourceVersion

	// Steady state: nothing the controller owns changed.
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (steady state): %v", err)
	}
	var after appsv1.Deployment
	if err := c.kubeClient.Get(ctx, key, &after); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if after.ResourceVersion != rvBefore {
		t.Errorf("steady-state reconcile issued an Update (resourceVersion %s → %s); the mutate fn must not clobber server-defaulted fields",
			rvBefore, after.ResourceVersion)
	}
	if after.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyAlways ||
		after.Spec.Template.Spec.DNSPolicy != corev1.DNSClusterFirst ||
		after.Spec.Template.Spec.SchedulerName != "default-scheduler" {
		t.Errorf("server-defaulted pod template fields were wiped: %+v", after.Spec.Template.Spec)
	}

	// A real change still applies.
	pool.CPU = 1.0
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (changed): %v", err)
	}
	if err := c.kubeClient.Get(ctx, key, &after); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if after.Spec.Replicas == nil || *after.Spec.Replicas != 8 { // ceil(1.0*4000/500)
		t.Errorf("replicas = %v, want 8 after the policy change", after.Spec.Replicas)
	}
	if after.Spec.Template.Spec.SchedulerName != "default-scheduler" {
		t.Errorf("an Update wiped the server-defaulted schedulerName: %q", after.Spec.Template.Spec.SchedulerName)
	}
}

// TestReconcilePool_GPUPoolWithoutGPUNodes covers the GPU limitation: with no GPU node in the
// pool there is no GPU resource name to request, so the pause pods carry cpu/memory only
// (a guessed nvidia.com/gpu request would strand them in Pending forever).
func TestReconcilePool_GPUPoolWithoutGPUNodes(t *testing.T) {
	pool := parsedPoolPolicy{
		Name:      "gpu-pool",
		CPU:       0.50,
		GPU:       0.50,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
		PodGPU:    resource.MustParse("1"),
		MinPods:   1,
	}
	c := newTestController(makeNode("n1", "gpu-pool", standardAlloc("4", "16Gi"), true)) // no GPU capacity
	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}

	var dep appsv1.Deployment
	key := types.NamespacedName{Name: "headroom-gpu-pool", Namespace: testNamespace}
	if err := c.kubeClient.Get(context.Background(), key, &dep); err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	reqs := dep.Spec.Template.Spec.Containers[0].Resources.Requests
	if len(reqs) != 2 {
		t.Errorf("requests = %v, want cpu+memory only (no GPU nodes ⇒ no GPU request)", reqs)
	}
	for _, gpu := range gpuResourceNames {
		if _, ok := reqs[gpu]; ok {
			t.Errorf("pause pod requests %q on a pool with no GPU nodes", gpu)
		}
	}
}

// --- priority class ---

func TestReconcile_EnsuresPriorityClass(t *testing.T) {
	c := newTestController()
	if _, err := c.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: syncRequestName},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	var pc schedulingv1.PriorityClass
	if err := c.kubeClient.Get(context.Background(), types.NamespacedName{Name: priorityClassName}, &pc); err != nil {
		t.Fatalf("Reconcile must create the PriorityClass (it is not only created by the startup runnable): %v", err)
	}
	if pc.Value != priorityValue {
		t.Errorf("value = %d, want %d", pc.Value, priorityValue)
	}
	if pc.PreemptionPolicy == nil || *pc.PreemptionPolicy != corev1.PreemptNever {
		t.Errorf("preemptionPolicy = %v, want Never", pc.PreemptionPolicy)
	}
}

func TestEnsurePriorityClass_ExistingIsNotMutated(t *testing.T) {
	ctx := context.Background()
	preempt := corev1.PreemptLowerPriority
	existing := &schedulingv1.PriorityClass{
		ObjectMeta:       metav1.ObjectMeta{Name: priorityClassName},
		Value:            100, // drifted: positive value makes headroom pods un-preemptable
		PreemptionPolicy: &preempt,
	}
	c := newTestController(existing)

	// Drift is reported (klog.Warningf) but never repaired — we hold no update verb, and the
	// Value is immutable anyway. It must not be an error either: headroom keeps running.
	if err := c.ensurePriorityClass(ctx); err != nil {
		t.Fatalf("ensurePriorityClass with a drifted existing class: %v", err)
	}
	var pc schedulingv1.PriorityClass
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: priorityClassName}, &pc); err != nil {
		t.Fatalf("get priority class: %v", err)
	}
	if pc.Value != 100 || pc.PreemptionPolicy == nil || *pc.PreemptionPolicy != corev1.PreemptLowerPriority {
		t.Errorf("existing PriorityClass was mutated: value=%d preemptionPolicy=%v", pc.Value, pc.PreemptionPolicy)
	}
}
