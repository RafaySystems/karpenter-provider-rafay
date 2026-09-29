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
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func hrGetDeployment(t *testing.T, c *Controller, pool string) *appsv1.Deployment {
	t.Helper()
	var dep appsv1.Deployment
	key := types.NamespacedName{Name: deploymentName(pool), Namespace: c.podNamespace}
	if err := c.kubeClient.Get(context.Background(), key, &dep); err != nil {
		t.Fatalf("get deployment %s: %v", key, err)
	}
	return &dep
}

func hrSimplePool(name string) parsedPoolPolicy {
	return parsedPoolPolicy{
		Name:      name,
		CPU:       0.50,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
	}
}

// TestHrReconcilePool_RestrictedPodSecurity is the regression test for R1-prov-headroom-6:
// under a namespace enforcing the restricted Pod Security Standard the ReplicaSet's pod
// creates are rejected while the Deployment is accepted, so the buffer silently never
// materialises. The template must satisfy restricted:latest, and a pause pod has no reason to
// carry a projected ServiceAccount token.
func TestHrReconcilePool_RestrictedPodSecurity(t *testing.T) {
	pool := hrSimplePool("worker-a")
	c := newTestController(livePool("worker-a"), makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	spec := hrGetDeployment(t, c, "worker-a").Spec.Template.Spec

	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		t.Errorf("automountServiceAccountToken = %v, want false", spec.AutomountServiceAccountToken)
	}
	if spec.SecurityContext == nil {
		t.Fatal("pod securityContext is nil")
	}
	if spec.SecurityContext.RunAsNonRoot == nil || !*spec.SecurityContext.RunAsNonRoot {
		t.Errorf("runAsNonRoot = %v, want true", spec.SecurityContext.RunAsNonRoot)
	}
	if spec.SecurityContext.SeccompProfile == nil || spec.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Errorf("seccompProfile = %v, want RuntimeDefault", spec.SecurityContext.SeccompProfile)
	}
	if len(spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(spec.Containers))
	}
	csc := spec.Containers[0].SecurityContext
	if csc == nil {
		t.Fatal("container securityContext is nil")
	}
	if csc.AllowPrivilegeEscalation == nil || *csc.AllowPrivilegeEscalation {
		t.Errorf("allowPrivilegeEscalation = %v, want false", csc.AllowPrivilegeEscalation)
	}
	dropsAll := false
	if csc.Capabilities != nil {
		for _, capability := range csc.Capabilities.Drop {
			if capability == "ALL" {
				dropsAll = true
			}
		}
	}
	if !dropsAll {
		t.Errorf("capabilities = %v, want drop [ALL]", csc.Capabilities)
	}
}

// TestHrReconcilePool_ExistingForeignSelectorIsPreserved locks in the "selector is only set
// on create" contract: a pre-existing Deployment with a legacy selector keeps that selector
// (it is immutable server-side), while every controller-owned field is brought in line.
func TestHrReconcilePool_ExistingForeignSelectorIsPreserved(t *testing.T) {
	ctx := context.Background()
	pool := hrSimplePool("worker-a")
	legacySelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy-headroom"}}
	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName("worker-a"),
			Namespace: testNamespace,
			Labels:    map[string]string{"app": "legacy-headroom", "team": "platform"},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: legacySelector,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy-headroom"}},
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"legacy": "true"},
					Tolerations:  []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
					Containers: []corev1.Container{
						{Name: "old-pause", Image: "old/pause:1"},
						{Name: "sidecar", Image: "old/sidecar:1"},
					},
				},
			},
		},
	}
	c := newTestController(livePool("worker-a"), existing, makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	dep := hrGetDeployment(t, c, "worker-a")

	if !reflect.DeepEqual(dep.Spec.Selector, legacySelector) {
		t.Errorf("selector = %v, want the pre-existing (immutable) selector %v", dep.Spec.Selector, legacySelector)
	}
	wantLabels := map[string]string{headroomLabel: "true", headroomPoolLabel: "worker-a"}
	if !reflect.DeepEqual(dep.Labels, wantLabels) {
		t.Errorf("deployment labels = %v, want exactly %v (foreign labels dropped)", dep.Labels, wantLabels)
	}
	tmpl := dep.Spec.Template
	if !reflect.DeepEqual(tmpl.Labels, wantLabels) {
		t.Errorf("template labels = %v, want %v", tmpl.Labels, wantLabels)
	}
	if !reflect.DeepEqual(tmpl.Spec.NodeSelector, map[string]string{karpenterNodePoolLabel: "worker-a"}) {
		t.Errorf("nodeSelector = %v, want only the pool selector", tmpl.Spec.NodeSelector)
	}
	if len(tmpl.Spec.Tolerations) != 0 {
		t.Errorf("tolerations = %v, want the pool's (none) — the legacy blanket toleration must be dropped", tmpl.Spec.Tolerations)
	}
	// Two containers is not the shape this controller owns: it is reset to the single pause
	// container.
	if len(tmpl.Spec.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(tmpl.Spec.Containers))
	}
	if ctr := tmpl.Spec.Containers[0]; ctr.Name != "pause" || ctr.Image != pauseImage {
		t.Errorf("container = %s/%s, want pause/%s", ctr.Name, ctr.Image, pauseImage)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 4 { // ceil(0.5*4000/500)
		t.Errorf("replicas = %v, want 4", dep.Spec.Replicas)
	}
}

// TestHrReconcilePool_SingleExistingContainerKeepsUnownedFields: the mutate fn edits the
// fetched container in place, so fields the controller does not own (env, probes,
// server-defaulted termination settings) survive; only name/image/pullPolicy/resources are
// rewritten.
func TestHrReconcilePool_SingleExistingContainerKeepsUnownedFields(t *testing.T) {
	ctx := context.Background()
	pool := hrSimplePool("worker-a")
	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: deploymentName("worker-a"), Namespace: testNamespace},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{headroomLabel: "true", headroomPoolLabel: "worker-a"}},
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:                   "renamed",
						Image:                  "other/pause:0",
						ImagePullPolicy:        corev1.PullAlways,
						TerminationMessagePath: "/dev/termination-log",
						Env:                    []corev1.EnvVar{{Name: "KEEP", Value: "me"}},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("9")},
						},
					}},
				},
			},
		},
	}
	c := newTestController(livePool("worker-a"), existing, makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	ctr := hrGetDeployment(t, c, "worker-a").Spec.Template.Spec.Containers[0]
	if ctr.Name != "pause" || ctr.Image != pauseImage || ctr.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("owned fields not rewritten: name=%s image=%s pullPolicy=%s", ctr.Name, ctr.Image, ctr.ImagePullPolicy)
	}
	if ctr.TerminationMessagePath != "/dev/termination-log" {
		t.Errorf("terminationMessagePath = %q, want the server default preserved", ctr.TerminationMessagePath)
	}
	if len(ctr.Env) != 1 || ctr.Env[0].Name != "KEEP" {
		t.Errorf("env = %v, want the unowned env preserved", ctr.Env)
	}
	if q := ctr.Resources.Requests[corev1.ResourceCPU]; q.Cmp(resource.MustParse("500m")) != 0 {
		t.Errorf("cpu request = %v, want the per-pod size 500m (resources are fully owned)", q.String())
	}
	if q := ctr.Resources.Limits[corev1.ResourceMemory]; q.Cmp(resource.MustParse("512Mi")) != 0 {
		t.Errorf("memory limit = %v, want 512Mi", q.String())
	}
}

// TestHrReconcilePool_LimitsAreAnIndependentCopy: requests and limits must be equal (Guaranteed
// QoS) but not the same map, so a later mutation of one cannot silently desync the other.
func TestHrReconcilePool_LimitsAreAnIndependentCopy(t *testing.T) {
	pool := hrSimplePool("worker-a")
	c := newTestController(livePool("worker-a"), makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	res := hrGetDeployment(t, c, "worker-a").Spec.Template.Spec.Containers[0].Resources
	if !reflect.DeepEqual(res.Requests, res.Limits) {
		t.Fatalf("requests %v != limits %v", res.Requests, res.Limits)
	}
	res.Requests[corev1.ResourceCPU] = resource.MustParse("9")
	if q := res.Limits[corev1.ResourceCPU]; q.Cmp(resource.MustParse("500m")) != 0 {
		t.Errorf("mutating requests changed limits (%v): they alias the same map", q.String())
	}
}

// TestHrReconcilePool_MultipleTolerationsAndSpreadSelector: several configured tolerations are
// carried verbatim in order, and the pack-affinity selector targets only this pool's pods (not
// every headroom pod in the namespace).
func TestHrReconcilePool_MultipleTolerationsAndSpreadSelector(t *testing.T) {
	pool := hrSimplePool("worker-a")
	pool.Tolerations = []corev1.Toleration{
		{Key: "a", Operator: corev1.TolerationOpExists},
		{Key: "b", Operator: corev1.TolerationOpEqual, Value: "1", Effect: corev1.TaintEffectNoExecute},
		{Key: "c", Operator: "", Value: "2", Effect: corev1.TaintEffectPreferNoSchedule},
	}
	c := newTestController(livePool("worker-a"), makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	spec := hrGetDeployment(t, c, "worker-a").Spec.Template.Spec
	if !reflect.DeepEqual(spec.Tolerations, pool.Tolerations) {
		t.Errorf("tolerations = %+v, want %+v", spec.Tolerations, pool.Tolerations)
	}
	for _, tsc := range spec.TopologySpreadConstraints {
		if tsc.LabelSelector == nil {
			t.Errorf("spread constraint without a selector would count every pod on the node: %+v", tsc)
			continue
		}
		if got := tsc.LabelSelector.MatchLabels[headroomPoolLabel]; got != "worker-a" {
			t.Errorf("spread selector pool label = %q, want worker-a", got)
		}
	}
	if spec.Affinity == nil || spec.Affinity.PodAffinity == nil {
		t.Fatal("pause pods must carry a pack-friendly podAffinity")
	}
	for _, term := range spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
		if term.PodAffinityTerm.LabelSelector == nil {
			t.Errorf("affinity term without a selector would pack next to any pod: %+v", term)
			continue
		}
		if got := term.PodAffinityTerm.LabelSelector.MatchLabels[headroomPoolLabel]; got != "worker-a" {
			t.Errorf("affinity selector pool label = %q, want worker-a", got)
		}
	}
}

// TestHrReconcilePool_ReplicasTrackNodeChanges: the replica count follows the pool's ready
// allocatable between reconciles (scale up when a node joins, down when one leaves).
func TestHrReconcilePool_ReplicasTrackNodeChanges(t *testing.T) {
	ctx := context.Background()
	pool := hrSimplePool("worker-a")
	n1 := makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true)
	c := newTestController(livePool("worker-a"), n1)
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	if r := hrGetDeployment(t, c, "worker-a").Spec.Replicas; r == nil || *r != 4 {
		t.Fatalf("replicas = %v, want 4 with one 4-CPU node", r)
	}

	n2 := makeNode("n2", "worker-a", standardAlloc("4", "16Gi"), true)
	if err := c.kubeClient.Create(ctx, n2); err != nil {
		t.Fatalf("create node: %v", err)
	}
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	if r := hrGetDeployment(t, c, "worker-a").Spec.Replicas; r == nil || *r != 8 {
		t.Fatalf("replicas = %v, want 8 with two 4-CPU nodes", r)
	}

	if err := c.kubeClient.Delete(ctx, n1); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	if err := c.kubeClient.Delete(ctx, n2); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	if r := hrGetDeployment(t, c, "worker-a").Spec.Replicas; r == nil || *r != 0 {
		t.Fatalf("replicas = %v, want 0 with no ready nodes and no minPods", r)
	}
}

// TestHrReconcilePool_PoolNameIsolation: nodes of a pool whose name shares a prefix with the
// configured pool must not be counted (exact label match, not prefix).
func TestHrReconcilePool_PoolNameIsolation(t *testing.T) {
	pool := hrSimplePool("worker")
	c := newTestController(
		livePool("worker"),
		makeNode("n1", "worker", standardAlloc("4", "16Gi"), true),
		makeNode("n2", "worker-2", standardAlloc("64", "256Gi"), true),
		makeNode("n3", "big-worker", standardAlloc("64", "256Gi"), true),
	)
	if err := c.reconcilePool(context.Background(), pool); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	if r := hrGetDeployment(t, c, "worker").Spec.Replicas; r == nil || *r != 4 {
		t.Fatalf("replicas = %v, want 4 (only the exact-match node counts)", r)
	}
}
