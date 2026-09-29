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
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// hrNewInterceptedController is newTestController with interceptor funcs on the fake client,
// for injecting API failures on specific calls.
func hrNewInterceptedController(funcs interceptor.Funcs, objs ...client.Object) *Controller {
	return &Controller{
		kubeClient:      fake.NewClientBuilder().WithScheme(hrScheme()).WithObjects(objs...).WithInterceptorFuncs(funcs).Build(),
		podNamespace:    testNamespace,
		configNamespace: testNamespace,
	}
}

func hrSyncRequest() reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: syncRequestName}}
}

func hrPod(name, namespace string, labels map[string]string, owners ...metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:            name,
		Namespace:       namespace,
		Labels:          labels,
		OwnerReferences: owners,
	}}
}

func hrDeployment(name, namespace string, labels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels}}
}

func hrExists(t *testing.T, c *Controller, obj client.Object) bool {
	t.Helper()
	fresh := obj.DeepCopyObject().(client.Object)
	err := c.kubeClient.Get(context.Background(), client.ObjectKeyFromObject(obj), fresh)
	if err == nil {
		return true
	}
	if kerrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("get %s: %v", client.ObjectKeyFromObject(obj), err)
	return false
}

func hrPolicyConfigMap(policy string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: testNamespace},
		Data:       map[string]string{"policy": policy},
	}
}

// --- cleanupLegacyPods ---

func TestHrCleanupLegacyPods_NeverDeletesNonLegacyObjects(t *testing.T) {
	headroomLabels := map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"}
	rsOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "headroom-pool-a-abc", UID: "rs-1"}
	jobOwner := metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "batch-1", UID: "job-1"}

	legacy := hrPod("headroom-legacy-0", testNamespace, headroomLabels)
	legacyNoPool := hrPod("headroom-legacy-1", testNamespace, map[string]string{headroomLabel: "true"})
	rsOwned := hrPod("headroom-owned-0", testNamespace, headroomLabels, rsOwner)
	jobOwned := hrPod("headroom-job-0", testNamespace, headroomLabels, jobOwner)
	otherNamespace := hrPod("headroom-legacy-0", "default", headroomLabels)
	unlabelled := hrPod("headroom-lookalike", testNamespace, map[string]string{headroomPoolLabel: "pool-a"})
	wrongValue := hrPod("headroom-false", testNamespace, map[string]string{headroomLabel: "false"})
	plain := hrPod("nginx", testNamespace, map[string]string{"app": "nginx"})

	c := newTestController(legacy, legacyNoPool, rsOwned, jobOwned, otherNamespace, unlabelled, wrongValue, plain)
	if err := c.cleanupLegacyPods(context.Background()); err != nil {
		t.Fatalf("cleanupLegacyPods: %v", err)
	}

	for _, p := range []*corev1.Pod{legacy, legacyNoPool} {
		if hrExists(t, c, p) {
			t.Errorf("legacy bare pod %s/%s was not deleted", p.Namespace, p.Name)
		}
	}
	kept := map[string]*corev1.Pod{
		"ReplicaSet-owned":            rsOwned,
		"Job-owned (any owner)":       jobOwned,
		"other namespace":             otherNamespace,
		"missing headroom label":      unlabelled,
		"headroom label not \"true\"": wrongValue,
		"unrelated pod":               plain,
	}
	for why, p := range kept {
		if !hrExists(t, c, p) {
			t.Errorf("%s pod %s/%s must be kept but was deleted", why, p.Namespace, p.Name)
		}
	}
}

func TestHrCleanupLegacyPods_DeleteErrorIsToleratedAndOthersProceed(t *testing.T) {
	headroomLabels := map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"}
	stuck := hrPod("headroom-legacy-stuck", testNamespace, headroomLabels)
	other := hrPod("headroom-legacy-other", testNamespace, headroomLabels)
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == stuck.Name {
				return kerrors.NewForbidden(schema.GroupResource{Resource: "pods"}, obj.GetName(), errors.New("rbac"))
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}
	c := hrNewInterceptedController(funcs, stuck, other)
	if err := c.cleanupLegacyPods(context.Background()); err != nil {
		t.Fatalf("a failed pod delete must not fail the sync, got: %v", err)
	}
	if !hrExists(t, c, stuck) {
		t.Error("the pod whose delete failed should still exist")
	}
	if hrExists(t, c, other) {
		t.Error("the loop must continue past a failed delete and remove the other legacy pod")
	}
}

func TestHrCleanupLegacyPods_NotFoundOnDeleteIsIgnored(t *testing.T) {
	legacy := hrPod("headroom-legacy-0", testNamespace, map[string]string{headroomLabel: "true"})
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return kerrors.NewNotFound(schema.GroupResource{Resource: "pods"}, obj.GetName())
		},
	}
	c := hrNewInterceptedController(funcs, legacy)
	if err := c.cleanupLegacyPods(context.Background()); err != nil {
		t.Fatalf("NotFound on delete (already gone) must be silent, got: %v", err)
	}
}

func TestHrCleanupLegacyPods_ListErrorIsReturned(t *testing.T) {
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return kerrors.NewServiceUnavailable("apiserver down")
			}
			return cl.List(ctx, list, opts...)
		},
	}
	c := hrNewInterceptedController(funcs)
	err := c.cleanupLegacyPods(context.Background())
	if err == nil {
		t.Fatal("expected the list error to be returned")
	}
	if !strings.Contains(err.Error(), "list headroom pods") {
		t.Errorf("error %q should be wrapped with context", err.Error())
	}
}

// --- cleanupStaleDeployments ---

func TestHrCleanupStaleDeployments_OnlyRemovesLabelledUnconfiguredDeployments(t *testing.T) {
	configured := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	stale := hrDeployment(deploymentName("pool-gone"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-gone"})
	noPoolLabel := hrDeployment("headroom-orphan", testNamespace, map[string]string{headroomLabel: "true"})
	lookalikeName := hrDeployment(deploymentName("pool-lookalike"), testNamespace, map[string]string{"app": "user-headroom"})
	wrongValue := hrDeployment("headroom-off", testNamespace, map[string]string{headroomLabel: "false", headroomPoolLabel: "pool-gone"})
	otherNamespace := hrDeployment(deploymentName("pool-gone"), "default", map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-gone"})
	plain := hrDeployment("nginx", testNamespace, map[string]string{"app": "nginx"})

	c := newTestController(configured, stale, noPoolLabel, lookalikeName, wrongValue, otherNamespace, plain)
	pools := []parsedPoolPolicy{{Name: "pool-a"}, {Name: "pool-b"}}
	if err := c.cleanupStaleDeployments(context.Background(), pools); err != nil {
		t.Fatalf("cleanupStaleDeployments: %v", err)
	}

	if hrExists(t, c, stale) {
		t.Error("headroom Deployment for a removed pool was not deleted")
	}
	// The pool label is what ties a headroom Deployment to its pool; without it the
	// Deployment cannot belong to any configured pool and is GC'd.
	if hrExists(t, c, noPoolLabel) {
		t.Error("headroom-labelled Deployment without a pool label was not deleted")
	}
	kept := map[string]*appsv1.Deployment{
		"configured pool":                 configured,
		"lookalike name without label":    lookalikeName,
		"headroom label not \"true\"":     wrongValue,
		"headroom label, other namespace": otherNamespace,
		"unrelated Deployment":            plain,
	}
	for why, d := range kept {
		if !hrExists(t, c, d) {
			t.Errorf("%s Deployment %s/%s must be kept but was deleted", why, d.Namespace, d.Name)
		}
	}
}

func TestHrCleanupStaleDeployments_NilPoolsRemovesEveryHeadroomDeployment(t *testing.T) {
	a := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	b := hrDeployment(deploymentName("pool-b"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-b"})
	plain := hrDeployment("nginx", testNamespace, map[string]string{"app": "nginx"})
	c := newTestController(a, b, plain)
	if err := c.cleanupStaleDeployments(context.Background(), nil); err != nil {
		t.Fatalf("cleanupStaleDeployments(nil): %v", err)
	}
	if hrExists(t, c, a) || hrExists(t, c, b) {
		t.Error("with no configured pools every headroom Deployment must be removed")
	}
	if !hrExists(t, c, plain) {
		t.Error("an unrelated Deployment was deleted")
	}
}

func TestHrCleanupStaleDeployments_DeleteErrorIsToleratedAndOthersProceed(t *testing.T) {
	stuck := hrDeployment(deploymentName("pool-stuck"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-stuck"})
	other := hrDeployment(deploymentName("pool-other"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-other"})
	funcs := interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == stuck.Name {
				return kerrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), errors.New("conflict"))
			}
			return cl.Delete(ctx, obj, opts...)
		},
	}
	c := hrNewInterceptedController(funcs, stuck, other)
	if err := c.cleanupStaleDeployments(context.Background(), nil); err != nil {
		t.Fatalf("a failed Deployment delete must not fail the sync, got: %v", err)
	}
	if !hrExists(t, c, stuck) {
		t.Error("the Deployment whose delete failed should still exist")
	}
	if hrExists(t, c, other) {
		t.Error("the loop must continue past a failed delete and remove the other stale Deployment")
	}
}

func TestHrCleanupStaleDeployments_ListErrorIsReturned(t *testing.T) {
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*appsv1.DeploymentList); ok {
				return kerrors.NewServiceUnavailable("apiserver down")
			}
			return cl.List(ctx, list, opts...)
		},
	}
	c := hrNewInterceptedController(funcs)
	err := c.cleanupStaleDeployments(context.Background(), nil)
	if err == nil {
		t.Fatal("expected the list error to be returned")
	}
	if !strings.Contains(err.Error(), "list headroom deployments") {
		t.Errorf("error %q should be wrapped with context", err.Error())
	}
}

// --- Reconcile GC paths (documented in docs/headroom.md: buffer is torn down when the
// ConfigMap is absent or declares no pools) ---

func TestHrReconcile_AbsentConfigMapTearsDownBuffer(t *testing.T) {
	dep := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	legacy := hrPod("headroom-legacy-0", testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	plain := hrDeployment("nginx", testNamespace, map[string]string{"app": "nginx"})
	c := newTestController(dep, legacy, plain, makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true))

	res, err := c.Reconcile(context.Background(), hrSyncRequest())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}
	if hrExists(t, c, dep) {
		t.Error("headroom Deployment must be torn down when the policy ConfigMap is absent")
	}
	if hrExists(t, c, legacy) {
		t.Error("legacy bare pod must be GC'd when the policy ConfigMap is absent")
	}
	if !hrExists(t, c, plain) {
		t.Error("unrelated Deployment was deleted")
	}
}

func TestHrReconcile_EmptyPoolsTearsDownBuffer(t *testing.T) {
	dep := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	c := newTestController(hrPolicyConfigMap("pools: []\n"), dep, makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true))

	res, err := c.Reconcile(context.Background(), hrSyncRequest())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}
	if hrExists(t, c, dep) {
		t.Error("headroom Deployment must be torn down when the policy declares no pools")
	}
}

func TestHrReconcile_PoolRemovedFromPolicyIsGCdOthersKept(t *testing.T) {
	policy := "pools:\n  - name: pool-a\n    cpu: 50%\n  - name: pool-b\n    minPods: 1\n"
	keepA := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	gone := hrDeployment(deploymentName("pool-c"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-c"})
	c := newTestController(hrPolicyConfigMap(policy), keepA, gone, livePool("pool-a"), livePool("pool-b"),
		makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true))

	if _, err := c.Reconcile(context.Background(), hrSyncRequest()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !hrExists(t, c, keepA) {
		t.Error("configured pool-a Deployment was deleted")
	}
	if hrExists(t, c, gone) {
		t.Error("pool-c Deployment (no longer in the policy) was not deleted")
	}
	// pool-b is new: it must have been created in the same sync (minPods cold start).
	b := hrGetDeployment(t, c, "pool-b")
	if b.Spec.Replicas == nil || *b.Spec.Replicas != 1 {
		t.Errorf("pool-b replicas = %v, want minPods floor of 1", b.Spec.Replicas)
	}
}

// TestHrReconcile_PartialPoolFailure: one pool failing to apply must not stop the others nor
// the GC steps, and the sync must report the failure (no RequeueAfter: controller-runtime
// backoff retries it).
func TestHrReconcile_PartialPoolFailure(t *testing.T) {
	policy := "pools:\n  - name: pool-a\n    cpu: 50%\n  - name: pool-b\n    cpu: 50%\n  - name: pool-c\n    cpu: 50%\n"
	stale := hrDeployment(deploymentName("pool-gone"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-gone"})
	legacy := hrPod("headroom-legacy-0", testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if d, ok := obj.(*appsv1.Deployment); ok && d.Name == deploymentName("pool-b") {
				return kerrors.NewInvalid(schema.GroupKind{Group: "apps", Kind: "Deployment"}, d.Name, nil)
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	c := hrNewInterceptedController(funcs,
		hrPolicyConfigMap(policy), stale, legacy,
		livePool("pool-a"), livePool("pool-b"), livePool("pool-c"),
		makeNode("na", "pool-a", standardAlloc("4", "16Gi"), true),
		makeNode("nb", "pool-b", standardAlloc("4", "16Gi"), true),
		makeNode("nc", "pool-c", standardAlloc("4", "16Gi"), true),
	)

	res, err := c.Reconcile(context.Background(), hrSyncRequest())
	if err == nil {
		t.Fatal("Reconcile must return the failed pool's error")
	}
	if !strings.Contains(err.Error(), `pool "pool-b"`) {
		t.Errorf("error %q should name the failed pool", err.Error())
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 (error path relies on controller-runtime backoff)", res.RequeueAfter)
	}

	for _, pool := range []string{"pool-a", "pool-c"} {
		if d := hrGetDeployment(t, c, pool); d.Spec.Replicas == nil || *d.Spec.Replicas != 4 {
			t.Errorf("%s replicas = %v, want 4 — the other pools must still be applied", pool, d.Spec.Replicas)
		}
	}
	if hrExists(t, c, hrDeployment(deploymentName("pool-b"), testNamespace, nil)) {
		t.Error("pool-b Deployment exists although its Create was rejected")
	}
	if hrExists(t, c, stale) {
		t.Error("stale Deployment GC must still run after a pool failure")
	}
	if hrExists(t, c, legacy) {
		t.Error("legacy pod GC must still run after a pool failure")
	}
}

// TestHrReconcile_ConfigMapGetErrorAbortsBeforeGC: a transient read failure on the ConfigMap
// (not NotFound) must not be mistaken for "headroom off" and GC the buffer.
func TestHrReconcile_ConfigMapGetErrorAbortsBeforeGC(t *testing.T) {
	dep := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.ConfigMap); ok {
				return kerrors.NewServiceUnavailable("apiserver down")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}
	c := hrNewInterceptedController(funcs, hrPolicyConfigMap("pools:\n  - name: pool-a\n    cpu: 10%\n"), dep)
	res, err := c.Reconcile(context.Background(), hrSyncRequest())
	if err == nil {
		t.Fatal("Reconcile must return the ConfigMap read error")
	}
	if !strings.Contains(err.Error(), "load config") {
		t.Errorf("error %q should be wrapped as a config load failure", err.Error())
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0 on the error path", res.RequeueAfter)
	}
	if !hrExists(t, c, dep) {
		t.Error("headroom Deployment was GC'd on a transient ConfigMap read failure")
	}
}

// TestHrReconcile_ConfigMapInOtherNamespaceIsIgnored: only the ConfigMap in configNamespace
// counts; a same-named one elsewhere must not configure (or protect) pools.
func TestHrReconcile_ConfigMapInOtherNamespaceIsIgnored(t *testing.T) {
	other := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: "default"},
		Data:       map[string]string{"policy": "pools:\n  - name: pool-a\n    cpu: 50%\n"},
	}
	dep := hrDeployment(deploymentName("pool-a"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "pool-a"})
	c := newTestController(other, dep, makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true))
	if _, err := c.Reconcile(context.Background(), hrSyncRequest()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if hrExists(t, c, dep) {
		t.Error("a ConfigMap in another namespace must not keep the buffer alive")
	}
}
