/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package headroom

// fup_inert_pool_test.go — regression tests for R2-autoscaling-optout-transitions-3: headroom
// cannot tell an inert pool. edge-broker renders a pool with autoScaling=false as a NodePool
// annotated karpenter.rafay.io/auto-scaling="false" with limits.nodes "0"; nodeadoption never
// adopts its nodes, so none carries karpenter.sh/nodepool and the NodePool cannot launch. A
// headroom-policy entry with minPods>0 for such a pool yields a Deployment of permanently
// Pending pause pods that the fork's scheduler rejects on every loop.
//
// reconcilePool must consult the NodePool and keep the headroom Deployment at 0 replicas (or
// absent) for an inert pool, both when the pool was never live and after a live→inert flip.

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// fupAutoScalingAnnotation is the annotation edge-broker stamps on every NodePool it renders
// (nodeadoption keys off the same string).
const fupAutoScalingAnnotation = "karpenter.rafay.io/auto-scaling"

// fupScheme is the client-go scheme plus Karpenter's NodePool, which the headroom package does
// not otherwise register (it never reads NodePools today).
func fupScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	gv := schema.GroupVersion{Group: "karpenter.sh", Version: "v1"}
	s.AddKnownTypes(gv, &karpv1.NodePool{}, &karpv1.NodePoolList{})
	metav1.AddToGroupVersion(s, gv)
	return s
}

func fupController(t *testing.T, objs ...client.Object) *Controller {
	t.Helper()
	return &Controller{
		kubeClient:      fake.NewClientBuilder().WithScheme(fupScheme(t)).WithObjects(objs...).Build(),
		podNamespace:    testNamespace,
		configNamespace: testNamespace,
	}
}

// fupNodePool returns a broker-shaped NodePool: live pools carry auto-scaling="true" and a
// positive node limit; inert ones "false" and limits.nodes 0.
func fupNodePool(name string, live bool) *karpv1.NodePool {
	np := &karpv1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{}}}
	if live {
		np.Annotations[fupAutoScalingAnnotation] = "true"
		np.Spec.Limits = karpv1.Limits{"nodes": resource.MustParse("10")}
	} else {
		np.Annotations[fupAutoScalingAnnotation] = "false"
		np.Spec.Limits = karpv1.Limits{"nodes": resource.MustParse("0")}
	}
	return np
}

// fupMinPodsPool is a headroom-policy entry that only carries a cold-start floor.
func fupMinPodsPool(name string, minPods int) parsedPoolPolicy {
	return parsedPoolPolicy{
		Name:      name,
		MinPods:   minPods,
		PodCPU:    resource.MustParse("500m"),
		PodMemory: resource.MustParse("512Mi"),
	}
}

// fupHeadroomReplicas returns the headroom Deployment's replica count, or 0 when the
// Deployment does not exist (both are acceptable "inert" outcomes).
func fupHeadroomReplicas(t *testing.T, c *Controller, pool string) int32 {
	t.Helper()
	var dep appsv1.Deployment
	key := types.NamespacedName{Name: deploymentName(pool), Namespace: c.podNamespace}
	err := c.kubeClient.Get(context.Background(), key, &dep)
	if kerrors.IsNotFound(err) {
		return 0
	}
	if err != nil {
		t.Fatalf("get deployment %s: %v", key, err)
	}
	if dep.Spec.Replicas == nil {
		return 1 // API default
	}
	return *dep.Spec.Replicas
}

// TestFupReconcilePoolSkipsInertNodePool: an inert pool (never adopted, cannot launch) with
// minPods: 2 must not get 2 permanently Pending pause pods.
func TestFupReconcilePoolSkipsInertNodePool(t *testing.T) {

	c := fupController(t, fupNodePool("gpu-pool", false)) // no node carries karpenter.sh/nodepool=gpu-pool
	if err := c.reconcilePool(context.Background(), fupMinPodsPool("gpu-pool", 2)); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	if got := fupHeadroomReplicas(t, c, "gpu-pool"); got != 0 {
		t.Fatalf("headroom-gpu-pool replicas = %d for an inert NodePool (auto-scaling=false, limits.nodes 0), want 0: the pods can never schedule and the scheduler reports 'node limits have been exhausted' for each on every loop", got)
	}
}

// TestFupReconcilePoolLiveToInertFlipScalesToZero: a pool that was live (its nodes carry the
// label, so the buffer math has something to size from) and is then rendered inert must have
// its headroom scaled to 0 — otherwise every preempted pause pod's replacement pends forever.
func TestFupReconcilePoolLiveToInertFlipScalesToZero(t *testing.T) {

	ctx := context.Background()
	np := fupNodePool("worker-a", true)
	c := fupController(t, np, makeNode("n1", "worker-a", standardAlloc("4", "16Gi"), true))
	pool := hrSimplePool("worker-a")
	pool.MinPods = 2
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (live): %v", err)
	}
	if got := fupHeadroomReplicas(t, c, "worker-a"); got != 4 {
		t.Fatalf("live pool replicas = %d, want 4 (ceil(0.5*4000/500))", got)
	}

	// The catalog flips the pool to autoScaling=false: the broker re-renders the NodePool.
	var stored karpv1.NodePool
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: "worker-a"}, &stored); err != nil {
		t.Fatalf("get NodePool: %v", err)
	}
	stored.Annotations[fupAutoScalingAnnotation] = "false"
	stored.Spec.Limits = karpv1.Limits{"nodes": resource.MustParse("0")}
	if err := c.kubeClient.Update(ctx, &stored); err != nil {
		t.Fatalf("update NodePool: %v", err)
	}

	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (inert): %v", err)
	}
	if got := fupHeadroomReplicas(t, c, "worker-a"); got != 0 {
		t.Fatalf("replicas after the live→inert flip = %d, want 0", got)
	}
}

// TestFupReconcilePoolLiveNodePoolKeepsMinPods is the companion: a live NodePool with no
// ready nodes yet still gets its cold-start floor.
func TestFupReconcilePoolLiveNodePoolKeepsMinPods(t *testing.T) {
	c := fupController(t, fupNodePool("gpu-pool", true))
	if err := c.reconcilePool(context.Background(), fupMinPodsPool("gpu-pool", 2)); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	dep := hrGetDeployment(t, c, "gpu-pool")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 2 {
		t.Fatalf("replicas = %v, want the minPods floor 2 for a live pool with no nodes yet", dep.Spec.Replicas)
	}
	if sel := dep.Spec.Template.Spec.NodeSelector[karpenterNodePoolLabel]; sel != "gpu-pool" {
		t.Errorf("nodeSelector %s = %q, want gpu-pool", karpenterNodePoolLabel, sel)
	}
	if _, ok := dep.Spec.Template.Spec.NodeSelector[corev1.LabelHostname]; ok {
		t.Errorf("nodeSelector must not pin a hostname: %v", dep.Spec.Template.Spec.NodeSelector)
	}
}
