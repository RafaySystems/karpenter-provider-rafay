/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// fup_get_test.go — follow-up tests for CloudProvider.Get's kube fallback (getNodeFromKube):
//
//   - R3-karpenter-node-termination-get-2: the fork's node/termination controller calls Get()
//     for a NotReady node and skips the drain on NodeClaimNotFound. In broker mode Get answers
//     from the very Node object the controller is reconciling (pinned by its finalizer), so the
//     short-circuit can never fire. The test pins that Get keeps reporting the node as existing
//     — the fix is a bounded drain (terminationGracePeriod rendered by the broker), NOT making
//     Get consult NotReady/DeletionTimestamp, which would orphan a transiently NotReady machine.
//   - R3-karpenter-node-termination-get-5: getNodeFromKube lists and deep-copies every Node on
//     each reconcile instead of using the fork's spec.providerID field index.

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// fupProviderIDIndex is the field index the fork registers on Nodes (operator.go) and that
// AllNodesForNodeClaim already queries.
const fupProviderIDIndex = "spec.providerID"

// fupListRecorder records the ListOptions of every List so a test can see which selectors the
// provider sent to the cache.
type fupListRecorder struct {
	client.Client
	mu    sync.Mutex
	calls []client.ListOptions
}

func (r *fupListRecorder) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	var lo client.ListOptions
	lo.ApplyOptions(opts)
	r.mu.Lock()
	r.calls = append(r.calls, lo)
	r.mu.Unlock()
	return r.Client.List(ctx, list, opts...)
}

func (r *fupListRecorder) listCalls() []client.ListOptions {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]client.ListOptions(nil), r.calls...)
}

// fupIndexedClient builds a fake client with the fork's spec.providerID Node index registered,
// so a MatchingFields list on it works exactly as it does against the real informer cache.
func fupIndexedClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newFailureHandlerScheme(t)).
		WithIndex(&corev1.Node{}, fupProviderIDIndex, func(o client.Object) []string {
			return []string{o.(*corev1.Node).Spec.ProviderID}
		}).
		WithObjects(objs...).
		Build()
}

// ──────────────────────────── R3-karpenter-node-termination-get-2 ────────────────────────────

// TestFupGetTerminatingNotReadyNodeStillReportsExists: a Node that is NotReady, Terminating and
// pinned by karpenter.sh/termination is exactly what the fork's termination controller hands
// to Get(). The kube fallback finds that Node, so Get must report the instance as existing —
// the dead-instance short-circuit is unreachable in broker mode and the termination path must
// be bounded elsewhere (terminationGracePeriod), never by Get() guessing from readiness.
func TestFupGetTerminatingNotReadyNodeStillReportsExists(t *testing.T) {
	node := cpNode("host-get", "pool1", "oci-inst", cpGetID, cpBase)
	ts := metav1.NewTime(cpBase.Add(time.Hour))
	node.DeletionTimestamp = &ts
	node.Finalizers = []string{karpv1.TerminationFinalizer}
	node.Status.Conditions = []corev1.NodeCondition{{
		Type:   corev1.NodeReady,
		Status: corev1.ConditionFalse,
		Reason: "KubeletNotReady",
	}}
	cl := cpFakeClient(t, node)
	rc := &cpFakeRafayClient{} // GetNode → ErrGetNodeUnsupported (broker mode)
	cp := cpProvider(cl, nil, rc, nil)

	nc, err := cp.Get(context.Background(), cpGetID)
	// The specific check comes first: a NotFound here is the exact regression this test guards
	// against (a fix that makes Get consult readiness), so it gets its own message.
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatal("Get must not report NodeClaimNotFound for a node that is merely NotReady: that drops the finalizer of a live machine")
	}
	if err != nil {
		t.Fatalf("Get(NotReady Terminating node) = %v, want the node reported as existing", err)
	}
	if nc == nil || nc.Status.ProviderID != cpGetID {
		t.Fatalf("Get = %+v, want a NodeClaim carrying %s", nc, cpGetID)
	}
}

// ──────────────────────────── R3-karpenter-node-termination-get-5 ────────────────────────────

// TestFupGetKubeFallbackUsesProviderIDIndex: the kube fallback must ask the cache for the one
// Node carrying the providerID via the spec.providerID field index, not list and deep-copy the
// whole Node set on every termination reconcile.
func TestFupGetKubeFallbackUsesProviderIDIndex(t *testing.T) {
	rec := &fupListRecorder{Client: fupIndexedClient(t,
		cpNode("host-get", "pool1", "oci-inst", cpGetID, cpBase),
		cpNode("host-other", "pool1", "oci-inst", "rafay://pool1/oci-inst/host-other", cpBase),
	)}
	cp := cpProvider(rec, nil, &cpFakeRafayClient{}, nil)

	nc, err := cp.Get(context.Background(), cpGetID)
	if err != nil {
		t.Fatalf("Get (kube fallback) = %v", err)
	}
	if nc.Status.ProviderID != cpGetID {
		t.Errorf("providerID = %q, want %q", nc.Status.ProviderID, cpGetID)
	}

	calls := rec.listCalls()
	if len(calls) != 1 {
		t.Fatalf("Node LIST calls = %d, want exactly 1", len(calls))
	}
	if calls[0].FieldSelector == nil {
		t.Fatalf("Node LIST carried no field selector; want MatchingFields{%q: %s} so the cache answers from its index", fupProviderIDIndex, cpGetID)
	}
	got, exact := calls[0].FieldSelector.RequiresExactMatch(fupProviderIDIndex)
	if !exact || got != cpGetID {
		t.Fatalf("Node LIST field selector = %s, want an exact match on %q = %s", calls[0].FieldSelector, fupProviderIDIndex, cpGetID)
	}

	// The index must also answer NotFound for an ID nothing carries.
	if _, err := cp.Get(context.Background(), "rafay://pool1/oci-inst/host-gone"); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Get of an ID no Node carries = %v, want NodeClaimNotFoundError", err)
	}
}
