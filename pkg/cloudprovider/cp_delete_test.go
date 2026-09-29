/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_delete_test.go — CloudProvider.Delete, driven through the nodeBatcher seam (cpFakeBatcher).
//
// Delete is the convergence contract the whole termination flow depends on: Karpenter calls it
// every ~5s and only releases the node's finalizer once it returns NodeClaimNotFoundError.

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
)

const (
	cpRealID    = "rafay://pool1/oci-inst/host-w1-e6a5c"
	cpClaimUID  = "uid-1"
	cpRemoveOp  = cpClaimUID + "-remove"
	cpPoolName  = "pool1"
	cpSKUName   = "oci-inst"
	cpClaimName = "pool1-abcde"
)

// cpLiveNode is the Node object Karpenter holds alive (with its own finalizer) while it terminates
// a NodeClaim; the normal Delete() path always runs with such a Node present.
func cpLiveNode(providerID string) *corev1.Node {
	return cpNode("host-w1-e6a5c", cpPoolName, cpSKUName, providerID, cpBase.Add(-time.Hour))
}

// ──────────────────────────── guard clauses ────────────────────────────

func TestDeleteWithoutProviderIDIsNotFound(t *testing.T) {
	// No batcher at all: these branches must return before touching it.
	cp := cpProvider(cpFakeClient(t), nil, nil, nil)

	if err := cp.Delete(context.Background(), nil); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Errorf("Delete(nil) = %v, want NodeClaimNotFoundError", err)
	}
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, "")
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Errorf("Delete(empty providerID) = %v, want NodeClaimNotFoundError", err)
	}
}

// ──────────────────────────── Succeeded short-circuit ────────────────────────────

// Once the poller has recorded <uid>-remove SUCCEEDED, Delete must report the instance gone
// without another send and without touching the API server.
func TestDeleteSucceededShortCircuit(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpRemoveOp)
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	reader := &cpReader{Reader: cl}
	cp := cpProvider(cl, reader, nil, b)

	err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete after SUCCEEDED = %v, want NodeClaimNotFoundError", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove called %d times after SUCCEEDED, want 0", n)
	}
	if n := reader.listCount(); n != 0 {
		t.Errorf("%d LISTs issued on the short-circuit path, want 0", n)
	}
}

// The short-circuit also applies to a still-pending NodeClaim (its remove was sent with a
// resolved ID earlier); no lookup or cancel must happen.
func TestDeleteSucceededShortCircuitForPendingClaim(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpRemoveOp)
	cl := cpFakeClient(t)
	reader := &cpReader{Reader: cl}
	cp := cpProvider(cl, reader, nil, b)

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete = %v, want NodeClaimNotFoundError", err)
	}
	if len(b.cancelCalls()) != 0 || reader.listCount() != 0 {
		t.Errorf("cancels=%v lists=%d, want none on the SUCCEEDED path", b.cancelCalls(), reader.listCount())
	}
}

// ──────────────────────────── real providerID: EnqueueRemove ────────────────────────────

func TestDeleteRealProviderIDEnqueuesRemove(t *testing.T) {
	b := cpNewFakeBatcher()
	b.result.BatchID = "batch-rm-1"
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	cp := cpProvider(cl, nil, nil, b)

	err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if err != nil {
		t.Fatalf("Delete on broker ACK = %v, want nil (Karpenter requeues and calls again)", err)
	}
	removes := b.removeCalls()
	if len(removes) != 1 {
		t.Fatalf("EnqueueRemove calls = %d, want 1", len(removes))
	}
	got := removes[0]
	if got.operationID != cpRemoveOp {
		t.Errorf("operationID = %q, want %q (deterministic <uid>-remove so retries dedup)", got.operationID, cpRemoveOp)
	}
	if got.req.ClusterID != cpClusterID || got.req.ProjectID != cpProjectID {
		t.Errorf("cluster/project = %q/%q, want %q/%q", got.req.ClusterID, got.req.ProjectID, cpClusterID, cpProjectID)
	}
	if got.req.InstanceType != cpSKUName {
		t.Errorf("InstanceType = %q, want the NodeClaim's instance-type label %q", got.req.InstanceType, cpSKUName)
	}
	if got.req.NodePoolName != cpPoolName {
		t.Errorf("NodePoolName = %q, want %q", got.req.NodePoolName, cpPoolName)
	}
	if got.req.ProviderID != cpRealID {
		t.Errorf("ProviderID = %q, want %q", got.req.ProviderID, cpRealID)
	}
	if len(b.cancelCalls()) != 0 {
		t.Errorf("Cancel called (%v) on the real-ID path, want none", b.cancelCalls())
	}
}

// A duplicate ACK (the remove is already in flight at the broker) is still "queued": nil.
func TestDeleteDuplicateAckReturnsNil(t *testing.T) {
	b := cpNewFakeBatcher()
	b.result.Duplicate = true
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	cp := cpProvider(cl, nil, nil, b)

	if err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)); err != nil {
		t.Fatalf("Delete on duplicate ACK = %v, want nil", err)
	}
}

// A send failure is returned as-is so Karpenter retries (and must not be mistaken for NotFound,
// which would release the finalizer with the node still there).
func TestDeleteResultErrIsPropagated(t *testing.T) {
	b := cpNewFakeBatcher()
	sendErr := errors.New("send remove batch failed: broker unavailable")
	b.result.Err = sendErr
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	cp := cpProvider(cl, nil, nil, b)

	err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if !errors.Is(err, sendErr) {
		t.Fatalf("Delete = %v, want the batcher error %v", err, sendErr)
	}
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatal("a send failure must not be reported as NodeClaimNotFound")
	}
}

// While the item sits in the batcher queue, a cancelled context returns ctx.Err() and nothing
// else (the queued item is left for the dedup on the next call).
func TestDeleteContextCancelledWhileQueued(t *testing.T) {
	b := cpNewFakeBatcher()
	b.hold = true
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	cp := cpProvider(cl, nil, nil, b)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := cp.Delete(ctx, cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete with cancelled ctx = %v, want context.Canceled", err)
	}
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatal("ctx cancellation must not be reported as NodeClaimNotFound")
	}
	if n := len(b.removeCalls()); n != 1 {
		t.Errorf("EnqueueRemove calls = %d, want 1 (the item was queued before the ctx was checked)", n)
	}
}

// ──────────────────────────── pending providerID ────────────────────────────

// Node not joined: best-effort cancel of the add (keyed by the NodeClaim UID) and NotFound, so
// Karpenter drops the NodeClaim instead of waiting for a node that is not coming.
func TestDeletePendingNodeNotJoinedCancelsAdd(t *testing.T) {
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t) // no nodes at all
	cp := cpProvider(cl, nil, nil, b)

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete(pending, no node) = %v, want NodeClaimNotFoundError", err)
	}
	if got := b.cancelCalls(); len(got) != 1 || got[0] != cpClaimUID {
		t.Errorf("Cancel calls = %v, want exactly [%s] (the add's operationID is the NodeClaim UID)", got, cpClaimUID)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove calls = %d, want 0 when no node has joined", n)
	}
}

// Add SUCCEEDED, node joined but not yet bound by NodeProviderIDController: the remove carries the
// resolved providerID, the add is not cancelled, and the NodeClaim is given the ID first (a
// reservation, R1-prov-cloudprovider-3) so no other pending NodeClaim can be bound to the node
// while the removal is in flight.
//
// The lookup only runs once the add has SUCCEEDED (R2-add-cancel-path-1 / R1-prov-cloudprovider-3:
// a node that merely matches the pool/SKU may belong to someone else while this add has not
// landed), hence the markSucceeded precondition.
func TestDeletePendingResolvesJoinedNode(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpClaimUID)
	claim := cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName)
	joined := cpNode("host-w1-e6a5c", cpPoolName, cpSKUName, cpRealID, cpBase.Add(12*time.Minute))
	cl := cpFakeClient(t, claim, joined)
	cp := cpProvider(cl, nil, nil, b)

	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete(pending, node joined) = %v, want nil after ACK", err)
	}
	removes := b.removeCalls()
	if len(removes) != 1 || removes[0].req.ProviderID != cpRealID {
		t.Fatalf("removes = %+v, want one remove carrying the resolved providerID %s", removes, cpRealID)
	}
	if removes[0].operationID != cpRemoveOp {
		t.Errorf("operationID = %q, want %q", removes[0].operationID, cpRemoveOp)
	}
	if len(b.cancelCalls()) != 0 {
		t.Errorf("Cancel called (%v) although the node was resolved", b.cancelCalls())
	}
	var stored karpv1.NodeClaim
	if err := cl.Get(context.Background(), client.ObjectKey{Name: cpClaimName}, &stored); err != nil {
		t.Fatalf("get stored claim: %v", err)
	}
	if stored.Status.ProviderID != cpRealID {
		t.Errorf("stored status.providerID = %q, want the resolved %s reserved before the remove", stored.Status.ProviderID, cpRealID)
	}
	if claim.Status.ProviderID != PendingProviderIDPrefix+cpClaimUID {
		t.Error("Delete mutated its input NodeClaim; the reservation must go through the API")
	}
}

// Add not yet SUCCEEDED, a matching node joined: it is not this NodeClaim's to remove (the add may
// still be RUNNING for a machine that is coming, and the node may be another claim's). Delete
// asks the broker to cancel; a confirmed cancel means nothing was built, so NotFound.
func TestDeletePendingDoesNotResolveBeforeAddSucceeded(t *testing.T) {
	b := cpNewFakeBatcher() // Cancel → applied
	claim := cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName)
	joined := cpNode("host-w1-e6a5c", cpPoolName, cpSKUName, cpRealID, cpBase.Add(12*time.Minute))
	cl := cpFakeClient(t, claim, joined)
	reader := &cpReader{Reader: cl}
	cp := cpProvider(cl, reader, nil, b)

	err := cp.Delete(context.Background(), claim)
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete(pending, add cancelled) = %v, want NodeClaimNotFoundError", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0: the add never landed, the joined node is not this claim's", n)
	}
	if n := reader.listCount(); n != 0 {
		t.Errorf("%d uncached LISTs for an add that has not landed, want 0", n)
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-2 ────────────────────────────

// TestDeleteSkipsRemoveWhenNoNodeCarriesProviderID: a NodeClaim with a real providerID whose Node
// object is already gone (GC path: the platform retired the machine, or picked a different one
// than Karpenter asked for) must not trigger another count decrement. Nothing carries the ID, the
// op was never ACKed, so the instance is gone: report NotFound and send no remove.
func TestDeleteSkipsRemoveWhenNoNodeCarriesProviderID(t *testing.T) {
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t) // no Node carries cpRealID, not even a Terminating one
	cp := cpProvider(cl, nil, nil, b)

	err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete with no Node carrying the providerID = %v, want NodeClaimNotFoundError", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove called %d times for a machine that is already gone, want 0 (each such remove retires another healthy machine)", n)
	}
}

// TestDeleteSendsRemoveWhileNodeIsTerminating is the companion guard: the normal termination
// path — Node object present, Terminating under Karpenter's finalizer — must still send exactly
// one remove.
func TestDeleteSendsRemoveWhileNodeIsTerminating(t *testing.T) {
	b := cpNewFakeBatcher()
	node := cpLiveNode(cpRealID)
	now := metav1.NewTime(cpBase.Add(time.Hour))
	node.DeletionTimestamp = &now
	node.Finalizers = []string{karpv1.TerminationFinalizer}
	cl := cpFakeClient(t, node)
	cp := cpProvider(cl, nil, nil, b)

	if err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)); err != nil {
		t.Fatalf("Delete with Terminating Node = %v, want nil after ACK", err)
	}
	if n := len(b.removeCalls()); n != 1 {
		t.Fatalf("EnqueueRemove calls = %d, want 1 for a Terminating node", n)
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-1 ────────────────────────────

// TestDeleteRemoveRefusedAtPoolMinimumConverges: once the broker has permanently refused the
// remove (pool at scaling.min), the provider must stop re-sending it every 5s and let Karpenter
// converge (NotFound releases the finalizer; the still-running machine is re-adopted).
func TestDeleteRemoveRefusedAtPoolMinimumConverges(t *testing.T) {
	b := cpNewFakeBatcher()
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	cl := cpFakeClient(t, claim, cpLiveNode(cpRealID))
	backoff := NewPoolBackoff(5 * time.Minute)
	cp := cpProvider(cl, nil, nil, b)
	cp.poolBackoff = backoff
	handler := NewBatchFailureHandler(cl, cl, backoff, nil)

	// The poller reports the remove FAILED with the broker's minimum detail.
	handler(context.Background(), cpRemoveOp, "remove", `batch remove: pool "pool1" is at its minimum: 1 - 1 would drop below min 1`)

	err := cp.Delete(context.Background(), claim)
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete after a pool-at-minimum refusal = %v, want NodeClaimNotFoundError so the termination converges", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove called %d times after a permanent refusal, want 0", n)
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-3 ────────────────────────────

// TestDeletePendingDoesNotRemoveNodeOtherPendingClaimCanOwn: two pending NodeClaims A (older) and
// B in the same pool/SKU, one unowned node that joined after both. Delete(B) must not send a remove
// for a node it has not reserved — NodeProviderIDController can bind the same node to A in the same
// window. Either the node is left alone, or B first takes ownership (status.providerID patched).
func TestDeletePendingDoesNotRemoveNodeOtherPendingClaimCanOwn(t *testing.T) {
	b := cpNewFakeBatcher()
	a := cpPendingClaim("pool1-aaaaa", "uid-a", cpPoolName, cpSKUName)
	a.CreationTimestamp = metav1.NewTime(cpBase.Add(-time.Minute))
	bClaim := cpPendingClaim("pool1-bbbbb", "uid-b", cpPoolName, cpSKUName)
	joined := cpNode("host-x", cpPoolName, cpSKUName, "rafay://pool1/oci-inst/host-x", cpBase.Add(12*time.Minute))
	cl := cpFakeClient(t, a, bClaim, joined)
	cp := cpProvider(cl, nil, nil, b)

	if err := cp.Delete(context.Background(), bClaim); err != nil && !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete(B) = %v", err)
	}

	var stored karpv1.NodeClaim
	if err := cl.Get(context.Background(), client.ObjectKey{Name: "pool1-bbbbb"}, &stored); err != nil {
		t.Fatalf("get B: %v", err)
	}
	reserved := stored.Status.ProviderID == joined.Spec.ProviderID
	for _, rm := range b.removeCalls() {
		if rm.req.ProviderID == joined.Spec.ProviderID && !reserved {
			t.Fatalf("Delete(B) sent a remove for %s without reserving it (B.status.providerID=%q); A can be bound to the same node meanwhile", joined.Spec.ProviderID, stored.Status.ProviderID)
		}
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-4 ────────────────────────────

// TestDeletePendingReturnsListError: a transient API-server error inside findNodeProviderID must
// be returned so Karpenter retries the finalize in 5s — not swallowed into "instance gone". The
// lookup runs for an add that SUCCEEDED (a machine exists), hence the precondition.
func TestDeletePendingReturnsListError(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpClaimUID)
	cl := cpFakeClient(t)
	reader := &cpReader{Reader: cl, listErr: cpFailAllLists(cpErrBoom)}
	cp := cpProvider(cl, reader, nil, b)

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if err == nil {
		t.Fatal("Delete with a failing LIST = nil, want an error so the finalize is retried")
	}
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete with a failing LIST = %v, want an error that is NOT NodeClaimNotFound (the finalizer would be released with the node possibly alive)", err)
	}
	if got := b.cancelCalls(); len(got) != 0 {
		t.Errorf("Cancel called (%v) although the lookup did not run to completion", got)
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-6 ────────────────────────────

// TestDeletePendingResolvesNodeOnlyOnce: once the remove for a pending NodeClaim is in flight,
// the 5s re-invocations must not re-run the two uncached cluster-wide LISTs. The add has
// SUCCEEDED (that is when the lookup runs at all).
func TestDeletePendingResolvesNodeOnlyOnce(t *testing.T) {
	b := cpNewFakeBatcher()
	b.markSucceeded(cpClaimUID)
	claim := cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName)
	joined := cpNode("host-w1-e6a5c", cpPoolName, cpSKUName, cpRealID, cpBase.Add(12*time.Minute))
	cl := cpFakeClient(t, claim, joined)
	reader := &cpReader{Reader: cl}
	cp := cpProvider(cl, reader, nil, b)

	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("first Delete = %v", err)
	}
	after := reader.listCount()
	if after == 0 {
		t.Fatal("first Delete must resolve the node through the API reader")
	}

	// The remove is now in flight at the broker; the batcher answers the retry with Duplicate.
	b.result.Duplicate = true
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("second Delete = %v", err)
	}
	if got := reader.listCount(); got != after {
		t.Fatalf("second Delete issued %d more LIST(s) for an ID already resolved and in flight, want 0", got-after)
	}
}
