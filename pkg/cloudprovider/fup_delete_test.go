/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// fup_delete_test.go — follow-up regression tests for CloudProvider.Delete():
//
//   - R3-karpenter-node-termination-get-1: the SUCCEEDED short-circuit reports NotFound without
//     checking that the machine the platform retired is the one behind THIS NodeClaim.
//   - R2-add-cancel-path-1: the pending-ID branch fires a fire-and-forget Cancel and returns
//     NotFound regardless of whether the add was cancelled, is RUNNING, or already SUCCEEDED.
//
// fupBatcher extends cpFakeBatcher with the two signals Delete() makes load-bearing: the
// providerID the broker reports retired for a SUCCEEDED remove (SucceededResult), and the outcome
// of a synchronous cancel.

import (
	"context"
	"errors"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
)

// fupOtherMachineID is a machine in the same pool/SKU as cpRealID that is NOT the NodeClaim's.
const fupOtherMachineID = "rafay://pool1/oci-inst/host-w2-b7f01"

type fupBatcher struct {
	*cpFakeBatcher
	mu sync.Mutex
	// retired maps a SUCCEEDED remove operationID to the providerID the platform actually
	// retired for it (removal is untargeted: the platform picks the machine).
	retired map[string]string
	// details maps a SUCCEEDED operationID to the broker's detail (e.g. "node not retired: ...").
	details map[string]string
	// cancelApplied / cancelErr script the broker's answer to a Cancel: applied means the op
	// was still ACCEPTED and is now FAILED; not applied means it was RUNNING or terminal.
	cancelApplied bool
	cancelErr     error
}

func fupNewBatcher() *fupBatcher {
	return &fupBatcher{cpFakeBatcher: cpNewFakeBatcher(), retired: map[string]string{}, details: map[string]string{}}
}

// markSucceededWithDetail records a SUCCEEDED operation carrying the broker's detail.
func (b *fupBatcher) markSucceededWithDetail(operationID, detail string) {
	b.markSucceeded(operationID)
	b.mu.Lock()
	b.details[operationID] = detail
	b.mu.Unlock()
}

// markRetired records a SUCCEEDED remove whose platform-side victim was providerID.
func (b *fupBatcher) markRetired(operationID, providerID string) {
	b.markSucceeded(operationID)
	b.mu.Lock()
	b.retired[operationID] = providerID
	b.mu.Unlock()
}

// RetiredProviderID is the providerID the platform actually retired for a SUCCEEDED remove.
func (b *fupBatcher) RetiredProviderID(operationID string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.retired[operationID]
}

// Cancel answers with the scripted broker outcome.
func (b *fupBatcher) Cancel(ctx context.Context, operationID string) (bool, error) {
	_, _ = b.cpFakeBatcher.Cancel(ctx, operationID)
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cancelApplied, b.cancelErr
}

// SucceededResult reports the retired providerID (if any) as the broker's provider_ids, and the
// recorded detail.
func (b *fupBatcher) SucceededResult(operationID string) ([]string, string, bool) {
	if !b.Succeeded(operationID) {
		return nil, "", false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if pid := b.retired[operationID]; pid != "" {
		return []string{pid}, b.details[operationID], true
	}
	return nil, b.details[operationID], true
}

// ──────────────────────────── R3-karpenter-node-termination-get-1 ────────────────────────────

// TestFupDeleteSucceededForOtherMachineIsNotNodeClaimNotFound: the remove for NodeClaim A
// SUCCEEDED at the broker, but the platform retired machine B. Machine A is still running
// (its kubelet heartbeats; the Node is Ready), so Delete(A) must NOT report the instance gone:
// that releases A's Node finalizer, deletes the Node object, and a v1.31 kubelet never
// re-registers it — A becomes a ghost the platform still counts. Delete must keep returning
// nil (the termination stays pending) and must not re-send the remove either (that would
// retire yet another machine).
func TestFupDeleteSucceededForOtherMachineIsNotNodeClaimNotFound(t *testing.T) {
	b := fupNewBatcher()
	b.markRetired(cpRemoveOp, fupOtherMachineID)
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	cp := fupProvider(cl, b)

	err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete = %v: the platform retired %s, not this NodeClaim's %s; reporting NotFound drops the finalizer of a machine that is still running", err, fupOtherMachineID, cpRealID)
	}
	if err != nil {
		t.Fatalf("Delete = %v, want nil (termination stays pending until this machine is confirmed gone)", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove called %d times after the broker already retired a machine for this op, want 0 (another untargeted remove retires a third machine)", n)
	}
}

// TestFupDeleteSucceededForOwnMachineIsNotFound is the companion: when the retired machine is
// the NodeClaim's own, the SUCCEEDED short-circuit reports NotFound with no further send.
func TestFupDeleteSucceededForOwnMachineIsNotFound(t *testing.T) {
	b := fupNewBatcher()
	b.markRetired(cpRemoveOp, cpRealID)
	cl := cpFakeClient(t, cpLiveNode(cpRealID))
	cp := fupProvider(cl, b)

	err := cp.Delete(context.Background(), cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete after the broker retired %s = %v, want NodeClaimNotFoundError", cpRealID, err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove called %d times after SUCCEEDED, want 0", n)
	}
}

// ──────────────────────────── R2-add-cancel-path-1 ────────────────────────────

// TestFupDeletePendingAddAlreadySucceededSendsRemove: the add for a still-pending NodeClaim
// has SUCCEEDED at the broker (poller recorded <uid>), the machine exists or is about to join,
// but no Node carries a resolvable ID yet. Delete must not treat the claim as "never
// provisioned": a cancel cannot undo a finished add, and NotFound would finalize the NodeClaim
// while the machine lands with no owner. The correct move is to enqueue the (untargeted)
// <uid>-remove so the catalog count is decremented, and return nil until it SUCCEEDs.
func TestFupDeletePendingAddAlreadySucceededSendsRemove(t *testing.T) {
	b := fupNewBatcher()
	b.markSucceeded(cpClaimUID) // the ADD op (keyed by NodeClaim UID) finished at the broker
	cl := cpFakeClient(t)       // no Node has joined yet
	cp := fupProvider(cl, b)

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete = %v: the add already SUCCEEDED, so a machine exists for this NodeClaim; NotFound orphans it", err)
	}
	if err != nil {
		t.Fatalf("Delete = %v, want nil after the remove is queued", err)
	}
	removes := b.removeCalls()
	if len(removes) != 1 || removes[0].operationID != cpRemoveOp {
		t.Fatalf("removes = %+v, want exactly one %s so the catalog count of the finished add is undone", removes, cpRemoveOp)
	}
	fupRemoveReqFor(t, removes[0])
	if got := b.cancelCalls(); len(got) != 0 {
		t.Errorf("Cancel called (%v) for an add that already finished; nothing to cancel", got)
	}
}

// TestFupDeletePendingCancelNotAppliedKeepsClaimTerminating: the broker answers the cancel
// with an empty cancelled list (the op is RUNNING: the platform is building the machine).
// Delete must not report NotFound — the machine is coming and the NodeClaim must stay to own
// it. Once the poller records the add SUCCEEDED, the next Delete enqueues the remove.
func TestFupDeletePendingCancelNotAppliedKeepsClaimTerminating(t *testing.T) {
	b := fupNewBatcher()
	b.cancelApplied = false
	cl := cpFakeClient(t)
	cp := fupProvider(cl, b)
	claim := cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName)

	err := cp.Delete(context.Background(), claim)
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete = %v: the broker did not cancel the add (RUNNING); NotFound finalizes the NodeClaim while its machine is being built", err)
	}
	if err != nil {
		t.Fatalf("Delete with an un-cancellable add = %v, want nil (Karpenter retries in 5s)", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove called %d times while the add is still RUNNING, want 0", n)
	}

	// The add finishes: the follow-up Delete turns into a remove of the landed machine.
	b.markSucceeded(cpClaimUID)
	if err := cp.Delete(context.Background(), claim); err != nil && !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete after the add SUCCEEDED = %v", err)
	}
	removes := b.removeCalls()
	if len(removes) != 1 || removes[0].operationID != cpRemoveOp {
		t.Fatalf("removes after the add SUCCEEDED = %+v, want exactly one %s", removes, cpRemoveOp)
	}
}

// TestFupDeletePendingCancelErrorDoesNotReleaseFinalizer: a transport failure on the cancel
// leaves the add's fate unknown; Delete must return the error (Karpenter retries) rather than
// NotFound.
func TestFupDeletePendingCancelErrorDoesNotReleaseFinalizer(t *testing.T) {
	b := fupNewBatcher()
	b.cancelErr = errors.New("cancel operations: broker unavailable")
	cl := cpFakeClient(t)
	cp := fupProvider(cl, b)

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete = %v: the cancel did not reach the broker, so the add may still land a machine; NotFound orphans it", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove called %d times although the add's state is unknown, want 0", n)
	}
}

// TestFupDeletePendingCancelAppliedIsNotFound is the companion that passes today: the broker
// confirmed the cancel (op was ACCEPTED, now FAILED), so no machine will be built and the
// NodeClaim can be finalized without a remove.
func TestFupDeletePendingCancelAppliedIsNotFound(t *testing.T) {
	b := fupNewBatcher()
	b.cancelApplied = true
	cl := cpFakeClient(t)
	cp := fupProvider(cl, b)

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete after a confirmed cancel = %v, want NodeClaimNotFoundError", err)
	}
	if got := b.cancelCalls(); len(got) != 1 || got[0] != cpClaimUID {
		t.Errorf("Cancel calls = %v, want exactly [%s]", got, cpClaimUID)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove called %d times for a cancelled add, want 0 (no machine to remove)", n)
	}
}

// fupRemoveReqFor documents the shape a pending-branch remove is expected to carry: the
// NodeClaim's pool and instance type, and no providerID (the machine has not registered).
func fupRemoveReqFor(t *testing.T, got cpEnqueuedRemove) {
	t.Helper()
	want := rafay.RemoveNodesRequest{ClusterID: cpClusterID, ProjectID: cpProjectID, InstanceType: cpSKUName, NodePoolName: cpPoolName}
	if got.req.ClusterID != want.ClusterID || got.req.ProjectID != want.ProjectID || got.req.NodePoolName != want.NodePoolName || got.req.InstanceType != want.InstanceType {
		t.Errorf("remove request = %+v, want cluster/project/pool/sku of the NodeClaim (%+v)", got.req, want)
	}
}

// fupProvider is cpProvider over the extended fake batcher.
func fupProvider(cl client.Client, b *fupBatcher) *CloudProvider {
	cp := cpProvider(cl, nil, nil, nil)
	cp.batcher = b
	return cp
}
