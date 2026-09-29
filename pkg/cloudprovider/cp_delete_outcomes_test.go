/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_delete_outcomes_test.go — the outcome signals Delete() consults besides the batcher's
// SUCCEEDED set: the failure handler's per-operation records (PoolBackoff.FailedOp), the broker's
// "node not retired" detail, the memoised cancel outcome, the memoised "retired another machine"
// verdict, and the events those raise on the NodePool.

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
)

func cpNodePool(name string) *karpv1.NodePool {
	np := &karpv1.NodePool{}
	np.Name = name
	return np
}

// ──────────────────────────── PoolBackoff failed-op records ────────────────────────────

func TestPoolBackoffFailedOpRecords(t *testing.T) {
	var b *PoolBackoff
	b.MarkFailedOp("op", "x") // nil receiver: no-op
	if _, ok := b.FailedOp("op"); ok {
		t.Fatal("nil PoolBackoff must not report a record")
	}

	b = NewPoolBackoff(0) // disabled cooldown still keeps records
	now := cpBase
	b.now = func() time.Time { return now }
	b.MarkFailedOp("", "ignored")
	b.MarkFailedOp("op-1", "batch add: boom")
	if _, ok := b.FailedOp(""); ok {
		t.Error("an empty operationID must not be recorded")
	}
	detail, ok := b.FailedOp("op-1")
	if !ok || detail != "batch add: boom" {
		t.Fatalf("FailedOp(op-1) = %q,%t, want the recorded detail", detail, ok)
	}
	if _, ok := b.FailedOp("op-2"); ok {
		t.Error("an unrecorded operation must not be reported")
	}
	now = now.Add(failedOpRetention + time.Second)
	if _, ok := b.FailedOp("op-1"); ok {
		t.Error("a record older than failedOpRetention must be forgotten")
	}
}

func TestPermanentRefusalAndNotRetiredDetails(t *testing.T) {
	permanent := []string{
		`batch add: pool at maximum: pool "p" has 4 of max 4 nodes`,
		`batch add: pool "p" is at its maximum: 3 + 2 would exceed max 4`,
		`batch remove: pool at minimum: pool "p" has 1 of min 1`,
		`batch remove: pool "p" is at its minimum: 1 - 1 would drop below min 1`,
		`batch add: pool not found: "p" is not on cluster "c"`,
		`batch add: pool "p" is not on cluster "c"`,
		`batch add: pool sku mismatch: pool "p" is SKU "a"`,
		`batch add: pool "p" is SKU "a" on the cluster but "b" was requested`,
		`batch add: pool not auto-scaling: pool "p"`,
		`batch add: pool "p" has no scaling block`,
		`batch add: pool precondition: PaaS service pool not configured`,
		`batch add: variable "x" not found on compute instance`,
	}
	for _, d := range permanent {
		if !IsPermanentRefusalDetail(d) {
			t.Errorf("IsPermanentRefusalDetail(%q) = false, want true", d)
		}
	}
	for _, d := range []string{"", "batch add: ApplyCluster: still conflicting after 3 attempts", "batch expired at broker", "operation already in progress"} {
		if IsPermanentRefusalDetail(d) {
			t.Errorf("IsPermanentRefusalDetail(%q) = true, want false (transient)", d)
		}
	}
	if !IsNotRetiredDetail("node not retired: pool at minimum (1)") || !IsNotRetiredDetail("  Node Not Retired: unknown pool") {
		t.Error("IsNotRetiredDetail must match the contract prefix case-insensitively")
	}
	if IsNotRetiredDetail("retired node w1") || IsNotRetiredDetail("") {
		t.Error("IsNotRetiredDetail must only match the prefix")
	}
}

// ──────────────────────────── failure handler records ────────────────────────────

// Every FAILED add is recorded (even when the NodeClaim is already terminating, or missing), so a
// terminating pending NodeClaim's Delete() learns that no machine is coming.
func TestBatchFailureHandlerRecordsEveryFailedAdd(t *testing.T) {
	claim := newPoolClaim("deleting-claim", "op-del", PendingProviderIDPrefix+"op-del", "pool1")
	ts := metav1.NewTime(cpBase)
	claim.DeletionTimestamp = &ts
	claim.Finalizers = []string{karpv1.TerminationFinalizer}
	cl := cpFakeClient(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, cl, backoff, nil)

	handler(context.Background(), "op-del", "add", "batch add: platform error")
	handler(context.Background(), "op-missing", "add", "batch expired at broker")

	for _, op := range []string{"op-del", "op-missing"} {
		if _, ok := backoff.FailedOp(op); !ok {
			t.Errorf("FAILED add %s not recorded", op)
		}
	}
}

// A remove the broker refused permanently is recorded; a transient remove failure is not (Delete
// keeps retrying it).
func TestBatchFailureHandlerRecordsPermanentRemoveRefusalOnly(t *testing.T) {
	cl := cpFakeClient(t)
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, cl, backoff, nil)

	handler(context.Background(), "uid-min-remove", "remove", `batch remove: pool "pool1" is at its minimum: 1 - 1 would drop below min 1`)
	handler(context.Background(), "uid-tr-remove", "remove", "batch remove: ApplyCluster: operation already in progress")

	if _, ok := backoff.FailedOp("uid-min-remove"); !ok {
		t.Error("a permanently refused remove must be recorded for Delete()")
	}
	if _, ok := backoff.FailedOp("uid-tr-remove"); ok {
		t.Error("a transient remove failure must not be recorded (Delete keeps retrying)")
	}
	if cpBackoffHeld(backoff) != 0 {
		t.Error("remove failures must never hold a pool")
	}
}

// A permanent add refusal other than the maximum records a PoolRefusedByPlatform event carrying
// the broker's detail; the maximum keeps its own reason.
func TestBatchFailureHandlerPermanentRefusalEventReason(t *testing.T) {
	tests := []struct {
		name, detail, wantReason string
	}{
		{"not on cluster", `batch add: pool "pool1" is not on cluster "c"`, PoolRefusedEventReason},
		{"sku mismatch", `batch add: pool sku mismatch: pool "pool1" is SKU "a"`, PoolRefusedEventReason},
		{"at maximum", cpPoolAtMaxDetail, PoolAtMaxEventReason},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claim := newPoolClaim("pending-claim", "op-p", PendingProviderIDPrefix+"op-p", "pool1")
			cl := cpFakeClient(t, claim, cpNodePool("pool1"))
			rec := &capturingRecorder{}
			backoff := NewPoolBackoff(5 * time.Minute)
			handler := NewBatchFailureHandler(cl, cl, backoff, rec)

			handler(context.Background(), "op-p", "add", tt.detail)

			if nodeClaimExists(t, cl, "pending-claim") {
				t.Fatal("the pending NodeClaim must still be deleted")
			}
			if _, held := backoff.Until("pool1"); !held {
				t.Fatal("the pool must be held")
			}
			if len(rec.events) != 1 {
				t.Fatalf("events = %d, want 1", len(rec.events))
			}
			if rec.events[0].Reason != tt.wantReason || !strings.Contains(rec.events[0].Message, tt.detail) {
				t.Errorf("event = %s %q, want reason %s carrying the detail", rec.events[0].Reason, rec.events[0].Message, tt.wantReason)
			}
		})
	}
}

// ──────────────────────────── Delete: not-retired convergence ────────────────────────────

// The current broker reports a refused remove SUCCEEDED with a "node not retired" detail: Delete
// converges (NotFound) and warns once on the NodePool.
func TestDeleteSucceededNotRetiredConvergesWithEvent(t *testing.T) {
	b := fupNewBatcher()
	b.markSucceededWithDetail(cpRemoveOp, "node not retired: pool at minimum (1)")
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	cl := cpFakeClient(t, claim, cpLiveNode(cpRealID), cpNodePool(cpPoolName))
	rec := &capturingRecorder{}
	cp := NewCloudProvider(cl, cl, nil, cpClusterID, cpProjectID, nil, nil, WithEventRecorder(rec))
	cp.batcher = b

	for i := 0; i < 2; i++ {
		if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
			t.Fatalf("Delete #%d after a not-retired SUCCEEDED = %v, want NodeClaimNotFoundError", i+1, err)
		}
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove calls = %d, want 0", n)
	}
	if len(rec.events) != 1 {
		t.Fatalf("events = %d, want exactly 1 (warned once)", len(rec.events))
	}
	ev := rec.events[0]
	if ev.Reason != NodeNotRetiredEventReason || !strings.Contains(ev.Message, "pool at minimum (1)") || !strings.Contains(ev.Message, cpRealID) {
		t.Errorf("event = %s %q, want %s naming the detail and the machine", ev.Reason, ev.Message, NodeNotRetiredEventReason)
	}
}

// The legacy path of the same refusal (old broker FAILs the remove; the failure handler recorded
// it) also raises the NodeNotRetired event.
func TestDeleteRemoveRefusedAtPoolMinimumRecordsEvent(t *testing.T) {
	b := cpNewFakeBatcher()
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	cl := cpFakeClient(t, claim, cpLiveNode(cpRealID), cpNodePool(cpPoolName))
	rec := &capturingRecorder{}
	backoff := NewPoolBackoff(5 * time.Minute)
	cp := NewCloudProvider(cl, cl, nil, cpClusterID, cpProjectID, nil, backoff, WithEventRecorder(rec))
	cp.batcher = b
	handler := NewBatchFailureHandler(cl, cl, backoff, rec)

	handler(context.Background(), cpRemoveOp, "remove", `batch remove: pool "pool1" is at its minimum: 1 - 1 would drop below min 1`)
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete = %v, want NodeClaimNotFoundError", err)
	}
	if len(rec.events) != 1 || rec.events[0].Reason != NodeNotRetiredEventReason {
		t.Fatalf("events = %+v, want one %s", rec.events, NodeNotRetiredEventReason)
	}
}

// A transient remove FAILED (recorded by nobody: the handler only logs it) is re-sent as before.
func TestDeleteTransientRemoveFailureIsResent(t *testing.T) {
	b := cpNewFakeBatcher()
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)
	cl := cpFakeClient(t, claim, cpLiveNode(cpRealID))
	backoff := NewPoolBackoff(5 * time.Minute)
	cp := cpProvider(cl, nil, nil, b)
	cp.poolBackoff = backoff
	handler := NewBatchFailureHandler(cl, cl, backoff, nil)

	handler(context.Background(), cpRemoveOp, "remove", "batch remove: ApplyCluster: operation already in progress")
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete after a transient remove failure = %v, want nil (re-sent, awaiting)", err)
	}
	if n := len(b.removeCalls()); n != 1 {
		t.Fatalf("EnqueueRemove calls = %d, want 1", n)
	}
}

// ──────────────────────────── Delete: real ID, removal under way ────────────────────────────

// Once the remove was ACKed in this process the Node-existence check is skipped: the 5s retries
// go straight to the batcher's dedup, and a Node that vanishes mid-removal (the platform retired
// this very machine) does not turn into a spurious NotFound before the broker confirms.
func TestDeleteRealProviderIDSkipsNodeCheckOnceSent(t *testing.T) {
	b := cpNewFakeBatcher()
	node := cpLiveNode(cpRealID)
	cl := cpFakeClient(t, node)
	cp := cpProvider(cl, nil, nil, b)
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)

	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("first Delete = %v", err)
	}
	if err := cl.Delete(context.Background(), node); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	b.result.Duplicate = true
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("second Delete = %v, want nil (removal in flight; the broker decides)", err)
	}
	if n := len(b.removeCalls()); n != 2 {
		t.Errorf("EnqueueRemove calls = %d, want 2 (both dedup at the batcher)", n)
	}
}

// ──────────────────────────── Delete: pending, add outcome ────────────────────────────

// The failure handler recorded the add FAILED: Delete reports NotFound without a cancel RPC or a
// remove.
func TestDeletePendingAddFailedIsNotFoundWithoutCancel(t *testing.T) {
	b := cpNewFakeBatcher()
	backoff := NewPoolBackoff(5 * time.Minute)
	backoff.MarkFailedOp(cpClaimUID, "batch add: platform error")
	cl := cpFakeClient(t)
	cp := cpProvider(cl, nil, nil, b)
	cp.poolBackoff = backoff

	err := cp.Delete(context.Background(), cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName))
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete(pending, add FAILED) = %v, want NodeClaimNotFoundError", err)
	}
	if len(b.cancelCalls()) != 0 || len(b.removeCalls()) != 0 {
		t.Errorf("cancels=%v removes=%v, want none for an add that already failed", b.cancelCalls(), b.removeCalls())
	}
}

// "Not cancellable" never changes, so the 5s retries do not repeat the cancel RPC while waiting
// for the poller; a later FAILED (recorded by the handler) then converges.
func TestDeletePendingUncancellableIsMemoisedThenFailedConverges(t *testing.T) {
	b := fupNewBatcher()
	b.cancelApplied = false
	backoff := NewPoolBackoff(5 * time.Minute)
	cl := cpFakeClient(t)
	cp := fupProvider(cl, b)
	cp.poolBackoff = backoff
	claim := cpPendingClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName)

	for i := 0; i < 3; i++ {
		if err := cp.Delete(context.Background(), claim); err != nil {
			t.Fatalf("Delete #%d = %v, want nil while the add runs", i+1, err)
		}
	}
	if got := b.cancelCalls(); len(got) != 1 {
		t.Fatalf("Cancel calls = %v, want exactly one (memoised as uncancellable)", got)
	}

	backoff.MarkFailedOp(cpClaimUID, "batch add: platform error")
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete after the add FAILED = %v, want NodeClaimNotFoundError", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Errorf("EnqueueRemove calls = %d, want 0 for a failed add", n)
	}
}

// ──────────────────────────── Delete: retired another machine ────────────────────────────

// The verdict "the platform retired another machine" outlives the batcher's memory of the result:
// no remove is re-sent once the SUCCEEDED entry is gone, and the NodeClaim converges only when no
// Node carries its ID any more. The event is recorded once.
func TestDeleteRetiredOtherMachineIsMemoisedAndConvergesWhenNodeGone(t *testing.T) {
	b := fupNewBatcher()
	b.markRetired(cpRemoveOp, fupOtherMachineID)
	node := cpLiveNode(cpRealID)
	cl := cpFakeClient(t, node, cpNodePool(cpPoolName))
	rec := &capturingRecorder{}
	cp := NewCloudProvider(cl, cl, nil, cpClusterID, cpProjectID, nil, nil, WithEventRecorder(rec))
	cp.batcher = b
	claim := cpClaim(cpClaimName, cpClaimUID, cpPoolName, cpSKUName, cpRealID)

	for i := 0; i < 2; i++ {
		if err := cp.Delete(context.Background(), claim); err != nil {
			t.Fatalf("Delete #%d = %v, want nil while this machine's Node exists", i+1, err)
		}
	}
	// The batcher forgets the result (retention): the memo must still block a re-send.
	b.cpFakeBatcher.succeeded = map[string]bool{}
	if err := cp.Delete(context.Background(), claim); err != nil {
		t.Fatalf("Delete after the batcher forgot = %v, want nil", err)
	}
	if n := len(b.removeCalls()); n != 0 {
		t.Fatalf("EnqueueRemove calls = %d, want 0 (a re-send retires a third machine)", n)
	}
	if len(rec.events) != 1 || rec.events[0].Reason != RemoveRetiredOtherMachineEventReason || !strings.Contains(rec.events[0].Message, fupOtherMachineID) {
		t.Fatalf("events = %+v, want one %s naming %s", rec.events, RemoveRetiredOtherMachineEventReason, fupOtherMachineID)
	}

	// The operator retired the machine / released the Node: now the instance is gone.
	if err := cl.Delete(context.Background(), node); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	if err := cp.Delete(context.Background(), claim); !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Delete with no Node carrying the ID = %v, want NodeClaimNotFoundError", err)
	}
}
