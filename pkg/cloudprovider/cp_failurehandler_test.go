/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_failurehandler_test.go — NewBatchFailureHandler edge cases and publishPoolAtMaxEvent's
// best-effort contract.

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

const cpPoolAtMaxDetail = `batch add: pool at maximum: pool "pool1" has 3 of max 4 nodes; 2 requested, 1 refused`

func cpBackoffHeld(b *PoolBackoff) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.until)
}

// A failing NodeClaim LIST means the handler cannot tell which claim failed: nothing is deleted
// and no pool is held (the poller will not call again for this op; the 60-minute registration
// timeout is the fallback).
func TestBatchFailureHandlerListErrorIsNoop(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-le", PendingProviderIDPrefix+"op-le", "pool1")
	cl := cpFakeClient(t, claim)
	reader := &cpReader{Reader: cl, listErr: cpFailAllLists(cpErrBoom)}
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, reader, backoff, nil)

	handler(context.Background(), "op-le", "add", cpPoolAtMaxDetail)

	if !nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("a failing LIST must not delete anything")
	}
	if cpBackoffHeld(backoff) != 0 {
		t.Error("a failing LIST must not mark any pool")
	}
}

// A NodeClaim already being deleted is left alone, and the pool is not marked either (the
// handler returns before the pool-at-max branch).
func TestBatchFailureHandlerLeavesDeletingClaimAlone(t *testing.T) {
	claim := newPoolClaim("deleting-claim", "op-del", PendingProviderIDPrefix+"op-del", "pool1")
	ts := metav1.NewTime(cpBase)
	claim.DeletionTimestamp = &ts
	claim.Finalizers = []string{karpv1.TerminationFinalizer}
	cl := cpFakeClient(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	rec := &capturingRecorder{}
	handler := NewBatchFailureHandler(cl, cl, backoff, rec)

	handler(context.Background(), "op-del", "add", cpPoolAtMaxDetail)

	if !nodeClaimExists(t, cl, "deleting-claim") {
		t.Fatal("a NodeClaim with a deletion timestamp must be left to its own termination")
	}
	if _, held := backoff.Until("pool1"); held {
		t.Error("pool marked for a claim that is already terminating")
	}
	if len(rec.events) != 0 {
		t.Errorf("events = %d, want none", len(rec.events))
	}
}

// kubeClient.Delete failing is logged only: no panic, and the pool hold (recorded first) stands.
func TestBatchFailureHandlerDeleteErrorIsLogged(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-de", PendingProviderIDPrefix+"op-de", "pool1")
	base := cpFakeClient(t, claim)
	cl := &cpClient{Client: base, deleteErr: cpErrBoom}
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, base, backoff, nil)

	handler(context.Background(), "op-de", "add", cpPoolAtMaxDetail)

	if !nodeClaimExists(t, base, "pending-claim") {
		t.Fatal("precondition: the fake Delete error must have kept the claim")
	}
	if _, held := backoff.Until("pool1"); !held {
		t.Error("the pool hold is recorded before the delete and must survive a delete error")
	}
}

// A pool-at-maximum refusal for a NodeClaim with no nodepool label cannot hold anything
// (Mark("") is a no-op) and records no event, but still deletes the claim.
func TestBatchFailureHandlerPoolAtMaxWithoutPoolLabel(t *testing.T) {
	claim := newNodeClaim("pending-claim", "op-nolabel", PendingProviderIDPrefix+"op-nolabel")
	cl := cpFakeClient(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	rec := &capturingRecorder{}
	handler := NewBatchFailureHandler(cl, cl, backoff, rec)

	handler(context.Background(), "op-nolabel", "add", cpPoolAtMaxDetail)

	if nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("the pending NodeClaim must still be deleted")
	}
	if cpBackoffHeld(backoff) != 0 {
		t.Error("an empty pool name must not be recorded as a hold")
	}
	if len(rec.events) != 0 {
		t.Errorf("events = %d, want none without a pool", len(rec.events))
	}
}

// ──────────────────────────── regression: R1-xrepo-contract-8 ────────────────────────────

// TestBatchFailureHandlerPermanentRefusalHoldsPool: a first-class refusal that will never succeed
// on retry (pool not on the cluster, SKU mismatch, no scaling block, precondition) must hold the
// pool like 'pool at maximum' does; deleting the claim alone makes Karpenter re-create it every
// ~2.5 min forever through the global broker queue.
func TestBatchFailureHandlerPermanentRefusalHoldsPool(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-orphan", PendingProviderIDPrefix+"op-orphan", "p2")
	cl := cpFakeClient(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, cl, backoff, nil)

	handler(context.Background(), "op-orphan", "add", `batch add: pool "p2" is not on cluster "c"`)

	if _, held := backoff.Until("p2"); !held {
		t.Fatal("a permanent refusal must hold the pool back; otherwise the NodeClaim is re-created and refused indefinitely")
	}
}

// ──────────────────────────── publishPoolAtMaxEvent ────────────────────────────

func TestPublishPoolAtMaxEventBestEffort(t *testing.T) {
	np := &karpv1.NodePool{}
	np.Name = "pool1"
	cl := cpFakeClient(t, np)
	until := cpBase.Add(5 * time.Minute)

	tests := []struct {
		name string
		run  func(rec *capturingRecorder)
	}{
		{name: "nil recorder", run: func(rec *capturingRecorder) {
			publishPoolAtMaxEvent(context.Background(), cl, nil, "pool1", "d", until)
		}},
		{name: "nil reader", run: func(rec *capturingRecorder) {
			publishPoolAtMaxEvent(context.Background(), nil, rec, "pool1", "d", until)
		}},
		{name: "empty pool", run: func(rec *capturingRecorder) {
			publishPoolAtMaxEvent(context.Background(), cl, rec, "", "d", until)
		}},
		{name: "NodePool missing", run: func(rec *capturingRecorder) {
			publishPoolAtMaxEvent(context.Background(), cl, rec, "renamed-pool", "d", until)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &capturingRecorder{}
			tt.run(rec) // must not panic
			if len(rec.events) != 0 {
				t.Errorf("events = %d, want none", len(rec.events))
			}
		})
	}

	rec := &capturingRecorder{}
	publishPoolAtMaxEvent(context.Background(), cl, rec, "pool1", "pool at maximum: 4 of max 4", until)
	if len(rec.events) != 1 {
		t.Fatalf("events = %d, want 1", len(rec.events))
	}
	ev := rec.events[0]
	if got, ok := ev.InvolvedObject.(*karpv1.NodePool); !ok || got.Name != "pool1" {
		t.Errorf("involved object = %T %v, want NodePool pool1", ev.InvolvedObject, ev.InvolvedObject)
	}
	if !strings.Contains(ev.Message, "pool at maximum: 4 of max 4") || !strings.Contains(ev.Message, until.Format(time.RFC3339)) {
		t.Errorf("message = %q, want the detail and the hold end", ev.Message)
	}
	if len(ev.DedupeValues) != 1 || ev.DedupeValues[0] != "pool1" {
		t.Errorf("dedupe values = %v, want [pool1]", ev.DedupeValues)
	}
}
