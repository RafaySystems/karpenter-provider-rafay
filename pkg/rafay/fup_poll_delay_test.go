/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafay

// fup_poll_delay_test.go — regression tests for R2-timeout-ladder-3: registerAndAck used to apply
// a 120 s initial delay before the first status poll of a batch, so a remove whose SUCCEEDED
// tombstone the broker writes seconds after ACK was not observed for 2+ minutes (holding the
// drained node's termination finalizer), and an add the broker refused permanently at claim time
// ("pool at maximum", "pool not found", ...) kept the refused pool open to new NodeClaims for the
// same 2 minutes. Both kinds are now polled on the next ticker tick.

import (
	"context"
	"testing"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
)

// fupSucceededBroker answers every poll with SUCCEEDED for the given ops.
func fupSucceededBroker(opIDs ...string) *mockBroker {
	return &mockBroker{pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
		out := make([]*v1.KarpenterBatchNodeResult, 0, len(opIDs))
		for _, id := range opIDs {
			out = append(out, &v1.KarpenterBatchNodeResult{OperationId: id, State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED})
		}
		return out, nil
	}}
}

// TestFupRemoveBatchIsPolledWithoutInitialDelay: a freshly ACKed remove batch must be polled on
// the very next poller tick, so the broker's tombstone (written at catalog publish) reaches
// Succeeded() — and Delete() — without a hold.
func TestFupRemoveBatchIsPolledWithoutInitialDelay(t *testing.T) {
	m := fupSucceededBroker("op-rem")
	b := newTestBatcher(m)
	b.registerAndAck(opKindRemove, "batch-rm", []batchItem{{operationID: "op-rem", kind: opKindRemove}})

	b.pollBatch(context.Background(), "batch-rm")

	if got := m.pollCallCount(); got != 1 {
		t.Fatalf("poll calls right after a remove ACK = %d, want 1 (a remove's tombstone is written seconds after ACK)", got)
	}
	if !b.Succeeded("op-rem") {
		t.Fatal("Succeeded(op-rem) = false after the broker reported SUCCEEDED on the first poll")
	}
	if b.Tracking("batch-rm") {
		t.Error("a fully SUCCEEDED remove batch must be dropped from inProgress")
	}
}

// TestFupAddBatchIsPolledWithoutInitialDelay is the companion: an add batch is polled on the next
// tick too. A permanent refusal is decided when the broker claims the batch, seconds after ACK,
// and the failure handler must see it within one poll interval — not after a 120 s blind spot in
// which the refused pool keeps taking NodeClaims.
func TestFupAddBatchIsPolledWithoutInitialDelay(t *testing.T) {
	m := fupSucceededBroker("op-add")
	b := newTestBatcher(m)
	b.registerAndAck(opKindAdd, "batch-add", []batchItem{{operationID: "op-add", kind: opKindAdd}})

	b.pollBatch(context.Background(), "batch-add")

	if got := m.pollCallCount(); got != 1 {
		t.Fatalf("poll calls right after an add ACK = %d, want 1 (no initial delay for any batch kind)", got)
	}
	if !b.Succeeded("op-add") {
		t.Fatal("Succeeded(op-add) = false after the broker reported SUCCEEDED on the first poll")
	}
	if b.Tracking("batch-add") {
		t.Error("a fully SUCCEEDED add batch must be dropped from inProgress")
	}
}
