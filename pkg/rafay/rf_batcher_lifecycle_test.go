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

package rafay

// rf_batcher_lifecycle_test.go — NodeBatcher tests that batcher_test.go leaves out: the Run /
// batchSender / statusPoller / pollAll goroutine lifecycle driven end to end with the mockBroker
// and a cancellable context, pollBatch's error / UNKNOWN / foreign-op / emptyPolls-reset paths,
// opBatch and succeeded bookkeeping, Cancel's non-blocking and WithoutCancel contract, and the
// regression tests for the batcher findings of the autoscaling review.

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
)

// ──────────────────────────── helpers ──────────────────────────────────────

// rfEventually polls cond every millisecond until it holds or waitTimeout elapses.
func rfEventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", waitTimeout, msg)
}

// rfRecv waits for a result on ch, failing the test if none arrives within waitTimeout.
func rfRecv(t *testing.T, ch <-chan BatchResult) BatchResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(waitTimeout):
		t.Fatal("no result delivered within waitTimeout")
		return BatchResult{}
	}
}

func rfInFlight(b *NodeBatcher, opID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.inFlight[opID]
	return ok
}

func rfOpBatchHas(b *NodeBatcher, opID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.opBatch[opID]
	return ok
}

func rfEmptyPolls(b *NodeBatcher, batchID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bp, ok := b.inProgress[batchID]; ok {
		return bp.emptyPolls
	}
	return -1
}

// rfSucceededAt backdates a recorded SUCCEEDED entry.
func rfSucceededAt(b *NodeBatcher, opID string, at time.Time) {
	b.mu.Lock()
	b.succeeded[opID] = succeededResult{at: at}
	b.mu.Unlock()
}

// rfStartRun runs b.Run on a cancellable context with millisecond timings and returns the cancel
// function plus a channel closed when Run returns.
func rfStartRun(t *testing.T, b *NodeBatcher) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	b.batchWindow = 5 * time.Millisecond
	b.pollInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		b.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(waitTimeout):
			t.Error("Run did not return after cancel")
		}
	})
	return cancel, done
}

// rfCtxBroker wraps mockBroker to observe the context each CancelOperations call carries.
type rfCtxBroker struct {
	*mockBroker
	mu         sync.Mutex
	cancelCtxs []context.Context
}

func (m *rfCtxBroker) CancelOperations(ctx context.Context, ids []string) ([]string, error) {
	m.mu.Lock()
	m.cancelCtxs = append(m.cancelCtxs, ctx)
	m.mu.Unlock()
	return m.mockBroker.CancelOperations(ctx, ids)
}

// ──────────────────────────── Run lifecycle ────────────────────────────────

// TestRfRunSendsPollsAndStopsOnCancel drives the whole lifecycle through Run: an Enqueue is
// collected and sent by the batch sender, ACKed to the caller with the batch id, polled by the
// status poller until SUCCEEDED, and both goroutines stop when the context is cancelled.
func TestRfRunSendsPollsAndStopsOnCancel(t *testing.T) {
	// The broker reports RUNNING until the test has checked the in-flight state; a terminal
	// result from the first (5ms) poll would otherwise race the assertions below.
	var succeed atomic.Bool
	m := &mockBroker{
		pollFn: func(batchID string) ([]*v1.KarpenterBatchNodeResult, error) {
			state := v1.KARPENTER_NODE_OPERATION_STATE_RUNNING
			if succeed.Load() {
				state = v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED
			}
			return []*v1.KarpenterBatchNodeResult{{OperationId: "op-1", State: state, ProviderIds: []string{"rafay://np1/sku/h1"}}}, nil
		},
	}
	b := newTestBatcher(m)
	cancel, done := rfStartRun(t, b)

	ch := b.Enqueue("op-1", AddNodesRequest{ClusterID: "c1", InstanceType: "sku", NodePoolName: "np1"})
	r := rfRecv(t, ch)
	if r.Err != nil || r.BatchID != "batch-add" || r.Duplicate {
		t.Fatalf("ACK result = %+v, want BatchID batch-add, no error, not a duplicate", r)
	}
	m.mu.Lock()
	sends := len(m.sendBatchCalls)
	m.mu.Unlock()
	if sends != 1 {
		t.Fatalf("SendBatch calls = %d, want 1", sends)
	}
	if !rfInFlight(b, "op-1") || !b.Tracking("batch-add") {
		t.Fatal("after the ACK the op must be in flight and its batch tracked")
	}

	// A retry while in flight is short-circuited with the same batch id and no second send.
	if r := rfRecv(t, b.Enqueue("op-1", AddNodesRequest{})); !r.Duplicate || r.BatchID != "batch-add" {
		t.Errorf("in-flight retry = %+v, want Duplicate with BatchID batch-add", r)
	}

	succeed.Store(true)
	rfEventually(t, func() bool { return b.Succeeded("op-1") }, "poller never recorded SUCCEEDED")
	rfEventually(t, func() bool { return !b.Tracking("batch-add") }, "completed batch still tracked")
	if rfInFlight(b, "op-1") {
		t.Error("SUCCEEDED op still in flight")
	}
	if m.pollCallCount() < 1 {
		t.Error("PollBatchStatus was never called")
	}
	m.mu.Lock()
	sends = len(m.sendBatchCalls)
	m.mu.Unlock()
	if sends != 1 {
		t.Errorf("SendBatch calls = %d, want still 1 (no re-send of an in-flight op)", sends)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after cancel")
	}
	// The poller is stopped: no further polls arrive.
	polls := m.pollCallCount()
	time.Sleep(20 * time.Millisecond)
	if got := m.pollCallCount(); got != polls {
		t.Errorf("poll calls grew from %d to %d after Run returned", polls, got)
	}
}

// TestRfRunFailedRemoveIsResent pins the documented retry mechanics end to end: a remove the
// broker reports FAILED is handed to the failure handler and cleared from the in-flight set, so
// Karpenter's next Delete() re-sends it as a fresh batch, and the second attempt converges via
// Succeeded().
func TestRfRunFailedRemoveIsResent(t *testing.T) {
	m := &mockBroker{}
	var sendN int
	m.sendBatchRemoveFn = func([]*v1.KarpenterBatchNodeRemoveItem) (string, error) {
		sendN++ // called from the single sender goroutine
		if sendN == 1 {
			return "batch-r1", nil
		}
		return "batch-r2", nil
	}
	m.pollFn = func(batchID string) ([]*v1.KarpenterBatchNodeResult, error) {
		state := v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED
		detail := ""
		if batchID == "batch-r1" {
			state, detail = v1.KARPENTER_NODE_OPERATION_STATE_FAILED, "drain timeout"
		}
		return []*v1.KarpenterBatchNodeResult{{OperationId: "op-rem", State: state, Detail: detail}}, nil
	}
	b := newTestBatcher(m)
	rec := &failureRecorder{}
	b.SetFailureHandler(rec.handler)
	rfStartRun(t, b)

	req := RemoveNodesRequest{ClusterID: "c1", InstanceType: "sku", NodePoolName: "np1", ProviderID: "rafay://np1/sku/h1"}
	if r := rfRecv(t, b.EnqueueRemove("op-rem", req)); r.Err != nil || r.BatchID != "batch-r1" {
		t.Fatalf("first ACK = %+v, want batch-r1", r)
	}
	rfEventually(t, func() bool { return len(rec.snapshot()) == 1 }, "failure handler not invoked for the FAILED remove")
	if calls := rec.snapshot(); calls[0] != (failureCall{operationID: "op-rem", kind: opKindRemove, detail: "drain timeout"}) {
		t.Errorf("failure call = %+v, want op-rem/remove/drain timeout", calls[0])
	}
	if rfInFlight(b, "op-rem") {
		t.Fatal("a FAILED op must leave the in-flight set so it can be re-sent")
	}
	if b.Succeeded("op-rem") {
		t.Fatal("a FAILED op must not be reported succeeded")
	}

	// Karpenter's next Delete(): same operationID, fresh batch.
	if r := rfRecv(t, b.EnqueueRemove("op-rem", req)); r.Err != nil || r.BatchID != "batch-r2" || r.Duplicate {
		t.Fatalf("second ACK = %+v, want a fresh send with batch-r2", r)
	}
	rfEventually(t, func() bool { return b.Succeeded("op-rem") }, "second attempt never SUCCEEDED")
	if len(rec.snapshot()) != 1 {
		t.Errorf("failure handler calls = %+v, want only the first attempt's", rec.snapshot())
	}
	m.mu.Lock()
	sends := len(m.sendBatchRemoveCalls)
	m.mu.Unlock()
	if sends != 2 {
		t.Errorf("SendBatchRemove calls = %d, want exactly 2", sends)
	}
}

// TestRfRunCancelMidWindowSendsPartialBatch pins collectBatch's cancellation contract inside the
// running sender: an item already collected when the context dies is still handed to sendBatch
// (its caller gets a result), and the sender then exits instead of spinning.
func TestRfRunCancelMidWindowSendsPartialBatch(t *testing.T) {
	m := &mockBroker{}
	b := newTestBatcher(m)
	cancel, done := rfStartRun(t, b)
	b.batchWindow = time.Minute // window far longer than the test: only cancellation can close it

	ch := b.Enqueue("op-1", AddNodesRequest{})
	// The sender has taken the item and is waiting out the window once the queue is drained.
	rfEventually(t, func() bool { return len(b.queue) == 0 }, "sender never picked up the item")

	cancel()
	r := rfRecv(t, ch)
	if r.Err != nil || r.BatchID != "batch-add" {
		t.Errorf("result = %+v, want the partial batch sent and ACKed", r)
	}
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after cancel")
	}
	m.mu.Lock()
	sends := len(m.sendBatchCalls)
	m.mu.Unlock()
	if sends != 1 {
		t.Errorf("SendBatch calls = %d, want 1", sends)
	}
}

// TestRfSenderCancelBeforeFirstItemLeavesQueueUntouched pins the other side of that contract:
// once the context is cancelled before any item was collected the sender exits with nothing
// sent, and an item queued afterwards is never resolved by the batcher — the caller's own
// context is what unblocks it, exactly as Create()/Delete() are written.
//
// The sender goroutine is driven directly rather than through Run: Run only joins the status
// poller (batchSender is started with `go` and never waited for), so "Run returned" does not
// mean the sender has observed the cancellation yet, and collectBatch picks at random between a
// ready ctx.Done() and a ready queue item.
func TestRfSenderCancelBeforeFirstItemLeavesQueueUntouched(t *testing.T) {
	m := &mockBroker{}
	b := newTestBatcher(m)
	ctx, cancel := context.WithCancel(context.Background())
	senderDone := make(chan struct{})
	go func() {
		b.batchSender(ctx)
		close(senderDone)
	}()
	cancel()
	select {
	case <-senderDone:
	case <-time.After(waitTimeout):
		t.Fatal("batchSender did not return after cancel")
	}

	// The sender is gone, so nothing can ever resolve this waiter: a non-blocking receive is a
	// complete check.
	ch := b.Enqueue("op-late", AddNodesRequest{})
	select {
	case r := <-ch:
		t.Fatalf("unexpected result %+v after the batcher stopped", r)
	default:
	}
	if len(b.queue) != 1 || !pendingHas(b, "op-late") {
		t.Errorf("queue=%d pendingHas=%t, want the late item left queued with its waiter registered", len(b.queue), pendingHas(b, "op-late"))
	}
	m.mu.Lock()
	sends := len(m.sendBatchCalls)
	m.mu.Unlock()
	if sends != 0 {
		t.Errorf("SendBatch calls = %d, want 0 after the sender exited", sends)
	}
}

// TestRfCollectBatchCancellation covers collectBatch's two cancellation returns directly.
func TestRfCollectBatchCancellation(t *testing.T) {
	t.Run("cancelled before the first item returns nil", func(t *testing.T) {
		b := newTestBatcher(&mockBroker{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := b.collectBatch(ctx); got != nil {
			t.Errorf("collectBatch = %v, want nil", got)
		}
	})
	t.Run("cancelled mid-window returns the partial batch", func(t *testing.T) {
		b := newTestBatcher(&mockBroker{})
		b.batchWindow = time.Minute
		ctx, cancel := context.WithCancel(context.Background())
		b.queue <- batchItem{operationID: "op-1"}
		b.queue <- batchItem{operationID: "op-2"}
		out := make(chan []batchItem, 1)
		go func() { out <- b.collectBatch(ctx) }()
		rfEventually(t, func() bool { return len(b.queue) == 0 }, "collectBatch did not drain the queue")
		cancel()
		select {
		case batch := <-out:
			if len(batch) != 2 || batch[0].operationID != "op-1" || batch[1].operationID != "op-2" {
				t.Errorf("batch = %+v, want [op-1 op-2]", batch)
			}
		case <-time.After(waitTimeout):
			t.Fatal("collectBatch did not return after cancel")
		}
	})
}

// ──────────────────────────── pollAll / pollBatch paths ────────────────────

func TestRfPollAllPollsEveryInProgressBatch(t *testing.T) {
	m := &mockBroker{
		pollFn: func(batchID string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{{OperationId: "op-" + batchID, State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED}}, nil
		},
	}
	b := newTestBatcher(m)
	registerBatch(b, "a", opKindAdd, time.Now(), "op-a")
	registerBatch(b, "b", opKindRemove, time.Now(), "op-b")
	registerBatch(b, "c", opKindAdd, time.Now(), "op-c", "op-c2")

	b.pollAll(context.Background())

	m.mu.Lock()
	polled := append([]string(nil), m.pollCalls...)
	m.mu.Unlock()
	want := map[string]bool{"a": true, "b": true, "c": true}
	if len(polled) != 3 {
		t.Fatalf("poll calls = %v, want one per tracked batch", polled)
	}
	for _, id := range polled {
		if !want[id] {
			t.Errorf("unexpected poll for %q", id)
		}
		delete(want, id)
	}
	if !b.Succeeded("op-a") || !b.Succeeded("op-b") || !b.Succeeded("op-c") {
		t.Error("every batch's SUCCEEDED result must be recorded in one pollAll pass")
	}
	if got := inProgressBatchIDs(b); len(got) != 1 || got["c"] != opKindAdd {
		t.Errorf("inProgress after pollAll = %v, want only c (op-c2 unresolved)", got)
	}
	if got := remainingOpIDs(b, "c"); !reflect.DeepEqual(got, []string{"op-c2"}) {
		t.Errorf("remaining in c = %v, want [op-c2]", got)
	}
	// An empty pollAll is a no-op.
	b.pollAll(context.Background())
	if m.pollCallCount() != 4 {
		t.Errorf("poll calls = %d, want 4 (only the still-tracked batch polled again)", m.pollCallCount())
	}
}

// TestRfPollBatchErrorKeepsBatch pins that a failed status poll changes nothing: the batch stays
// tracked with its emptyPolls counter untouched, its ops stay in flight and no failure is
// reported — the next tick simply retries.
func TestRfPollBatchErrorKeepsBatch(t *testing.T) {
	m := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) { return nil, errors.New("broker unavailable") },
	}
	b := newTestBatcher(m)
	rec := &failureRecorder{}
	b.SetFailureHandler(rec.handler)
	registerBatch(b, "batch-1", opKindRemove, time.Now(), "op-1")
	b.mu.Lock()
	b.inProgress["batch-1"].emptyPolls = 2 // one empty poll away from expiry
	b.mu.Unlock()

	for i := 0; i < maxConsecutiveEmptyPolls+1; i++ {
		b.pollBatch(context.Background(), "batch-1")
	}

	if !b.Tracking("batch-1") {
		t.Fatal("batch dropped after poll errors, want kept")
	}
	if got := rfEmptyPolls(b, "batch-1"); got != 2 {
		t.Errorf("emptyPolls = %d, want unchanged 2 (a poll error is not an empty result)", got)
	}
	if !rfInFlight(b, "op-1") || !rfOpBatchHas(b, "op-1") {
		t.Error("op must stay in flight (and in opBatch) across poll errors")
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Errorf("failure handler called on poll error: %+v", calls)
	}
}

// TestRfPollBatchUnknownAndForeignResults pins that an UNKNOWN state and a result for an op the
// batch does not contain both leave the batch's items untouched and in flight.
func TestRfPollBatchUnknownAndForeignResults(t *testing.T) {
	m := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{
				{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_UNKNOWN},
				{OperationId: "op-foreign", State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED},
			}, nil
		},
	}
	b := newTestBatcher(m)
	rec := &failureRecorder{}
	b.SetFailureHandler(rec.handler)
	registerBatch(b, "batch-1", opKindAdd, time.Now(), "op-1", "op-2")

	b.pollBatch(context.Background(), "batch-1")

	if got := remainingOpIDs(b, "batch-1"); !reflect.DeepEqual(got, []string{"op-1", "op-2"}) {
		t.Errorf("remaining = %v, want [op-1 op-2]", got)
	}
	if !rfInFlight(b, "op-1") || !rfInFlight(b, "op-2") {
		t.Error("ops without a terminal result must stay in flight")
	}
	if b.Succeeded("op-foreign") || b.Succeeded("op-1") {
		t.Error("a result for an op outside the batch must not be recorded")
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Errorf("failure handler called: %+v", calls)
	}
	if got := rfEmptyPolls(b, "batch-1"); got != 0 {
		t.Errorf("emptyPolls = %d, want 0 (the response was non-empty)", got)
	}
}

// TestRfPollBatchNonEmptyResetsEmptyPolls pins that the empty-poll expiry counter is a
// consecutive count: one non-empty response restarts it.
func TestRfPollBatchNonEmptyResetsEmptyPolls(t *testing.T) {
	var results []*v1.KarpenterBatchNodeResult
	m := &mockBroker{pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) { return results, nil }}
	b := newTestBatcher(m)
	rec := &failureRecorder{}
	b.SetFailureHandler(rec.handler)
	registerBatch(b, "batch-1", opKindAdd, time.Now(), "op-1")
	ctx := context.Background()

	for i := 1; i < maxConsecutiveEmptyPolls; i++ {
		b.pollBatch(ctx, "batch-1")
	}
	if got := rfEmptyPolls(b, "batch-1"); got != maxConsecutiveEmptyPolls-1 {
		t.Fatalf("emptyPolls = %d, want %d", got, maxConsecutiveEmptyPolls-1)
	}
	results = []*v1.KarpenterBatchNodeResult{{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_RUNNING}}
	b.pollBatch(ctx, "batch-1")
	if got := rfEmptyPolls(b, "batch-1"); got != 0 {
		t.Fatalf("emptyPolls = %d after a non-empty poll, want 0", got)
	}
	results = nil
	for i := 1; i < maxConsecutiveEmptyPolls; i++ {
		b.pollBatch(ctx, "batch-1")
	}
	if !b.Tracking("batch-1") {
		t.Error("batch expired although the empty polls were not consecutive")
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Errorf("failure handler called: %+v", calls)
	}
}

// TestRfExpiryClearsOpBatch pins that both expiry paths (empty polls and max age) drop the
// operation from opBatch as well as from inFlight.
func TestRfExpiryClearsOpBatch(t *testing.T) {
	t.Run("empty-poll expiry", func(t *testing.T) {
		b := newTestBatcher(&mockBroker{})
		registerBatch(b, "batch-1", opKindAdd, time.Now(), "op-1")
		for i := 0; i < maxConsecutiveEmptyPolls; i++ {
			b.pollBatch(context.Background(), "batch-1")
		}
		if rfInFlight(b, "op-1") || rfOpBatchHas(b, "op-1") {
			t.Error("expired op still in inFlight/opBatch")
		}
	})
	t.Run("max-age expiry", func(t *testing.T) {
		b := newTestBatcher(&mockBroker{})
		registerBatch(b, "batch-1", opKindRemove, time.Now().Add(-maxBatchAge-time.Minute), "op-1")
		b.pollBatch(context.Background(), "batch-1")
		if rfInFlight(b, "op-1") || rfOpBatchHas(b, "op-1") {
			t.Error("expired op still in inFlight/opBatch")
		}
	})
}

// TestRfPollBatchClearsOpBatchOnTerminal is the regression test for R1-prov-batcher-1: opBatch
// is documented as "kept in step with inFlight", so an operation that reaches SUCCEEDED or FAILED
// through a status poll must leave opBatch exactly as it leaves inFlight (pollBatch used to
// delete from inFlight only, growing opBatch by one dead entry per completed operation).
func TestRfPollBatchClearsOpBatchOnTerminal(t *testing.T) {
	m := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{
				{OperationId: "op-s", State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED},
				{OperationId: "op-f", State: v1.KARPENTER_NODE_OPERATION_STATE_FAILED, Detail: "drain timeout"},
			}, nil
		},
	}
	b := newTestBatcher(m)
	b.SetFailureHandler((&failureRecorder{}).handler)
	registerBatch(b, "batch-1", opKindRemove, time.Now(), "op-s", "op-f")

	b.pollBatch(context.Background(), "batch-1")

	for _, op := range []string{"op-s", "op-f"} {
		if rfInFlight(b, op) {
			t.Errorf("%s still in flight after its terminal poll", op)
		}
		if rfOpBatchHas(b, op) {
			t.Errorf("%s still in opBatch after its terminal poll: opBatch must be kept in step with inFlight", op)
		}
	}
}

// ──────────────────────────── succeeded retention ──────────────────────────

// TestRfSucceededRetentionOnPoll pins the retention a poller tick applies while a batch is being
// polled: an entry older than succeededRetention is pruned, a younger one is kept, and the batch
// itself is still polled on the same tick.
func TestRfSucceededRetentionOnPoll(t *testing.T) {
	m := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_RUNNING}}, nil
		},
	}
	b := newTestBatcher(m)
	registerBatch(b, "batch-1", opKindAdd, time.Now(), "op-1")
	rfSucceededAt(b, "old", time.Now().Add(-succeededRetention-time.Minute))
	rfSucceededAt(b, "young", time.Now().Add(-succeededRetention+time.Minute))

	b.pollAll(context.Background())

	if b.Succeeded("old") {
		t.Error("entry older than succeededRetention survived a poller tick")
	}
	if !b.Succeeded("young") {
		t.Error("entry younger than succeededRetention was pruned")
	}
	if m.pollCallCount() != 1 || !b.Tracking("batch-1") {
		t.Errorf("poll calls = %d, tracking = %t; the tick must still poll the RUNNING batch", m.pollCallCount(), b.Tracking("batch-1"))
	}
}

// TestRfSucceededPrunedWithoutActiveBatch is the regression test for R1-prov-batcher-3: the
// documented 2h retention must hold whether or not a batch is being polled. With no in-progress
// batch, a poller tick (pollAll) must still prune entries older than succeededRetention (pruning
// used to run only inside pollBatch after a non-empty result, so a quiet cluster kept every
// SUCCEEDED opID from its last burst until the next batch completed).
func TestRfSucceededPrunedWithoutActiveBatch(t *testing.T) {
	b := newTestBatcher(&mockBroker{})
	rfSucceededAt(b, "old", time.Now().Add(-succeededRetention-time.Minute))
	rfSucceededAt(b, "young", time.Now().Add(-time.Minute))

	b.pollAll(context.Background())

	if b.Succeeded("old") {
		t.Error("entry older than succeededRetention survived a poller tick with no in-progress batch")
	}
	if !b.Succeeded("young") {
		t.Error("entry younger than succeededRetention was pruned")
	}
}

// TestRfEnqueueAfterSucceededResends pins the documented "one redundant round trip": once an op
// is SUCCEEDED it is no longer in flight, so a further EnqueueRemove for the same opID (Delete()
// only stops calling once it has seen Succeeded) is queued again rather than short-circuited.
func TestRfEnqueueAfterSucceededResends(t *testing.T) {
	m := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{{OperationId: "op-rem", State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED}}, nil
		},
	}
	b := newTestBatcher(m)
	registerBatch(b, "batch-1", opKindRemove, time.Now(), "op-rem")
	b.pollBatch(context.Background(), "batch-1")
	if !b.Succeeded("op-rem") {
		t.Fatal("precondition: op-rem SUCCEEDED")
	}

	ch := b.EnqueueRemove("op-rem", RemoveNodesRequest{})
	select {
	case r := <-ch:
		t.Fatalf("unexpected immediate result %+v: a SUCCEEDED op is not in flight and must be queued", r)
	default:
	}
	if len(b.queue) != 1 {
		t.Errorf("queue length = %d, want 1 (redundant re-send is queued)", len(b.queue))
	}
	if !b.Succeeded("op-rem") {
		t.Error("re-enqueueing must not forget the SUCCEEDED record")
	}
}

// ──────────────────────────── failure handler / Cancel ─────────────────────

// TestRfFailItemsWithoutHandler pins that terminal failures and expiries are safe with no
// FailureHandler registered: nothing panics and state is updated as usual.
func TestRfFailItemsWithoutHandler(t *testing.T) {
	m := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_FAILED, Detail: "x"}}, nil
		},
	}
	b := newTestBatcher(m)
	registerBatch(b, "batch-1", opKindAdd, time.Now(), "op-1")
	registerBatch(b, "batch-old", opKindRemove, time.Now().Add(-maxBatchAge-time.Minute), "op-2")

	b.pollAll(context.Background())
	b.failItems(context.Background(), opKindAdd, []batchItem{{operationID: "op-3"}}, expiredBatchDetail)

	if b.Tracking("batch-1") || b.Tracking("batch-old") {
		t.Error("terminal/expired batches still tracked")
	}
	if rfInFlight(b, "op-1") || rfInFlight(b, "op-2") {
		t.Error("terminal/expired ops still in flight")
	}
}

// TestRfCancelReportsOutcomeAndIgnoresCallerCancellation pins Cancel's contract: it runs the
// broker call synchronously on a context detached from the caller's (an already-cancelled
// reconcile ctx still reaches the broker) but bounded by cancelTimeout, it reports whether the
// broker actually cancelled the op, and no outcome mutates the batcher's own state.
func TestRfCancelReportsOutcomeAndIgnoresCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	m := &rfCtxBroker{mockBroker: &mockBroker{
		cancelFn: func(ids []string) ([]string, error) {
			close(entered)
			<-release
			return nil, nil // "no longer cancellable"
		},
	}}
	b := newTestBatcher(m)
	registerBatch(b, "batch-1", opKindRemove, time.Now(), "op-1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the reconcile that asked for the cancel is already gone

	type outcome struct {
		applied bool
		err     error
	}
	returned := make(chan outcome, 1)
	go func() {
		applied, err := b.Cancel(ctx, "op-1")
		returned <- outcome{applied, err}
	}()
	select {
	case <-entered:
	case <-time.After(waitTimeout):
		t.Fatal("CancelOperations was not invoked")
	}
	select {
	case <-returned:
		t.Fatal("Cancel returned before the broker answered; it must report the broker's outcome")
	case <-time.After(50 * time.Millisecond):
	}
	m.mu.Lock()
	cctx := m.cancelCtxs[0]
	m.mu.Unlock()
	if cctx.Err() != nil {
		t.Errorf("broker cancel ctx already done (%v): Cancel must detach from the caller's cancellation", cctx.Err())
	}
	if dl, ok := cctx.Deadline(); !ok || time.Until(dl) > cancelTimeout {
		t.Errorf("broker cancel ctx deadline = %v/%t, want one within cancelTimeout", dl, ok)
	}
	close(release)
	select {
	case o := <-returned:
		if o.err != nil || o.applied {
			t.Errorf("Cancel = (applied=%t, err=%v), want (false, nil) for a no-longer-cancellable op", o.applied, o.err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Cancel did not return after the broker answered")
	}

	// An op the broker does cancel is reported as applied; a broker error is surfaced.
	m.mu.Lock()
	m.cancelFn = func(ids []string) ([]string, error) { return ids, nil }
	m.mu.Unlock()
	if applied, err := b.Cancel(context.Background(), "op-1"); err != nil || !applied {
		t.Errorf("Cancel of a queued op = (applied=%t, err=%v), want (true, nil)", applied, err)
	}
	m.mu.Lock()
	m.cancelFn = func(ids []string) ([]string, error) { return nil, errors.New("broker down") }
	m.mu.Unlock()
	if applied, err := b.Cancel(context.Background(), "op-1"); err == nil || applied {
		t.Errorf("Cancel with a broker error = (applied=%t, err=%v), want (false, error)", applied, err)
	}

	// Nothing about the tracked batch changes as a result of a cancel outcome.
	if !rfInFlight(b, "op-1") || !b.Tracking("batch-1") {
		t.Error("Cancel mutated inFlight/inProgress; only the poller may do that")
	}
}

// ──────────────────────────── enqueue vs ACK concurrency ───────────────────

// TestRfEnqueueConcurrentWithRegisterAndAck is the stress form of the regression test for
// R1-prov-batcher-2: enqueueItem's in-flight check and its pending registration must be atomic
// with registerAndAck (both now run under b.mu), otherwise a second caller for the same
// operationID that lands between the ACK's in-flight mark and its resolvePending queues a
// duplicate batchItem. The invariant: once the ACK and every concurrent enqueue have returned,
// no item is queued and no waiter is pending — each caller either joined the ACKed waiters or
// was short-circuited as in flight.
func TestRfEnqueueConcurrentWithRegisterAndAck(t *testing.T) {
	const iterations = 2000
	const callers = 4
	for i := 0; i < iterations; i++ {
		b := newTestBatcher(&mockBroker{})
		first := b.EnqueueRemove("op", RemoveNodesRequest{})
		item := <-b.queue // the sender collected the item and is about to be ACKed

		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make([]<-chan BatchResult, callers)
		for j := 0; j < callers; j++ {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				<-start
				results[j] = b.EnqueueRemove("op", RemoveNodesRequest{})
			}(j)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			b.registerAndAck(opKindRemove, "batch-1", []batchItem{item})
		}()
		close(start)
		wg.Wait()

		if n := len(b.queue); n != 0 {
			t.Fatalf("iteration %d: %d duplicate item(s) queued for an operation that was ACKed concurrently", i, n)
		}
		if n := pendingLen(b); n != 0 {
			t.Fatalf("iteration %d: %d pending waiter(s) left behind after the ACK", i, n)
		}
		mustResult(t, first)
		for j, ch := range results {
			if r := mustResult(t, ch); r.Err != nil || r.BatchID != "batch-1" {
				t.Fatalf("iteration %d caller %d: result %+v, want BatchID batch-1", i, j, r)
			}
		}
	}
}
