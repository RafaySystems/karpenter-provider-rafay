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

// batcher_test.go — unit tests for NodeBatcher using a mock batchBroker (no real gRPC).
//
// Internal methods (collectBatch, sendBatch, pollBatch) are exercised directly so each test is
// deterministic; the long-running Run() goroutines are never started.

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
)

// waitTimeout is the upper bound for any blocking assertion in this file. Tests that rely on it
// only ever wait this long on failure; the happy path completes in milliseconds.
const waitTimeout = 3 * time.Second

// ──────────────────────────── mock batchBroker ────────────────────────────

// mockBroker is a scripted batchBroker. Each method records its call and delegates to the
// corresponding fn if set, otherwise returns a benign default.
type mockBroker struct {
	mu sync.Mutex

	sendBatchFn       func(nodes []*v1.KarpenterBatchNodeAddItem) (string, error)
	sendBatchRemoveFn func(nodes []*v1.KarpenterBatchNodeRemoveItem) (string, error)
	pollFn            func(batchID string) ([]*v1.KarpenterBatchNodeResult, error)
	cancelFn          func(operationIDs []string) ([]string, error)

	sendBatchCalls       [][]*v1.KarpenterBatchNodeAddItem
	sendBatchRemoveCalls [][]*v1.KarpenterBatchNodeRemoveItem
	pollCalls            []string
	cancelCalls          [][]string
}

var _ batchBroker = (*mockBroker)(nil)

func (m *mockBroker) SendBatch(_ context.Context, nodes []*v1.KarpenterBatchNodeAddItem) (string, error) {
	m.mu.Lock()
	m.sendBatchCalls = append(m.sendBatchCalls, nodes)
	fn := m.sendBatchFn
	m.mu.Unlock()
	if fn != nil {
		return fn(nodes)
	}
	return "batch-add", nil
}

func (m *mockBroker) SendBatchRemove(_ context.Context, nodes []*v1.KarpenterBatchNodeRemoveItem) (string, error) {
	m.mu.Lock()
	m.sendBatchRemoveCalls = append(m.sendBatchRemoveCalls, nodes)
	fn := m.sendBatchRemoveFn
	m.mu.Unlock()
	if fn != nil {
		return fn(nodes)
	}
	return "batch-remove", nil
}

func (m *mockBroker) PollBatchStatus(_ context.Context, batchID string) ([]*v1.KarpenterBatchNodeResult, error) {
	m.mu.Lock()
	m.pollCalls = append(m.pollCalls, batchID)
	fn := m.pollFn
	m.mu.Unlock()
	if fn != nil {
		return fn(batchID)
	}
	return nil, nil // unknown batch → empty result list, like the real broker
}

func (m *mockBroker) CancelOperations(_ context.Context, operationIDs []string) ([]string, error) {
	m.mu.Lock()
	m.cancelCalls = append(m.cancelCalls, operationIDs)
	fn := m.cancelFn
	m.mu.Unlock()
	if fn != nil {
		return fn(operationIDs)
	}
	return operationIDs, nil
}

func (m *mockBroker) pollCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pollCalls)
}

// ──────────────────────────── test helpers ────────────────────────────────

// newTestBatcher builds a NodeBatcher wired to the given mock instead of a real *BrokerClient.
func newTestBatcher(broker batchBroker) *NodeBatcher {
	b := NewNodeBatcher(nil)
	b.client = broker
	return b
}

// registerBatch installs an in-progress batch directly (as registerAndAck would), with pollAfter
// already in the past so pollBatch proceeds immediately. Its operations are marked in-flight, so
// re-enqueueing them is suppressed exactly as it would be after a real broker ACK.
func registerBatch(b *NodeBatcher, batchID, kind string, sentAt time.Time, opIDs ...string) {
	items := make([]batchItem, 0, len(opIDs))
	for _, id := range opIDs {
		items = append(items, batchItem{operationID: id, kind: kind})
	}
	b.mu.Lock()
	b.inProgress[batchID] = &inProgressBatch{
		batchID:   batchID,
		kind:      kind,
		items:     items,
		sentAt:    sentAt,
		pollAfter: time.Now().Add(-time.Minute),
	}
	for _, id := range opIDs {
		b.inFlight[id] = struct{}{}
	}
	b.mu.Unlock()
}

// mustResult asserts a result is already buffered on ch (results are delivered synchronously by
// sendBatch / enqueueItem before they return) and returns it.
func mustResult(t *testing.T, ch <-chan BatchResult) BatchResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	default:
		t.Fatal("expected a result on the channel, got none")
		return BatchResult{}
	}
}

func pendingLen(b *NodeBatcher) int {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	return len(b.pending)
}

func pendingHas(b *NodeBatcher, operationID string) bool {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	_, ok := b.pending[operationID]
	return ok
}

func inProgressBatchIDs(b *NodeBatcher) map[string]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make(map[string]string, len(b.inProgress))
	for id, bp := range b.inProgress {
		out[id] = bp.kind
	}
	return out
}

func remainingOpIDs(b *NodeBatcher, batchID string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	bp, ok := b.inProgress[batchID]
	if !ok {
		return nil
	}
	ids := make([]string, 0, len(bp.items))
	for _, item := range bp.items {
		ids = append(ids, item.operationID)
	}
	return ids
}

// failureCall records one FailureHandler invocation.
type failureCall struct {
	operationID string
	kind        string
	detail      string
}

// failureRecorder is a FailureHandler that records every call.
type failureRecorder struct {
	mu    sync.Mutex
	calls []failureCall
}

func (r *failureRecorder) handler(_ context.Context, operationID, kind, detail string) {
	r.mu.Lock()
	r.calls = append(r.calls, failureCall{operationID: operationID, kind: kind, detail: detail})
	r.mu.Unlock()
}

func (r *failureRecorder) snapshot() []failureCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]failureCall(nil), r.calls...)
}

// ──────────────────────────── 1. Enqueue dedup ─────────────────────────────

func TestEnqueueDedup(t *testing.T) {
	tests := []struct {
		name    string
		enqueue func(b *NodeBatcher, opID string) <-chan BatchResult
	}{
		{
			name: "add",
			enqueue: func(b *NodeBatcher, opID string) <-chan BatchResult {
				return b.Enqueue(opID, AddNodesRequest{ClusterID: "c1"})
			},
		},
		{
			name: "remove",
			enqueue: func(b *NodeBatcher, opID string) <-chan BatchResult {
				return b.EnqueueRemove(opID, RemoveNodesRequest{ClusterID: "c1"})
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBatcher(&mockBroker{})

			ch1 := tc.enqueue(b, "op-1")
			ch2 := tc.enqueue(b, "op-1")

			// Each caller gets its own channel, but the operation is queued exactly once and both
			// waiters are tracked under the single operationID.
			if got := len(b.queue); got != 1 {
				t.Errorf("queue length = %d, want 1 (dedup must not enqueue a second item)", got)
			}
			if got := pendingLen(b); got != 1 {
				t.Errorf("pending map size = %d, want 1 operationID", got)
			}
			select {
			case r := <-ch1:
				t.Errorf("unexpected early result before send: %+v", r)
			default:
			}

			// Both waiters — the original and the dedup'd retry — must be resolved by the single
			// result. Delivering to only one would leave the other blocked until its context died.
			b.resolvePending("op-1", BatchResult{})
			mustResult(t, ch1)
			mustResult(t, ch2)
			if pendingHas(b, "op-1") {
				t.Error("pending entry must be cleared once the result is delivered")
			}
		})
	}
}

// TestEnqueueSuppressedWhileInFlight verifies that an operation already ACKed by the broker is not
// re-sent. Karpenter calls Delete() every 5s for the whole life of a removal, so without this a
// single removal would push a duplicate batch onto the broker's queue on every reconcile.
func TestEnqueueSuppressedWhileInFlight(t *testing.T) {
	b := newTestBatcher(&mockBroker{})
	registerBatch(b, "batch-1", opKindRemove, time.Now(), "op-rem")

	ch := b.EnqueueRemove("op-rem", RemoveNodesRequest{ClusterID: "c1"})

	if got := len(b.queue); got != 0 {
		t.Errorf("queue length = %d, want 0 (an in-flight operation must not be re-sent)", got)
	}
	if r := mustResult(t, ch); r.Err != nil {
		t.Errorf("in-flight enqueue should resolve successfully, got err: %v", r.Err)
	}
}

// TestSucceededRecordedOnPoll verifies the signal Delete() relies on to converge: once the broker
// reports a remove SUCCEEDED, Succeeded() reports it and the op is no longer in flight.
func TestSucceededRecordedOnPoll(t *testing.T) {
	broker := &mockBroker{
		pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) {
			return []*v1.KarpenterBatchNodeResult{{
				OperationId: "op-rem",
				State:       v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED,
			}}, nil
		},
	}
	b := newTestBatcher(broker)
	registerBatch(b, "batch-1", opKindRemove, time.Now(), "op-rem")

	if b.Succeeded("op-rem") {
		t.Fatal("operation must not be reported succeeded before the broker says so")
	}

	b.pollBatch(context.Background(), "batch-1")

	if !b.Succeeded("op-rem") {
		t.Error("SUCCEEDED remove must be recorded so Delete() can report the instance gone")
	}
	b.mu.Lock()
	_, stillInFlight := b.inFlight["op-rem"]
	b.mu.Unlock()
	if stillInFlight {
		t.Error("a terminal operation must be cleared from the in-flight set")
	}
}

// ──────────────────────────── 2. Queue-full rollback ───────────────────────

func TestEnqueueQueueFull(t *testing.T) {
	b := newTestBatcher(&mockBroker{})
	b.queue = make(chan batchItem, 1) // shrink the queue so it is trivially fillable

	// Fill the queue.
	chOK := b.Enqueue("op-ok", AddNodesRequest{})
	select {
	case r := <-chOK:
		t.Fatalf("unexpected result for queued item: %+v", r)
	default:
	}

	// Next enqueue must fail immediately with a queue-full error.
	chFull := b.Enqueue("op-full", AddNodesRequest{})
	r := mustResult(t, chFull)
	if r.Err == nil || !strings.Contains(r.Err.Error(), "batcher queue full") {
		t.Fatalf("result error = %v, want a queue-full error", r.Err)
	}

	// Rollback: the rolled-back entry must be absent from pending while the queued one remains.
	// This is the deterministic equivalent of the concurrent-dedup-during-rollback guarantee:
	// insert+send happen under one pendingMu acquisition, so no other goroutine can ever observe
	// the entry of a rolled-back item.
	if pendingHas(b, "op-full") {
		t.Error("pending still contains rolled-back operationID op-full")
	}
	if !pendingHas(b, "op-ok") {
		t.Error("pending lost the successfully queued operationID op-ok")
	}

	// A retry of the rolled-back operationID must not dedup against a stale entry: it gets a
	// fresh channel and (queue still full) a fresh immediate error — never a hang.
	chRetry := b.Enqueue("op-full", AddNodesRequest{})
	if chRetry == chFull {
		t.Error("retry after rollback returned the stale channel (dedup hit on rolled-back entry)")
	}
	r = mustResult(t, chRetry)
	if r.Err == nil || !strings.Contains(r.Err.Error(), "batcher queue full") {
		t.Fatalf("retry result error = %v, want a queue-full error", r.Err)
	}
	if pendingHas(b, "op-full") {
		t.Error("pending contains op-full after second rollback")
	}
}

// ──────────────────────────── 3. collectBatch ──────────────────────────────

func TestCollectBatch(t *testing.T) {
	tests := []struct {
		name          string
		maxBatchSize  int
		window        time.Duration
		preQueued     []string // queued before collectBatch starts
		lateQueued    []string // queued after asserting collectBatch is blocked
		wantBatch     []string
		wantQueueLeft int
	}{
		{
			name:         "blocks for first item then window closes",
			maxBatchSize: 10,
			window:       100 * time.Millisecond,
			lateQueued:   []string{"op-1"},
			wantBatch:    []string{"op-1"},
		},
		{
			name:         "window collects multiple items below max",
			maxBatchSize: 10,
			window:       150 * time.Millisecond,
			preQueued:    []string{"op-1", "op-2", "op-3"},
			wantBatch:    []string{"op-1", "op-2", "op-3"},
		},
		{
			name:          "maxBatchSize cap returns before window expires",
			maxBatchSize:  2,
			window:        10 * time.Second, // far longer than waitTimeout: only the cap can return in time
			preQueued:     []string{"op-1", "op-2", "op-3"},
			wantBatch:     []string{"op-1", "op-2"},
			wantQueueLeft: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newTestBatcher(&mockBroker{})
			b.maxBatchSize = tc.maxBatchSize
			b.batchWindow = tc.window

			for _, id := range tc.preQueued {
				b.queue <- batchItem{operationID: id}
			}

			done := make(chan []batchItem, 1)
			go func() { done <- b.collectBatch(context.Background()) }()

			if len(tc.preQueued) == 0 {
				// No busy spin: with an empty queue collectBatch must block well past the window,
				// not return an empty batch.
				select {
				case batch := <-done:
					t.Fatalf("collectBatch returned %d item(s) before any item was queued", len(batch))
				case <-time.After(5 * tc.window):
				}
			}

			for _, id := range tc.lateQueued {
				b.queue <- batchItem{operationID: id}
			}

			var batch []batchItem
			select {
			case batch = <-done:
			case <-time.After(waitTimeout):
				t.Fatal("collectBatch did not return in time")
			}

			got := make([]string, 0, len(batch))
			for _, item := range batch {
				got = append(got, item.operationID)
			}
			if !reflect.DeepEqual(got, tc.wantBatch) {
				t.Errorf("batch = %v, want %v", got, tc.wantBatch)
			}
			if left := len(b.queue); left != tc.wantQueueLeft {
				t.Errorf("items left in queue = %d, want %d", left, tc.wantQueueLeft)
			}
		})
	}
}

// ──────────────────────────── 4. sendBatch partitions ──────────────────────

func TestSendBatchPartitions(t *testing.T) {
	sendErr := errors.New("broker dial failed")
	tests := []struct {
		name    string
		addErr  error // returned by SendBatch; SendBatchRemove always succeeds
		wantAdd bool  // add partition tracked in inProgress and ACKed without error
	}{
		{name: "mixed adds and removes split into two broker calls", addErr: nil, wantAdd: true},
		{name: "SendBatch error fails only the add partition", addErr: sendErr, wantAdd: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockBroker{}
			if tc.addErr != nil {
				m.sendBatchFn = func([]*v1.KarpenterBatchNodeAddItem) (string, error) { return "", tc.addErr }
			}
			b := newTestBatcher(m)

			addCh := b.Enqueue("op-add", AddNodesRequest{
				ClusterID: "c1", ProjectID: "p1", InstanceType: "m5.large", NodePoolName: "np1",
			})
			remCh := b.EnqueueRemove("op-rem", RemoveNodesRequest{
				ClusterID: "c1", ProjectID: "p1", InstanceType: "m5.large", NodePoolName: "np1",
				ProviderID: "rafay://c1/n1",
			})

			b.sendBatch(context.Background(), []batchItem{<-b.queue, <-b.queue})

			// Exactly one broker call per kind, each with only its partition's items.
			m.mu.Lock()
			addCalls, remCalls := m.sendBatchCalls, m.sendBatchRemoveCalls
			m.mu.Unlock()
			if len(addCalls) != 1 || len(addCalls[0]) != 1 || addCalls[0][0].GetOperationId() != "op-add" {
				t.Fatalf("SendBatch calls = %v, want one call with [op-add]", addCalls)
			}
			if got := addCalls[0][0].GetInstanceType(); got != "m5.large" {
				t.Errorf("add item InstanceType = %q, want m5.large", got)
			}
			if len(remCalls) != 1 || len(remCalls[0]) != 1 || remCalls[0][0].GetOperationId() != "op-rem" {
				t.Fatalf("SendBatchRemove calls = %v, want one call with [op-rem]", remCalls)
			}
			if got := remCalls[0][0].GetProviderId(); got != "rafay://c1/n1" {
				t.Errorf("remove item ProviderId = %q, want rafay://c1/n1", got)
			}

			// The remove partition is always ACKed with an empty BatchResult.
			remRes := mustResult(t, remCh)
			if remRes.Err != nil || remRes.ProviderID != "" {
				t.Errorf("remove result = %+v, want zero BatchResult", remRes)
			}

			addRes := mustResult(t, addCh)
			if tc.wantAdd {
				if addRes.Err != nil || addRes.ProviderID != "" {
					t.Errorf("add result = %+v, want zero BatchResult", addRes)
				}
			} else {
				if addRes.Err == nil || !strings.Contains(addRes.Err.Error(), sendErr.Error()) {
					t.Errorf("add result error = %v, want wrapped %q", addRes.Err, sendErr)
				}
			}

			// Pending is fully cleaned in both outcomes (ACK or send failure).
			if got := pendingLen(b); got != 0 {
				t.Errorf("pending map size = %d, want 0", got)
			}

			// Only ACKed partitions are tracked for status polling.
			tracked := inProgressBatchIDs(b)
			if kind, ok := tracked["batch-remove"]; !ok || kind != opKindRemove {
				t.Errorf("inProgress = %v, want batch-remove tracked with kind %q", tracked, opKindRemove)
			}
			if kind, ok := tracked["batch-add"]; ok != tc.wantAdd || (ok && kind != opKindAdd) {
				t.Errorf("inProgress = %v, want batch-add tracked=%t with kind %q", tracked, tc.wantAdd, opKindAdd)
			}
		})
	}
}

// ──────────────────────────── 5. pollBatch results ─────────────────────────

func TestPollBatch(t *testing.T) {
	tests := []struct {
		name          string
		kind          string
		opIDs         []string
		results       []*v1.KarpenterBatchNodeResult
		wantFailures  []failureCall
		wantDropped   bool
		wantRemaining []string
	}{
		{
			name:  "FAILED invokes failure handler with kind and detail",
			kind:  opKindAdd,
			opIDs: []string{"op-1"},
			results: []*v1.KarpenterBatchNodeResult{
				{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_FAILED, Detail: "no capacity"},
			},
			wantFailures: []failureCall{{operationID: "op-1", kind: opKindAdd, detail: "no capacity"}},
			wantDropped:  true,
		},
		{
			name:  "SUCCEEDED is log-only",
			kind:  opKindRemove,
			opIDs: []string{"op-1"},
			results: []*v1.KarpenterBatchNodeResult{
				{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED, ProviderIds: []string{"rafay://c1/n1"}},
			},
			wantDropped: true,
		},
		{
			name:  "partial results keep remaining items",
			kind:  opKindAdd,
			opIDs: []string{"op-1", "op-2", "op-3"},
			results: []*v1.KarpenterBatchNodeResult{
				{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED},
				{OperationId: "op-2", State: v1.KARPENTER_NODE_OPERATION_STATE_RUNNING},
				// op-3 missing from the response entirely
			},
			wantRemaining: []string{"op-2", "op-3"},
		},
		{
			name:  "mixed failure and still-pending item",
			kind:  opKindRemove,
			opIDs: []string{"op-1", "op-2"},
			results: []*v1.KarpenterBatchNodeResult{
				{OperationId: "op-1", State: v1.KARPENTER_NODE_OPERATION_STATE_FAILED, Detail: "drain timeout"},
				{OperationId: "op-2", State: v1.KARPENTER_NODE_OPERATION_STATE_ACCEPTED},
			},
			wantFailures:  []failureCall{{operationID: "op-1", kind: opKindRemove, detail: "drain timeout"}},
			wantRemaining: []string{"op-2"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := &mockBroker{
				pollFn: func(string) ([]*v1.KarpenterBatchNodeResult, error) { return tc.results, nil },
			}
			b := newTestBatcher(m)
			rec := &failureRecorder{}
			b.SetFailureHandler(rec.handler)
			registerBatch(b, "batch-1", tc.kind, time.Now(), tc.opIDs...)

			b.pollBatch(context.Background(), "batch-1")

			if got := rec.snapshot(); !reflect.DeepEqual(got, tc.wantFailures) {
				t.Errorf("failure handler calls = %+v, want %+v", got, tc.wantFailures)
			}
			remaining := remainingOpIDs(b, "batch-1")
			if tc.wantDropped {
				if _, ok := inProgressBatchIDs(b)["batch-1"]; ok {
					t.Error("batch-1 still tracked, want dropped after all items resolved")
				}
			} else if !reflect.DeepEqual(remaining, tc.wantRemaining) {
				t.Errorf("remaining items = %v, want %v", remaining, tc.wantRemaining)
			}
		})
	}
}

// ──────────────────────────── 6. empty-poll expiry ─────────────────────────

func TestPollBatchEmptyPollExpiry(t *testing.T) {
	m := &mockBroker{} // default PollBatchStatus returns an empty result list
	b := newTestBatcher(m)
	rec := &failureRecorder{}
	b.SetFailureHandler(rec.handler)
	registerBatch(b, "batch-1", opKindAdd, time.Now(), "op-1", "op-2")
	ctx := context.Background()

	// The first maxConsecutiveEmptyPolls-1 empty polls keep the batch alive.
	for i := 1; i < maxConsecutiveEmptyPolls; i++ {
		b.pollBatch(ctx, "batch-1")
		if _, ok := inProgressBatchIDs(b)["batch-1"]; !ok {
			t.Fatalf("batch dropped after %d empty poll(s), want it kept until %d", i, maxConsecutiveEmptyPolls)
		}
		if got := rec.snapshot(); len(got) != 0 {
			t.Fatalf("failure handler called after %d empty poll(s): %+v", i, got)
		}
	}

	// The Nth consecutive empty poll expires the batch.
	b.pollBatch(ctx, "batch-1")
	if _, ok := inProgressBatchIDs(b)["batch-1"]; ok {
		t.Errorf("batch still tracked after %d consecutive empty polls, want dropped", maxConsecutiveEmptyPolls)
	}
	want := []failureCall{
		{operationID: "op-1", kind: opKindAdd, detail: expiredBatchDetail},
		{operationID: "op-2", kind: opKindAdd, detail: expiredBatchDetail},
	}
	if got := rec.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("failure handler calls = %+v, want %+v", got, want)
	}
}

// ──────────────────────────── 7. max-age expiry ────────────────────────────

func TestPollBatchMaxAgeExpiry(t *testing.T) {
	m := &mockBroker{}
	b := newTestBatcher(m)
	rec := &failureRecorder{}
	b.SetFailureHandler(rec.handler)
	// sentAt manipulation: pretend the batch was ACKed beyond maxBatchAge ago.
	registerBatch(b, "batch-1", opKindRemove, time.Now().Add(-maxBatchAge-time.Minute), "op-1")

	b.pollBatch(context.Background(), "batch-1")

	if got := m.pollCallCount(); got != 0 {
		t.Errorf("PollBatchStatus called %d time(s), want 0 (expiry precedes the broker poll)", got)
	}
	if _, ok := inProgressBatchIDs(b)["batch-1"]; ok {
		t.Error("batch still tracked after exceeding maxBatchAge, want dropped")
	}
	want := []failureCall{{operationID: "op-1", kind: opKindRemove, detail: expiredBatchDetail}}
	if got := rec.snapshot(); !reflect.DeepEqual(got, want) {
		t.Errorf("failure handler calls = %+v, want %+v", got, want)
	}
}

// ──────────────────────────── 8. Cancel delegation ─────────────────────────

func TestCancelDelegatesToBroker(t *testing.T) {
	tests := []struct {
		name      string
		cancelled []string
		err       error
	}{
		{name: "operation cancelled at broker", cancelled: []string{"op-1"}},
		{name: "operation no longer cancellable", cancelled: nil},
		{name: "broker error is swallowed", err: errors.New("unavailable")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan []string, 1)
			m := &mockBroker{
				cancelFn: func(ids []string) ([]string, error) {
					got <- ids
					return tc.cancelled, tc.err
				},
			}
			b := newTestBatcher(m)

			b.Cancel(context.Background(), "op-1")

			select {
			case ids := <-got:
				if !reflect.DeepEqual(ids, []string{"op-1"}) {
					t.Errorf("CancelOperations called with %v, want [op-1]", ids)
				}
			case <-time.After(waitTimeout):
				t.Fatal("CancelOperations was not invoked")
			}
		})
	}
}
