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

// batcher.go - NodeBatcher: collects individual Create()/Delete() requests into batches,
// sends each batch to edge-broker via BatchStreamOperations, and polls for results.
//
// Flow:
//  1. Create() calls NodeBatcher.Enqueue(item) and Delete() calls NodeBatcher.EnqueueRemove(item);
//     both block on item.resultCh.
//  2. batchSender goroutine blocks until the first item arrives, then collects up to maxBatchSize
//     items within batchWindow (10s / 10 nodes). The collected batch is partitioned by kind:
//     adds go out via SendBatch, removes via SendBatchRemove — each partition is its own broker
//     batch. Collection of the next batch starts immediately.
//  3. Broker ACK (KarpenterBatchAccepted) resolves every waiter on the operation, so the blocked
//     Create()/Delete() call returns as soon as the batch is queued at the broker. For adds the
//     real ProviderID is resolved later by NodeProviderIDController when the node joins.
//  4. statusPoller goroutine polls every pollInterval for each in-progress batch. Terminal SUCCEEDED
//     operations are recorded (see Succeeded) — this is how Delete() learns a node removal actually
//     completed, which is what lets it return NodeClaimNotFound and release the node's termination
//     finalizer. Terminal FAILED results are reported to the registered FailureHandler
//     (SetFailureHandler) with the item's kind ("add"/"remove") and the broker's detail text.
//     Batches that the broker no longer knows (consecutive empty status responses) or that exceed
//     maxBatchAge are dropped, with every remaining item treated as FAILED ("batch expired at
//     broker").
//
// In-flight suppression:
//   Karpenter re-invokes Delete() every 5 seconds for the entire duration of a node removal. An
//   operationID that has been ACKed and has not yet reached a terminal state is held in the
//   in-flight set; enqueueing it again resolves immediately instead of sending a duplicate batch,
//   so a single removal cannot flood the broker's (global, 64-slot) queue with hundreds of copies.
//
// Cancellation:
//   Cancel(operationID) sends a best-effort, fire-and-forget cancel to the broker. Only
//   operations still queued (ACCEPTED) at the broker are cancelled; RUNNING or terminal
//   operations are untouched.
//
// Deduplication:
//   Enqueue/EnqueueRemove are idempotent per operationID. If Create()/Delete() is cancelled and
//   retried with the same operationID, the retry receives the same resultCh as the original call.
//   When the broker eventually delivers a result (SUCCEEDED or FAILED), all waiters on that
//   channel are unblocked. The pending map is cleaned up only when a result is delivered, so
//   retries always find the correct channel regardless of how many times the call is retried.

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"k8s.io/klog/v2"
)

const (
	defaultMaxBatchSize  = 10
	defaultBatchWindow   = 10 * time.Second
	defaultPollInterval  = 30 * time.Second
	defaultInitialDelay  = 120 * time.Second // wait before first poll after a batch is sent
	batchItemQueueBuffer = 256

	// maxBatchAge bounds how long a sent batch is tracked by the status poller. Result channels
	// were already resolved at ACK; items still unresolved at the broker after this long are
	// treated as expired (failure handler invoked, batch dropped).
	maxBatchAge = 2 * time.Hour

	// maxConsecutiveEmptyPolls is how many empty status responses in a row mark a batch as
	// unknown/expired at the broker (the broker replies with an empty result list instead of an
	// error for unknown batch IDs).
	maxConsecutiveEmptyPolls = 3

	// cancelTimeout bounds the fire-and-forget broker cancel call issued by Cancel.
	cancelTimeout = 10 * time.Second

	// succeededRetention bounds how long a SUCCEEDED operationID is remembered so Delete() can
	// report the removal complete. It only has to outlive the few seconds between the poller
	// observing SUCCEEDED and Karpenter's next Delete() reconcile; the generous window also covers
	// a slow termination. If an entry is ever pruned too early, Delete() re-sends the removal, the
	// broker skips it (the operation is already SUCCEEDED in Redis) and the next poll re-records
	// it — so the worst case is one redundant round trip, never a second node removal.
	succeededRetention = 2 * time.Hour

	// expiredBatchDetail is the failure detail reported when a batch expires (max age or
	// unknown at the broker).
	expiredBatchDetail = "batch expired at broker"
)

// Operation kinds carried on batchItem and reported to the FailureHandler.
const (
	opKindAdd    = "add"
	opKindRemove = "remove"
)

// BatchResult is the outcome delivered to each Create()/Delete() caller when the broker ACKs
// (or the send fails).
type BatchResult struct {
	ProviderID string
	Err        error
}

// FailureHandler is invoked by the status poller when the broker reports a terminal FAILED
// state for an operation. kind is "add" or "remove".
type FailureHandler func(ctx context.Context, operationID, kind, detail string)

// batchBroker is the subset of *BrokerClient the NodeBatcher calls. It exists solely so tests
// can substitute a mock; production code always passes the concrete *BrokerClient via
// NewNodeBatcher.
type batchBroker interface {
	SendBatch(ctx context.Context, nodes []*v1.KarpenterBatchNodeAddItem) (string, error)
	SendBatchRemove(ctx context.Context, nodes []*v1.KarpenterBatchNodeRemoveItem) (string, error)
	PollBatchStatus(ctx context.Context, batchID string) ([]*v1.KarpenterBatchNodeResult, error)
	CancelOperations(ctx context.Context, operationIDs []string) ([]string, error)
}

// batchItem represents one pending Create()/Delete() request inside the batcher.
// Callers waiting on the operation are tracked separately (see NodeBatcher.pending), keyed by
// operationID, so several waiters can be resolved together.
type batchItem struct {
	operationID string
	kind        string             // opKindAdd or opKindRemove
	req         AddNodesRequest    // set when kind == opKindAdd
	removeReq   RemoveNodesRequest // set when kind == opKindRemove
}

// inProgressBatch groups items that were sent together in one BatchStreamOperations call.
// A batch carries items of exactly one kind (adds and removes are sent as separate batches).
type inProgressBatch struct {
	batchID    string
	kind       string // opKindAdd or opKindRemove
	items      []batchItem
	sentAt     time.Time // when the broker ACKed the batch; used for max-age expiry
	pollAfter  time.Time // don't poll until this time (initial delay)
	emptyPolls int       // consecutive status polls that returned no results (unknown/expired at broker)
}

// NodeBatcher collects individual node-add/node-remove requests and sends them in batches to
// edge-broker.
type NodeBatcher struct {
	queue  chan batchItem
	client batchBroker

	maxBatchSize int
	batchWindow  time.Duration
	pollInterval time.Duration
	initialDelay time.Duration

	// mu guards inProgress, inFlight and succeeded.
	mu         sync.Mutex
	inProgress map[string]*inProgressBatch // batchID → batch

	// inFlight holds every operationID that has been ACKed by the broker and has not yet reached
	// a terminal state. Karpenter re-invokes Delete() every 5s for the whole life of a node
	// removal, so without this an in-flight remove would be re-sent as a fresh batch on every
	// reconcile, flooding the broker's queue. An enqueue for an in-flight operationID resolves
	// immediately instead of sending again (the caller's contract — "queued at the broker" — is
	// already satisfied).
	inFlight map[string]struct{}

	// succeeded records operationIDs the broker reported SUCCEEDED, with the time observed.
	// Delete() consults this to decide when a node removal has actually completed (see Succeeded).
	// Entries are pruned after succeededRetention.
	succeeded map[string]time.Time

	// pendingMu guards pending. pending maps operationID → the result channels of every caller
	// currently waiting on that operation. Only the first caller for an operationID enqueues a
	// batchItem; retries and concurrent callers register another channel and are all resolved
	// together, so no waiter can be left blocked on a channel that was already drained.
	// Lock order when both locks are needed: mu → pendingMu.
	pendingMu sync.Mutex
	pending   map[string][]chan BatchResult

	// failureMu guards failureHandler, which is invoked from the status poller goroutine on
	// terminal FAILED results and on batch expiry.
	failureMu      sync.Mutex
	failureHandler FailureHandler
}

// NewNodeBatcher creates a NodeBatcher using the given BrokerClient.
func NewNodeBatcher(client *BrokerClient) *NodeBatcher {
	return &NodeBatcher{
		queue:        make(chan batchItem, batchItemQueueBuffer),
		client:       client,
		maxBatchSize: defaultMaxBatchSize,
		batchWindow:  defaultBatchWindow,
		pollInterval: defaultPollInterval,
		initialDelay: defaultInitialDelay,
		inProgress:   make(map[string]*inProgressBatch),
		inFlight:     make(map[string]struct{}),
		succeeded:    make(map[string]time.Time),
		pending:      make(map[string][]chan BatchResult),
	}
}

// Succeeded reports whether the broker has confirmed operationID reached SUCCEEDED.
//
// CloudProvider.Delete() uses this to decide when a node removal has actually completed. It cannot
// use "no Kubernetes Node carries this providerID" instead: during termination Karpenter holds the
// Node object alive with its own finalizer, and that finalizer is only released once Delete()
// reports the instance is gone — so node existence is circular and never converges.
func (b *NodeBatcher) Succeeded(operationID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.succeeded[operationID]
	return ok
}

// SetFailureHandler registers fn to be called for every terminal FAILED operation reported by
// the broker (and for items of expired batches). Set it before Run, or at any time — access is
// guarded by a mutex.
func (b *NodeBatcher) SetFailureHandler(fn FailureHandler) {
	b.failureMu.Lock()
	b.failureHandler = fn
	b.failureMu.Unlock()
}

// failureHandlerFn returns the currently registered failure handler (or nil).
func (b *NodeBatcher) failureHandlerFn() FailureHandler {
	b.failureMu.Lock()
	defer b.failureMu.Unlock()
	return b.failureHandler
}

// Enqueue adds a node-add request to the batch queue and returns a channel that will receive
// the result. The caller should block on the channel (with ctx cancellation).
//
// Enqueue is idempotent per operationID: if the same operationID is enqueued again before
// its result is delivered (e.g. Create() was cancelled and retried), the same channel is
// returned and no second batchItem is added to the queue.
func (b *NodeBatcher) Enqueue(operationID string, req AddNodesRequest) <-chan BatchResult {
	return b.enqueueItem(batchItem{operationID: operationID, kind: opKindAdd, req: req})
}

// EnqueueRemove adds a node-remove request to the batch queue and returns a channel that will
// receive the result. Idempotent per operationID, exactly like Enqueue.
func (b *NodeBatcher) EnqueueRemove(operationID string, req RemoveNodesRequest) <-chan BatchResult {
	return b.enqueueItem(batchItem{operationID: operationID, kind: opKindRemove, removeReq: req})
}

// enqueueItem registers the caller as a waiter on item.operationID and, if this is the first
// waiter and the operation is not already in flight at the broker, queues a batchItem for it.
//
// The insert-then-queue-send sequence runs under a single pendingMu acquisition, so a concurrent
// dedup hit can never observe the map entry of a rolled-back (queue-full) item. The queue send is
// non-blocking (select/default), so holding the lock cannot deadlock.
func (b *NodeBatcher) enqueueItem(item batchItem) <-chan BatchResult {
	ch := make(chan BatchResult, 1)

	// Already ACKed by the broker and awaiting a terminal status: the caller's contract ("queued
	// at the broker") is satisfied, so resolve immediately rather than sending a duplicate batch.
	// Karpenter calls Delete() every 5s for the entire life of a removal, so without this each
	// reconcile would push another remove batch onto the broker's queue.
	// Lock order: mu → pendingMu (mu is released before pendingMu is taken below).
	b.mu.Lock()
	_, inFlight := b.inFlight[item.operationID]
	b.mu.Unlock()
	if inFlight {
		klog.V(4).Infof("batcher: operationID=%s already in flight at broker — not re-sending", item.operationID)
		ch <- BatchResult{}
		return ch
	}

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()
	if waiters, ok := b.pending[item.operationID]; ok {
		// A batchItem is already queued for this operationID; join the existing waiters so every
		// caller is resolved when the result arrives.
		b.pending[item.operationID] = append(waiters, ch)
		klog.V(4).Infof("batcher: dedup operationID=%s — joined %d existing waiter(s)", item.operationID, len(waiters))
		return ch
	}
	b.pending[item.operationID] = []chan BatchResult{ch}

	// The buffered queue (capacity 256) makes this non-blocking in normal operation. If the
	// queue is full, deliver an error immediately and roll back the pending entry so the
	// caller is not stuck and a retry starts fresh.
	select {
	case b.queue <- item:
	default:
		ch <- BatchResult{Err: fmt.Errorf("batcher queue full (capacity %d)", batchItemQueueBuffer)}
		delete(b.pending, item.operationID)
	}
	return ch
}

// resolvePending delivers result to every caller waiting on operationID and clears the entry.
//
// The pending entry is removed BEFORE the results are delivered: once a waiter consumes its
// result, a fast retry for the same operationID must not join a waiter list that is already being
// drained — it must start fresh instead. Each waiter channel is buffered (cap 1) and written
// exactly once, so delivery never blocks.
func (b *NodeBatcher) resolvePending(operationID string, result BatchResult) {
	b.pendingMu.Lock()
	waiters := b.pending[operationID]
	delete(b.pending, operationID)
	b.pendingMu.Unlock()

	for _, ch := range waiters {
		ch <- result
	}
}

// Run starts the batch sender and status poller goroutines. Blocks until ctx is cancelled.
func (b *NodeBatcher) Run(ctx context.Context) {
	go b.batchSender(ctx)
	b.statusPoller(ctx)
}

// Cancel sends a best-effort, fire-and-forget cancellation for operationID to the broker and
// returns immediately. Only operations still queued (ACCEPTED) at the broker are cancelled;
// RUNNING or terminal operations are untouched. Errors are logged only.
func (b *NodeBatcher) Cancel(ctx context.Context, operationID string) {
	go func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
		defer cancel()
		cancelled, err := b.client.CancelOperations(cctx, []string{operationID})
		if err != nil {
			klog.Warningf("batcher: cancel operationID=%s failed: %v", operationID, err)
			return
		}
		if len(cancelled) == 0 {
			klog.Infof("batcher: cancel operationID=%s — operation no longer cancellable (running or finished)", operationID)
			return
		}
		klog.Infof("batcher: cancelled operationID=%s at broker", operationID)
	}()
}

// ──────────────────────── batch sender goroutine ──────────────────────────

func (b *NodeBatcher) batchSender(ctx context.Context) {
	for {
		batch := b.collectBatch(ctx)
		if len(batch) == 0 {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		b.sendBatch(ctx, batch)
	}
}

// collectBatch blocks until the first item arrives, then collects until the batch window
// expires or maxBatchSize is reached. The window timer starts at the FIRST item, so an idle
// batcher never spins and the first request of a burst is delayed by at most one full window.
func (b *NodeBatcher) collectBatch(ctx context.Context) []batchItem {
	var batch []batchItem

	// Block indefinitely for the first item.
	select {
	case <-ctx.Done():
		return nil
	case item, ok := <-b.queue:
		if !ok {
			return nil
		}
		batch = append(batch, item)
	}
	if len(batch) >= b.maxBatchSize {
		return batch
	}

	timer := time.NewTimer(b.batchWindow)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return batch
		case item, ok := <-b.queue:
			if !ok {
				return batch
			}
			batch = append(batch, item)
			if len(batch) >= b.maxBatchSize {
				return batch
			}
		case <-timer.C:
			return batch
		}
	}
}

// sendBatch partitions the collected items by kind and sends each partition as its own broker
// batch (adds via SendBatch, removes via SendBatchRemove). A failed send fails only that
// partition's items.
func (b *NodeBatcher) sendBatch(ctx context.Context, batch []batchItem) {
	var adds, removes []batchItem
	for _, item := range batch {
		if item.kind == opKindRemove {
			removes = append(removes, item)
		} else {
			adds = append(adds, item)
		}
	}
	if len(adds) > 0 {
		b.sendAddBatch(ctx, adds)
	}
	if len(removes) > 0 {
		b.sendRemoveBatch(ctx, removes)
	}
}

func (b *NodeBatcher) sendAddBatch(ctx context.Context, batch []batchItem) {
	// Build the proto request.
	nodes := make([]*v1.KarpenterBatchNodeAddItem, 0, len(batch))
	for _, item := range batch {
		nodes = append(nodes, &v1.KarpenterBatchNodeAddItem{
			OperationId:  item.operationID,
			ClusterId:    item.req.ClusterID,
			ProjectId:    item.req.ProjectID,
			InstanceType: item.req.InstanceType,
			NodePoolName: item.req.NodePoolName,
		})
	}

	batchID, err := b.client.SendBatch(ctx, nodes)
	if err != nil {
		b.failBatchSend(opKindAdd, batch, err)
		return
	}
	b.registerAndAck(opKindAdd, batchID, batch)
}

func (b *NodeBatcher) sendRemoveBatch(ctx context.Context, batch []batchItem) {
	// Build the proto request.
	nodes := make([]*v1.KarpenterBatchNodeRemoveItem, 0, len(batch))
	for _, item := range batch {
		nodes = append(nodes, &v1.KarpenterBatchNodeRemoveItem{
			OperationId:  item.operationID,
			ClusterId:    item.removeReq.ClusterID,
			ProjectId:    item.removeReq.ProjectID,
			InstanceType: item.removeReq.InstanceType,
			NodePoolName: item.removeReq.NodePoolName,
			ProviderId:   item.removeReq.ProviderID,
		})
	}

	batchID, err := b.client.SendBatchRemove(ctx, nodes)
	if err != nil {
		b.failBatchSend(opKindRemove, batch, err)
		return
	}
	b.registerAndAck(opKindRemove, batchID, batch)
}

// failBatchSend delivers a send error to every item of the failed partition. The items were never
// registered as in-flight (registerAndAck did not run), so a retry re-sends them.
func (b *NodeBatcher) failBatchSend(kind string, batch []batchItem, err error) {
	klog.Errorf("batcher: send %s batch failed: %v", kind, err)
	for _, item := range batch {
		b.resolvePending(item.operationID, BatchResult{Err: fmt.Errorf("send %s batch failed: %w", kind, err)})
	}
}

// registerAndAck records the sent batch for status polling, marks its operations in-flight, and
// unblocks each caller — broker ACK is sufficient for both adds and removes. For adds the real
// ProviderID is resolved later by NodeProviderIDController when the node joins; for removes
// Delete() converges once the poller observes the operation SUCCEEDED (see Succeeded).
func (b *NodeBatcher) registerAndAck(kind, batchID string, batch []batchItem) {
	klog.Infof("batcher: sent %s batch batchID=%s nodeCount=%d", kind, batchID, len(batch))

	now := time.Now()
	b.mu.Lock()
	b.inProgress[batchID] = &inProgressBatch{
		batchID:   batchID,
		kind:      kind,
		items:     batch,
		sentAt:    now,
		pollAfter: now.Add(b.initialDelay),
	}
	// Mark in-flight before resolving the waiters, so a retry that lands immediately after a
	// caller returns sees the operation as in flight and does not re-send it.
	for _, item := range batch {
		b.inFlight[item.operationID] = struct{}{}
	}
	b.mu.Unlock()

	for _, item := range batch {
		b.resolvePending(item.operationID, BatchResult{})
	}
}

// clearInFlight drops operationIDs from the in-flight set. Callers must hold b.mu.
func (b *NodeBatcher) clearInFlightLocked(items []batchItem) {
	for _, item := range items {
		delete(b.inFlight, item.operationID)
	}
}

// pruneSucceededLocked bounds the succeeded set. Callers must hold b.mu.
func (b *NodeBatcher) pruneSucceededLocked(now time.Time) {
	for opID, at := range b.succeeded {
		if now.Sub(at) > succeededRetention {
			delete(b.succeeded, opID)
		}
	}
}

// ──────────────────────── status poller goroutine ─────────────────────────

func (b *NodeBatcher) statusPoller(ctx context.Context) {
	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.pollAll(ctx)
		}
	}
}

func (b *NodeBatcher) pollAll(ctx context.Context) {
	b.mu.Lock()
	batchIDs := make([]string, 0, len(b.inProgress))
	for id := range b.inProgress {
		batchIDs = append(batchIDs, id)
	}
	b.mu.Unlock()

	for _, batchID := range batchIDs {
		b.pollBatch(ctx, batchID)
	}
}

func (b *NodeBatcher) pollBatch(ctx context.Context, batchID string) {
	b.mu.Lock()
	bp, ok := b.inProgress[batchID]
	if !ok {
		b.mu.Unlock()
		return
	}
	// Respect initial delay before first poll.
	if time.Now().Before(bp.pollAfter) {
		b.mu.Unlock()
		return
	}
	// Never track a batch longer than maxBatchAge. Result channels were already resolved at
	// ACK; items still unresolved at the broker get the expired-batch failure treatment.
	if time.Since(bp.sentAt) > maxBatchAge {
		klog.Warningf("batcher: batch batchID=%s exceeded max age %s with %d unresolved item(s) — dropping", batchID, maxBatchAge, len(bp.items))
		expired, kind := bp.items, bp.kind
		delete(b.inProgress, batchID)
		b.clearInFlightLocked(expired)
		b.mu.Unlock()
		b.failItems(ctx, kind, expired, expiredBatchDetail)
		return
	}
	b.mu.Unlock()

	results, err := b.client.PollBatchStatus(ctx, batchID)
	if err != nil {
		klog.Warningf("batcher: PollBatchStatus batchID=%s failed: %v", batchID, err)
		return
	}

	// Build a map from operationID → result for quick lookup.
	resultMap := make(map[string]*v1.KarpenterBatchNodeResult, len(results))
	for _, r := range results {
		resultMap[r.GetOperationId()] = r
	}

	// Failures are collected under the lock and reported to the failure handler after
	// releasing it (the handler may call the Kubernetes API).
	type failedOp struct {
		operationID string
		detail      string
	}
	var failed []failedOp

	b.mu.Lock()
	bp, ok = b.inProgress[batchID]
	if !ok {
		b.mu.Unlock()
		return
	}

	// The broker replies with an empty result list for unknown/expired batch IDs (instead of a
	// stream error). After maxConsecutiveEmptyPolls in a row, treat every remaining item as
	// failed and drop the batch.
	if len(results) == 0 {
		bp.emptyPolls++
		if bp.emptyPolls >= maxConsecutiveEmptyPolls {
			klog.Warningf("batcher: batch batchID=%s unknown at broker after %d consecutive empty polls — dropping %d item(s)", batchID, bp.emptyPolls, len(bp.items))
			expired, kind := bp.items, bp.kind
			delete(b.inProgress, batchID)
			b.clearInFlightLocked(expired)
			b.mu.Unlock()
			b.failItems(ctx, kind, expired, expiredBatchDetail)
			return
		}
		b.mu.Unlock()
		return
	}
	bp.emptyPolls = 0

	now := time.Now()
	allDone := true
	remaining := bp.items[:0] // reuse slice
	for _, item := range bp.items {
		nr, found := resultMap[item.operationID]
		if !found {
			allDone = false
			remaining = append(remaining, item)
			continue
		}
		switch nr.GetState() {
		case v1.KARPENTER_NODE_OPERATION_STATE_SUCCEEDED:
			pid := ""
			if pids := nr.GetProviderIds(); len(pids) > 0 {
				pid = pids[0]
			}
			klog.Infof("batcher: node %s succeeded operationID=%s providerID=%s", bp.kind, item.operationID, pid)
			// Record the completion so Delete() can report a finished removal to Karpenter as
			// NodeClaimNotFound — the signal that releases the node's termination finalizer.
			b.succeeded[item.operationID] = now
			delete(b.inFlight, item.operationID)
		case v1.KARPENTER_NODE_OPERATION_STATE_FAILED:
			klog.Warningf("batcher: node %s failed operationID=%s: %s", bp.kind, item.operationID, nr.GetDetail())
			failed = append(failed, failedOp{operationID: item.operationID, detail: nr.GetDetail()})
			// Not in flight any more: a retry (Karpenter re-invoking Delete, or a fresh Create)
			// must be able to re-send this operation.
			delete(b.inFlight, item.operationID)
		default:
			// ACCEPTED or RUNNING — not done yet.
			allDone = false
			remaining = append(remaining, item)
		}
	}
	b.pruneSucceededLocked(now)

	if allDone || len(remaining) == 0 {
		delete(b.inProgress, batchID)
		klog.Infof("batcher: batch complete batchID=%s", batchID)
	} else {
		bp.items = remaining
	}
	kind := bp.kind
	b.mu.Unlock()

	if fn := b.failureHandlerFn(); fn != nil {
		for _, f := range failed {
			fn(ctx, f.operationID, kind, f.detail)
		}
	}
}

// failItems applies the expired-batch failure treatment: a warning log plus a failure-handler
// invocation per item. Result channels were already resolved at broker ACK, so only the
// handler is notified.
func (b *NodeBatcher) failItems(ctx context.Context, kind string, items []batchItem, detail string) {
	fn := b.failureHandlerFn()
	for _, item := range items {
		klog.Warningf("batcher: node %s failed operationID=%s: %s", kind, item.operationID, detail)
		if fn != nil {
			fn(ctx, item.operationID, kind, detail)
		}
	}
}
