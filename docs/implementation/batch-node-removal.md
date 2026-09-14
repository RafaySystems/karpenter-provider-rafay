# Batch Node Removal

Design doc for the batched node-delete path. It replaces the old single-op
`KarpenterNodeService` client (`pkg/rafay/karpenter_node_stream.go`, `AddNodes`/`RemoveNode`,
`pkg/brokerproto`), which has been **deleted** — all add/remove traffic now flows through the
NodeBatcher and `KarpenterBatchService.BatchStreamOperations`. For the batcher's general
behavior (windows, deadlines, expiry, FAILED feedback) see
[karpenter-rafay-internals.md §4](../karpenter-rafay-internals.md#4-nodebatcher-batch-send-architecture);
for the add path see [batch-node-addition.md](batch-node-addition.md).

## Overview

Node removal goes through the same PaaS layer as addition: the broker decrements the
`Worker Node SKU` catalog on the workspace compute instance and republishes it, and the platform
retires machines until the actual count matches the declared count. Like adds, removals are slow
(minutes) and serialized per cluster, so individual `Delete()` calls are collected into batches.

Key asymmetry with the add path: **the catalog is declarative — the platform chooses which
physical machine is retired.** The provider sends the node's ProviderID with each remove item,
but the current PaaS API cannot target a specific machine; only the per-`{pool, sku}` count goes
down. This is a platform limitation. The ProviderID is carried on the wire anyway so targeted
removal can be implemented broker-side later without a protocol change.

---

## Protocol

Shared types live in `edge-common/pkg/edge/v1/edge_karpenter_batch.go` (mirrored into
`edge-broker/vendor/`). The removal/cancel work added these messages:

| Type | Direction | Purpose |
|------|-----------|---------|
| `KarpenterBatchNodeRemoveItem` | client→broker | One node to remove: `operation_id`, `cluster_id`, `project_id`, `instance_type`, `node_pool_name`, `provider_id` |
| `KarpenterBatchNodeRemoveRequest` | client→broker | Remove batch payload: `batch_id` + nodes list |
| `KarpenterBatchOperationCancel` | client→broker | Best-effort cancel: `operation_ids` list |
| `KarpenterBatchCancelAck` | broker→client | `cancelled_operation_ids` — only the ops actually transitioned |
| `KarpenterBatchRejected` | broker→client | In-band rejection (`batch_id`, `reason`) — e.g. broker queue full |

The stream union messages gained new fields (proto field numbers matter — they are wire
contract):

```
KarpenterBatchStreamClientToBroker          KarpenterBatchStreamBrokerToClient
  1: batch_add                                1: batch_accepted
  2: status_poll                              2: batch_status
  3: batch_remove        ← new                3: batch_rejected      ← new
  4: cancel_ops          ← new                4: cancel_ack          ← new
```

A remove batch is ACKed with the same `KarpenterBatchAccepted` as an add batch, and its status
is polled with the same `status_poll` / `batch_status` messages.

---

## Provider Flow (karpenter-provider-rafay)

`CloudProvider.Delete()` in `pkg/cloudprovider/cloudprovider.go`:

```
Delete(nodeClaim):                        ← called by Karpenter every ~5 s until it converges
  0. operationID = <NodeClaim UID> + "-remove"
     if batcher.Succeeded(operationID):
        → return NodeClaimNotFoundError   ✓ CONVERGENCE: releases the Node's termination finalizer
  1. providerID = nodeClaim.Status.ProviderID
     if pending ("rafay://pending/<uid>"):
        findNodeProviderID()  — uncached apiReader; Nodes listed by the nodepoolname label,
                                sku_name matched in code (instance-type label OR NodeClass name),
                                created after the NodeClaim, not owned by another NodeClaim
        if no node found  → batcher.Cancel(uid)   (best-effort, see below)
                          → return NodeClaimNotFoundError
  2. resultCh = batcher.EnqueueRemove(operationID, RemoveNodesRequest{
         clusterID, projectID,            // from RAFAY_CLUSTER_ID / RAFAY_PROJECT_ID env
         instanceType, nodePoolName, providerID})
     → if the operationID is already in flight at the broker, this resolves IMMEDIATELY
       and nothing is re-sent
  3. block on resultCh → returns nil at broker ACK
     → Karpenter requeues in 5 s and re-enters at step 0
```

**The `"-remove"` operationID suffix** keeps the remove operation distinct from the add
operation of the same NodeClaim (which used the bare UID) in both the batcher's pending map and
the broker's Redis records. Without it, a remove would dedup against the completed add record
and silently no-op.

### Delete() must converge — and node existence cannot be how

Karpenter core's `awaitInstanceTermination`
(`../karpenter/pkg/controllers/node/termination/controller.go`) calls `cloudProvider.Delete()` on
**every** reconcile and releases the Node's termination finalizer **only** when `Delete()` returns a
`NodeClaimNotFoundError`. Anything else — including `nil` — requeues after 5 seconds:

```go
deleteErr := c.cloudProvider.Delete(ctx, nodeClaim)
if cloudprovider.IgnoreNodeClaimNotFoundError(deleteErr) != nil {
    return reconcile.Result{}, deleteErr
}
if !cloudprovider.IsNodeClaimNotFoundError(deleteErr) {
    return reconcile.Result{RequeueAfter: 5 * time.Second}, nil
}
```

`Delete()` used to return `nil` unconditionally after broker ACK, so it never produced the one error
that ends the loop: **NodeClaims and Nodes stayed `Terminating` forever.**

The completion signal is the **broker reporting the remove operation SUCCEEDED**. The batcher's
status poller records every SUCCEEDED operationID (`NodeBatcher.Succeeded`, retained
`succeededRetention` = 2 h), and the next `Delete()` call — at most 5 s later — returns
`NodeClaimNotFoundError`.

> **Node existence cannot be used instead.** "Return NotFound once no kube Node carries this
> providerID" is circular: during termination **Karpenter itself holds the Node object alive with
> its own finalizer**, and only drops that finalizer once `Delete()` reports the instance gone. The
> Node can never disappear *first*. Only an external signal — the broker's SUCCEEDED — terminates
> the loop. (`Get()` still exists and still falls back to listing kube Nodes, but it is **not** the
> termination completion signal.)

### In-flight suppression

Because `Delete()` is re-invoked every 5 s for the entire life of a removal, an unguarded
`EnqueueRemove` would push a **fresh remove batch on every reconcile**, flooding the broker's
**global** 64-slot queue with hundreds of copies of one removal — starving every other edge behind
it (the queue is process-wide, not per-edge).

The batcher therefore holds every ACKed-but-not-terminal operationID in an `inFlight` set.
Re-enqueueing one resolves the caller immediately without sending anything: the caller's contract
("queued at the broker") is already satisfied. An operationID leaves the set as soon as it goes
terminal (SUCCEEDED or FAILED) or its batch expires, so a genuine retry after a failure is never
suppressed.

Repeated `Delete()` calls are therefore cheap at every layer:

- **batcher, pre-ACK:** the same operationID joins the existing waiter list — `pending` maps an
  operationID to a **slice** of channels, so all waiters are resolved together (with one shared
  buffered channel, only the first waiter got the result and the rest blocked until their context
  was cancelled);
- **batcher, post-ACK:** the in-flight set short-circuits the send entirely;
- **broker:** a re-sent item finds its op record in RUNNING/SUCCEEDED and is skipped (idempotency,
  see below) — the broker still replies `BatchAccepted`.

**Batching.** The batchSender collects adds and removes from the same queue and the same
window (first item blocks, then 10 s / 10 items), then partitions by kind: adds go out via
`SendBatch`, removes via `SendBatchRemove` — each partition is its own broker batch with its own
UUID `batch_id`. A failed send fails only that partition's items. All broker RPCs carry the 30 s
`brokerCallTimeout`.

**Failure feedback.** The status poller reports terminal FAILED removes to the registered
`FailureHandler`, which for `kind != "add"` only logs — there is nothing to clean up
client-side. The operationID is cleared from the in-flight set, and since the NodeClaim still holds
its finalizer, Karpenter's next `Delete()` re-sends the removal (the broker rewrites the FAILED
record to ACCEPTED). This is exactly why FAILED records are **not** tombstoned — see below.

### Cancellation of pending adds

When a NodeClaim is deleted before its node ever joined (still on the synthetic pending
ProviderID, no matching node found), the old code called `RemoveNode("", uid)` with an empty
ProviderID — which was broken. Now `Delete()` fires `batcher.Cancel(uid)`:

- fire-and-forget goroutine, 10 s timeout (`cancelTimeout`), errors logged only;
- sends `cancel_ops` with the **add** operationID (the bare NodeClaim UID);
- the broker transitions only ops still in ACCEPTED to FAILED `"cancelled by client"` via a
  Redis compare-and-set; RUNNING and terminal ops are untouched (a RUNNING op already holds the
  PaaS update — cancelling mid-flight would desync the catalog);
- `CancelAck.cancelled_operation_ids` reports what was actually transitioned.

If the cancel loses the race (op already RUNNING), the node is provisioned anyway, joins the
cluster unclaimed, and is later removed by normal Karpenter consolidation/garbage collection.

---

## Broker Flow (edge-broker)

All in `pkg/context/karpenter_batch_stream.go` unless noted.

### handleBatchRemove

Mirrors `handleBatchAdd` exactly, with kind `"delete"`:

0. **`batchOpOwnedBy` check — done first, before the per-op idempotency loop.** If the `batch_id`
   is already owned by a different edge, reply `KarpenterBatchRejected{reason: "batch id belongs to
   another edge"}` and **write nothing** (no per-op records, no index). The stream stays alive. This
   is the batch-level edge-scoping guard; the per-op `edge_id` skip below is the record-level one.
1. Per-op idempotency check against `/edge/karpenter/nodeop/<opID>`:
   - RUNNING → skip (a live `processBatch` owns it);
   - SUCCEEDED → skip (already done) **and refresh the tombstone's 24-h TTL** (see TTL strategy);
   - ACCEPTED → re-enqueue without touching Redis (restart recovery; duplicates are harmless,
     see CAS below);
   - FAILED or absent → write `{kind: "delete", state: ACCEPTED}` (15-min TTL) and enqueue.
   - owned by a **different edge** → skip (see edge scoping below).
2. Write the `batchID → all opIDs` index (`/edge/karpenter/batchop/<batchID>`, 90-min TTL) so
   status polls report every node in the batch, including skipped ones. A `batchop` index owned by
   another edge is never overwritten.
3. Enqueue a `karpenterBatchQueueItem` carrying `removeItems` (a queue item is exactly one
   kind — `items` for adds or `removeItems` for removes, never both) into the same serialized
   queue as add batches (`karpenterBatchQueueSize = 64`, one batch processed at a time).
4. Reply `KarpenterBatchAccepted`.

**Queue full:** instead of killing the stream, the new/retry ops are marked FAILED
(`"batch queue full; try again"`) and the broker replies in-band with
`KarpenterBatchRejected{reason: "batch queue full"}`. The client surfaces this as
`ErrBatchRejected` to the `Delete()` caller, which retries later. The stream stays alive.
The same treatment applies to add batches.

The queue-full rejection uses the **compare-and-set** ACCEPTED→FAILED transition, **not a blind
patch**: an op that a processor has meanwhile claimed (ACCEPTED→RUNNING) is already doing catalog
work and must not be reported FAILED. The CAS makes the loser of that race a no-op.

**Edge scoping.** Every `nodeop` and `batchop` record carries the `edge_id` of the authenticated
stream that created it (the edge id comes from the mTLS client certificate). Status polls and
cancels only act on records belonging to the calling edge — a batch owned by another edge is
answered exactly like an unknown batch id (empty result list), so no cross-edge state leaks.
Records with an **empty** `edge_id` were written by an older broker and remain visible to everyone,
so operations in flight across a broker upgrade are unaffected (the Redis key schema is unchanged).

> ⚠️ Because the edge id is derived from the client certificate, `KarpenterBatchService`
> **fundamentally cannot be served over a plaintext listener** — there is no peer certificate to
> derive it from, and the stream is rejected with `ErrorInvalidPeer`. The
> `EDGE_BROKER_GRPC_INSECURE=true` guidance pointing at the broker's internal `:5449` port cannot
> work for batch operations. (It used to be worse: `edge-common`'s `GetEdgeClientInfo` did an
> unchecked `p.AuthInfo.(credentials.TLSInfo)` type assertion, and `AuthInfo` is `nil` on a
> plaintext connection — so opening `BatchStreamOperations` on `:5449` **panicked and crashed the
> whole broker process**, since gRPC does not recover handler panics. That was also an
> unauthenticated DoS. It now fails cleanly.)

**Unknown batch on status poll:** `handleBatchStatusPoll` for a missing/expired `batchop`
record replies with an **empty** `BatchStatus` instead of erroring the stream; the client
treats 3 consecutive empty responses as batch expiry (items → FAILED `"batch expired at
broker"` → failure handler).

### processBatchRemove

Mirrors `processBatchAdd` (50-min context, one batch at a time):

1. Re-read the `batchop` index and CAS each enqueued op ACCEPTED → RUNNING
   (`casKarpenterNodeOpState`, a go-redis WATCH/MULTI/EXEC optimistic transaction on the
   nodeop key). The CAS simultaneously:
   - **claims** the op — an op cancelled meanwhile, or claimed by an overlapping duplicate
     queue item, loses the CAS and is skipped (not processed, not patched);
   - **extends the TTL** from the 15-min ACCEPTED window to the 60-min RUNNING window.
2. Group the claimed items by `{pool, sku}` into **negative** `PoolSkuDelta`s.
3. `ApplyBatchKarpenterStreamNodeRemove` (`pkg/context/karpenter_node_lifecycle.go`): one
   Get → `bulkIncrementWorkerNodeCatalogValue` → Apply → Publish, then poll the compute
   instance every 30 s until SUCCESS or FAILED.
4. SUCCESS → mark every claimed op SUCCEEDED (**no provider IDs** — the platform picked the
   machines); FAILED → mark every claimed op FAILED with the error detail.

### Negative deltas, clamped at zero

`bulkIncrementWorkerNodeCatalogValue` (`pkg/context/karpenter_workspace.go`) applies positive
and negative deltas in a single pass over the catalog JSON:

- a decrement never drives a row's `noOfSku` below **0** — the clamp is visible in the
  returned prev/next counts;
- a negative delta for a pool+sku that has no catalog row is ignored rather than appending a
  negative row.

So an over-eager remove batch (e.g. Karpenter deleting NodeClaims for nodes the catalog no
longer counts) degrades to a no-op instead of corrupting the catalog.

### Which machine gets removed

The platform retires machines to converge actual count to the declared catalog count. There is
**no guarantee** the retired machine is the one whose NodeClaim Karpenter deleted. In practice
this is fine for pools of identical SKUs: Karpenter drains and deletes the kube Node object
*before* `CloudProvider.Delete()` is called, and capacity-wise removing "a node of that SKU"
is equivalent. The visible artifact: occasionally the PaaS retires machine B while Karpenter
deleted node A's object — node B's kube object then goes NotReady/disappears and Karpenter's
garbage collection reconciles it. The per-item `provider_id` is already on the wire for a
future broker-side targeted removal once the PaaS supports it.

---

## Redis Records and TTL Strategy

| Key | TTL | Notes |
|-----|-----|-------|
| `/edge/karpenter/nodeop/<opID>` | 15 min in ACCEPTED (`karpenterBatchAcceptedTTL`) | short on purpose — see below |
| same key after CAS to RUNNING, or on a terminal **FAILED** write | 60 min (`karpenterBatchRunningTTL`) | must exceed the 50-min processBatch timeout; a FAILED op must stay **retryable**. **Exception:** the queue-full-rejection FAILED write keeps the 15-min `karpenterBatchAcceptedTTL`, not 60 min — the 60-min FAILED TTL applies only to the `processBatch` terminal FAILED and `cancel_ops` paths |
| same key on a terminal **SUCCEEDED** write | **24 h** (`karpenterBatchSucceededTTL`), refreshed on every duplicate send | **tombstone** — see below |
| `/edge/karpenter/batchop/<batchID>` | 90 min (`karpenterBatchOpTTL`) | outlives the longest processing window + buffer |

The short ACCEPTED TTL bounds the broker-restart blind spot: if the broker pod dies after
ACKing a batch but before processing it, the queue item is lost and the client is *polling*,
not resending `batch_remove`. With a 15-min TTL the next status poll reports FAILED
(`"operation record expired"`), the failure handler runs (log-only for removes), and Karpenter's
ongoing `Delete()` retries re-enqueue the operation — recovery in minutes, not an hour.

### SUCCEEDED is a tombstone — replay protection

**This is the fix for a double-decrement bug that retired healthy machines.**

`Delete()` re-sends the same deterministic `<uid>-remove` operationID for as long as Karpenter keeps
retrying — which, on the 5 s termination loop, can run for a long time (a slow drain, a stuck PDB, a
provider restart). The terminal SUCCEEDED record is the **only** thing that makes those re-sends
no-ops, so it must outlive any plausible client retry horizon.

When SUCCEEDED expired with the 60-minute RUNNING TTL:

```
t=0      remove <uid>-remove → ACCEPTED → RUNNING → SUCCEEDED (catalog −1)   ✓ correct
t=60m    the SUCCEEDED record expires
t=60m+   Karpenter's Delete() retry re-sends <uid>-remove
         → broker sees NO record → treats it as a BRAND NEW operation → ACCEPTED
         → processBatch decrements the catalog AGAIN                          ✗ an extra,
                                                                                healthy machine
                                                                                is retired
```

…once per hour, per terminating NodeClaim. SUCCEEDED records are now long-lived **tombstones**
(24 h) whose TTL is **refreshed on each duplicate send**, so a persistently retrying client keeps
its own tombstone alive and the operation can never be applied twice.

**FAILED records deliberately keep the short TTL and are NOT tombstoned.** A FAILED op *must* stay
retryable (`handleBatchRemove` re-accepts it), and letting a FAILED record expire is equivalent to
re-accepting it. Tombstoning failures would strand a recoverable removal forever.

## Crash / Idempotency Analysis

| Failure | Outcome |
|---------|---------|
| Provider restarts after EnqueueRemove, before ACK | `resultCh` is lost with the process; Karpenter retries `Delete()` → same `<uid>-remove` operationID → broker-side idempotency dedups (record ACCEPTED/RUNNING → skip or re-enqueue, never double-processed). |
| Provider restarts after ACK | The in-memory `succeeded` and `inFlight` sets are lost. Karpenter retries `Delete()`, which re-sends the removal; the broker recognises the opID as already SUCCEEDED (24-h **tombstone**) and **skips it** — no second decrement — and the next status poll re-records it, so the following `Delete()` converges. Worst case: one redundant round trip, never a second node removal. |
| Provider restarts, and the SUCCEEDED tombstone has expired | Cannot happen within any realistic retry horizon (24 h, refreshed on each re-send). This is exactly the window that, at the old 60-min TTL, caused a **second catalog decrement** and retired an extra healthy machine. |
| Broker restarts after ACK, before processing | Queue item lost. ACCEPTED record expires in ≤15 min → status poll reports FAILED → client retry re-enqueues. |
| Broker restarts mid-processBatchRemove | RUNNING records expire in ≤60 min → `"operation record expired"` on poll. The catalog publish may or may not have landed; the next remove retry decrements again, and the zero-clamp prevents underflow if the platform already converged. |
| Duplicate queue items (overlapping batches with the same opID) | Only the first processor wins the ACCEPTED→RUNNING CAS; the second finds no ACCEPTED nodes and exits as a no-op. |
| Cancel races ACCEPTED→RUNNING | Both transitions go through the WATCH-based CAS on the same key; exactly one wins, the loser leaves the record untouched. |
| **Queue-full rejection races ACCEPTED→RUNNING** | Same CAS. The rejection can only move an op that is still ACCEPTED to FAILED, so it cannot clobber an op a processor concurrently claimed (which is already mutating the catalog). |
| Karpenter re-invokes `Delete()` every 5 s for the life of the removal | The in-flight set resolves each re-enqueue immediately without sending, so a single removal cannot flood the broker's global 64-slot queue. |
| Several callers await the same operationID | `pending` holds a **slice** of result channels per operationID; all waiters are resolved together, so none is left blocked on a channel another waiter already drained. |
| Client batch tracking outlives the broker records | Batches older than 2 h, or unknown at the broker for 3 consecutive polls, are dropped client-side with items treated as FAILED (`"batch expired at broker"`). |

## Key Files

| File | Repo | Purpose |
|------|------|---------|
| `pkg/edge/v1/edge_karpenter_batch.go` | edge-common | Remove/cancel/rejected message types + stream union fields 3/4 |
| `pkg/common/grpc.go` | edge-common | `GetEdgeClientInfo` — edge id from the mTLS client cert; returns `ErrorInvalidPeer` (no longer panics) on a plaintext peer |
| `pkg/cloudprovider/cloudprovider.go` | provider | `Delete()`: `Succeeded()` convergence check, pending-claim cancel, `"-remove"` opID, EnqueueRemove |
| `pkg/rafay/batcher.go` | provider | `EnqueueRemove`, `Succeeded`, in-flight suppression, multi-waiter `pending`, kind partitioning, `Cancel`, failure feedback |
| `pkg/rafay/batch_stream.go` | provider | `sendBatchRemove`, `cancelOps` stream helpers |
| `pkg/context/karpenter_batch_stream.go` | edge-broker | `handleBatchRemove`, `handleCancelOps`, `processBatchRemove`, TTL strategy |
| `pkg/context/karpenter_node_lifecycle.go` | edge-broker | `ApplyBatchKarpenterStreamNodeRemove` |
| `pkg/context/karpenter_workspace.go` | edge-broker | `bulkIncrementWorkerNodeCatalogValue` zero-clamp |
| `pkg/context/karpenter_node_stream.go` | edge-broker | nodeop Redis records, `casKarpenterNodeOpState` (WATCH/MULTI/EXEC) |
