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
`Worker Node SKU` catalog on the workspace compute instance and republishes it (or lowers
`scaling.desired` on a first-class MKS `Cluster`), and the platform retires machines until the
actual count matches the declared count. Like adds, removals are slow (10–20 minutes) and
serialized per edge (one batch at a time per cluster, clusters in parallel), so individual
`Delete()` calls are collected into batches.

Key asymmetry with the add path: **the catalog is declarative — the platform chooses which
physical machine is retired.** The provider sends the node's ProviderID with each remove item,
but the current PaaS API cannot target a specific machine; only the per-`{pool, sku}` count goes
down. This is a platform limitation. The ProviderID is carried on the wire anyway so targeted
removal can be implemented broker-side later without a protocol change — and the provider's
`Delete()` compensates for the choice today (no remove for a machine that is already gone; a
SUCCEEDED remove counts as done only once the NodeClaim's own machine has stopped; a remove after
which the platform retired another machine is held, never re-sent). `docs/architecture.md` §6.2
carries the current decision tree; this document keeps the design rationale.

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
                                            (only after the Node is drained and volumes detached)
  0. operationID = <NodeClaim UID> + "-remove"
     if batcher.SucceededResult(operationID) is set:
        detail "node not retired: …"          → NodeClaimNotFoundError + event NodeNotRetired ✓
        provider IDs named, ours not among them → hold (event RemoveRetiredOtherMachine,
                                                 annotation remove-retired-other), never re-sent
        untargeted: Node gone or NotReady     → NodeClaimNotFoundError ✓ CONVERGENCE: releases
                                                 the Node's termination finalizer
                    Node still Ready          → nil, up to RAFAY_REMOVE_SETTLE_WINDOW (60m),
                                                 then the retired-other hold
        pending providerID                    → NodeClaimNotFoundError ✓
  1. providerID = nodeClaim.Status.ProviderID
     if pending ("rafay://pending/<uid>"):  resolvePendingRemoval — only the add's own state:
        remove already ACKed here → re-send as is
        add SUCCEEDED   → findNodeProviderID() (uncached apiReader, under NodeOwnershipMu;
                          Nodes by nodepoolname label, sku_name matched in code, created after
                          the NodeClaim, not owned by another NodeClaim incl. the adoption
                          annotation) → reserve the ID on the NodeClaim, send the remove
                          (untargeted / empty providerID if nothing registered)
        add FAILED      → NodeClaimNotFoundError (no RPC)
        otherwise       → batcher.Cancel(uid) SYNCHRONOUSLY: applied → NodeClaimNotFoundError;
                          not applied → nil until the poller reports the add's outcome
     else (first remove in this process): look up the Node carrying providerID —
        none, or Terminating with a stopped kubelet deleted no later than the NodeClaim and
        not chosen by the disruption queue → NodeClaimNotFoundError, send NOTHING
        (event NodeRetiredExternally in the second case)
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
(Karpenter core `pkg/controllers/node/termination/controller.go`) calls `cloudProvider.Delete()` on
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
status poller records every SUCCEEDED operationID (`NodeBatcher.Succeeded` / `SucceededResult`,
retained `succeededRetention` = 2 h), and the next `Delete()` call — at most 5 s later — acts on
it: because the removal is untargeted, it returns `NodeClaimNotFoundError` once the NodeClaim's
own Node is gone or NotReady (a kubelet stops heartbeating ~40 s after its machine halts), and
keeps the NodeClaim Terminating while the Node is still Ready, for up to `RAFAY_REMOVE_SETTLE_WINDOW`.

> **Node existence cannot be used instead — readiness can.** "Return NotFound once no kube Node
> carries this providerID" is circular: during termination **Karpenter itself holds the Node object
> alive with its own finalizer**, and only drops that finalizer once `Delete()` reports the instance
> gone. The Node can never disappear *first*. Only an external signal — the broker's SUCCEEDED —
> terminates the loop. The finalizer keeps the Node **object**, though, not the kubelet's heartbeat,
> so `Ready` is a valid answer to "is *this* machine gone" — the question SUCCEEDED alone cannot
> answer on an untargeted removal. (`Get()` still exists and still finds the kube Node, but it is
> **not** the termination completion signal, and it must not guess from readiness either: a
> transiently NotReady machine is alive.)

### In-flight suppression

Because `Delete()` is re-invoked every 5 s for the entire life of a removal, an unguarded
`EnqueueRemove` would push a **fresh remove batch on every reconcile**, flooding this edge's
16-slot broker queue with hundreds of copies of one removal. (It used to be worse: the queue was
one global 64-slot channel shared by every edge, so one runaway removal starved every other
cluster behind it. The queue is per edge now; head-of-line blocking exists only within an edge.)

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
`FailureHandler`, which for `kind != "add"` only logs a transient failure — there is nothing to
clean up client-side. The operationID is cleared from the in-flight set, and since the NodeClaim
still holds its finalizer, Karpenter's next `Delete()` re-sends the removal (the broker rewrites
the FAILED record to ACCEPTED). This is exactly why FAILED records are **not** tombstoned — see
below. A remove an old broker FAILED for a permanent reason (`is at its minimum`, `pool at
minimum`, …) is recorded in `PoolBackoff` instead, and the next `Delete()` converges with a
`NodeNotRetired` event rather than re-sending every 5 s; a current broker reports that case as
SUCCEEDED `node not retired: …` (see processBatchRemove).

### Cancellation of pending adds

When a NodeClaim is deleted before its node ever joined (still on the synthetic pending
ProviderID), the old code called `RemoveNode("", uid)` with an empty ProviderID — which was
broken. Now `Delete()` consults the add's own state (`resolvePendingRemoval`): an add the
failure handler recorded as FAILED needs nothing; an add that SUCCEEDED gets a remove
(untargeted if no Node registered yet — a cancel cannot undo a finished add, and NotFound would
finalize the NodeClaim while its machine lands with no owner and the catalog keeps counting
it); otherwise `Delete()` calls `batcher.Cancel(ctx, uid)`:

- **synchronous**, 10 s timeout (`cancelTimeout`); the answer decides what `Delete()` returns;
- sends `cancel_ops` with the **add** operationID (the bare NodeClaim UID);
- the broker transitions only ops still in ACCEPTED to FAILED `"cancelled by client"` via a
  Redis compare-and-set; RUNNING and terminal ops are untouched (a RUNNING op already holds the
  PaaS update — cancelling mid-flight would desync the catalog);
- `CancelAck.cancelled_operation_ids` reports what was actually transitioned: cancelled →
  `NodeClaimNotFoundError`; not cancelled → `nil`, the NodeClaim stays Terminating until the
  poller reports the add's terminal state (memoised, one RPC per NodeClaim); RPC error → returned,
  Karpenter retries in 5 s.

If the cancel loses the race (op already RUNNING), the node is provisioned anyway — and because
the NodeClaim is kept, it gets an owner and, once the add reports SUCCEEDED, a proper remove. A
node that joined with **no** NodeClaim waiting for it (a lost fire-and-forget cancel from an older
provider) is *not* cleaned up by consolidation or garbage collection: `List()` visibility only
protects registered NodeClaims from GC, and every disruption method refuses a Node without a
NodeClaim. Only the node-adoption controller (default on) gives it an owner — once Ready, and then
`WhenEmpty` reclaims it only when empty; with adoption off it must be removed from the catalog by
hand.

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
   - FAILED or absent → write `{kind: "delete", state: ACCEPTED}` (15-min TTL, stamped with
     `edge_id` / `project_id` / `cluster_id`) and enqueue.
   - UNKNOWN (0) → skip and log; never re-accepted, never re-created.
   - owned by a **different edge** → skip (see edge scoping below).
2. Write the `batchID → all opIDs` index (`/edge/karpenter/batchop/<batchID>`, 3-h TTL) so
   status polls report every node in the batch, including skipped ones. A `batchop` index owned by
   another edge is never overwritten.
3. Enqueue a `karpenterBatchQueueItem` carrying `removeItems` (a queue item is exactly one
   kind — `items` for adds or `removeItems` for removes, never both) onto **this edge's** FIFO
   in `KarpenterBatchQueue` (16 waiting batches per edge, one batch processed at a time per
   edge, edges in parallel), the same queue its add batches use.
4. Reply `KarpenterBatchAccepted`.

**Queue full / too large / foreign batch id:** decided **before** any write. The broker replies
in-band with `KarpenterBatchRejected{reason: "batch queue full"}` (this edge already has 16
waiting batches), `"batch too large: <n> nodes, max 64"`, or `"batch id belongs to another edge"`,
and writes no op record and no index. The client surfaces this as `ErrBatchRejected` to the
`Delete()` caller, which retries later. The stream stays alive. The same treatment applies to add
batches.

In the residual race where the queue fills between the check and the enqueue, only the ops this
call itself wrote ACCEPTED are marked FAILED (`"batch queue full; try again"`, 15-min TTL) with the
**compare-and-set** ACCEPTED→FAILED transition, **not a blind patch**: an op that a processor has
meanwhile claimed (ACCEPTED→RUNNING) is already doing catalog work and must not be reported FAILED,
and an op that was already ACCEPTED from an earlier call keeps its state and its earlier queue
item. The CAS makes the loser of that race a no-op.

**Edge scoping.** Every `nodeop` and `batchop` record carries the `edge_id` of the authenticated
stream that created it (the edge id comes from the mTLS client certificate). Status polls and
cancels only act on records belonging to the calling edge — a batch owned by another edge is
answered exactly like an unknown batch id (empty result list), so no cross-edge state leaks.
Records with an **empty** `edge_id` were written by an older broker and remain visible to everyone,
so operations in flight across a broker upgrade are unaffected (the Redis key schema is unchanged).

> ⚠️ Because the edge id is derived from the client certificate, `KarpenterBatchService`
> **fundamentally cannot be served over a plaintext listener** — there is no peer certificate to
> derive it from, and the stream is rejected with `ErrorNoClientID` (`"NO CLIENT ID"`; the broker
> maps every `GetEdgeClientInfo` failure to it and never surfaces edge-common's internal
> `ErrorInvalidPeer`). The `EDGE_BROKER_GRPC_INSECURE=true` guidance pointing at the broker's
> internal `:5449` port cannot work for batch operations. (It used to be worse: `edge-common`'s
> `GetEdgeClientInfo` did an unchecked `p.AuthInfo.(credentials.TLSInfo)` type assertion, and
> `AuthInfo` is `nil` on a plaintext connection — so opening `BatchStreamOperations` on `:5449`
> **panicked and crashed the whole broker process**, since gRPC did not recover handler panics.
> That was also an unauthenticated DoS. It now fails cleanly, and both servers carry
> panic-recovery interceptors.)

**Unknown batch on status poll:** `handleBatchStatusPoll` for a missing/expired `batchop`
record replies with an **empty** `BatchStatus` instead of erroring the stream; the client
treats 3 consecutive empty responses as batch expiry (items → FAILED `"batch expired at
broker"` → failure handler).

### processBatchRemove

Mirrors `processBatchAdd` (**60-minute** `karpenterBatchRemoveTimeout` — a retire takes 10–20
minutes and the deadline must never go below an hour; one batch at a time per edge, under the
per-cluster Redis lock):

1. Re-read the `batchop` index and CAS each enqueued op ACCEPTED → RUNNING
   (`casKarpenterNodeOpState`, a go-redis WATCH/MULTI/EXEC optimistic transaction on the
   nodeop key). The CAS simultaneously:
   - **claims** the op — an op cancelled meanwhile, or claimed by an overlapping duplicate
     queue item, loses the CAS and is skipped (not processed, not patched);
   - **extends the TTL** from the 15-min ACCEPTED window to the 2-h RUNNING window.
2. Group the claimed items by `{pool, sku}` into **negative** `PoolSkuDelta`s.
3. `KarpenterBackend.RemoveNodes` (`karpenter_backend_oneclick.go` for the compute-instance
   catalog, `karpenter_backend_firstclass.go` for an MKS `Cluster`): one Get →
   `applyWorkerNodeCatalogDeltas` → Apply → Publish, calling `onPublished()` at the commit point,
   then poll the platform every 30 s until it settles.
4. **Every claimed op is tombstoned SUCCEEDED inside `onPublished`** — the moment the decrement
   is published, before the settle poll (**no provider IDs** — the platform picks the machines).
   A settle failure afterwards keeps SUCCEEDED with detail
   `catalog decremented; compute instance did not settle`; a write that failed past the commit
   point is SUCCEEDED `committed; …`. FAILED is written only for a failure **before** the commit
   point (retryable). A remove the platform cannot apply at all (row at `minNodeCount`, at 0,
   no row, other SKU, not opted in; first-class: pool at `scaling.min`, unknown pool, SKU
   mismatch, manual pool) is converged as SUCCEEDED with detail `node not retired: <reason>`
   (e.g. `node not retired: pool at minimum (1): pool "p" has 1 nodes; 1 requested, 1 refused`),
   nothing written; in a partially refused batch each refused op carries its own detail.

### Negative deltas, refused below the minimum

`applyWorkerNodeCatalogDeltas` (`pkg/context/karpenter_workspace.go`) applies positive
and negative deltas in a single pass over the catalog JSON:

- a decrement never drives a row's `noOfSku` below its `minNodeCount` (or **0**) — the refused
  part is reported as `node not retired` rather than applied;
- a negative delta for a pool+sku that has no catalog row, has another SKU or is not opted in is
  refused rather than appending a negative row;
- on a single-row catalog the live worker count from `status.output` is the base when it differs
  from `noOfSku`.

So an over-eager remove batch (e.g. Karpenter deleting NodeClaims for nodes the catalog no
longer counts) degrades to a `node not retired` convergence instead of corrupting the catalog.

### Which machine gets removed

The platform retires machines to converge actual count to the declared catalog count. There is
**no guarantee** the retired machine is the one whose NodeClaim Karpenter deleted (on the
oneclick backend it is deterministically the newest one). Capacity-wise removing "a node of that
SKU" is equivalent for pools of identical SKUs, but the visible artifact matters: Karpenter has
drained node A, and the PaaS retires machine B — node B's kube object goes NotReady and its
NodeClaim is deleted by the node termination controller, while node A's machine is still
running. The provider handles both halves: `Delete()` for A keeps its NodeClaim Terminating
while A's Node is still Ready (up to `RAFAY_REMOVE_SETTLE_WINDOW`, then the
`RemoveRetiredOtherMachine` hold — the drained Node is never dropped while its kubelet is alive,
since a v1.31 kubelet never re-registers on its own), and `Delete()` for B finds B's Node
Terminating with a stopped kubelet, deleted outside Karpenter, and reports it gone **without**
sending a remove (`NodeRetiredExternally`) — one remove that retired one more healthy machine
per cycle was the GC feedback cascade. The per-item `provider_id` is already on the wire for a
future broker-side targeted removal once the PaaS supports it.

---

## Redis Records and TTL Strategy

| Key | TTL | Notes |
|-----|-----|-------|
| `/edge/karpenter/nodeop/<opID>` | 15 min in ACCEPTED (`karpenterBatchAcceptedTTL`) | short on purpose — see below |
| same key after CAS to RUNNING, or on a terminal **FAILED** write | 2 h (`karpenterBatchRunningTTL` = add timeout + 30 min) | exceeds the 90-min add / 60-min remove processing timeouts; a FAILED op must stay **retryable**. **Exception:** the `batch queue full; try again`, `broker shutting down; retry` and `cluster locked by another broker; retry` writes keep the 15-min `karpenterBatchAcceptedTTL` — the 2-h FAILED TTL applies to the `processBatch` terminal FAILED and `cancel_ops` paths |
| same key on a terminal **SUCCEEDED** write | **24 h** (`karpenterBatchSucceededTTL`), refreshed on every duplicate send | **tombstone** — see below; written at publish for removes |
| `/edge/karpenter/batchop/<batchID>` | 3 h (`karpenterBatchOpTTL`) | outlives the longest processing window + buffer |
| `/edge/karpenter/lock/<edgeID>` | 2 h (`karpenterClusterLockTTL`) | one batch per cluster across broker replicas |

The short ACCEPTED TTL bounds the queued-behind-a-long-add blind spot: an op that sits in the
edge's queue behind the same cluster's earlier 60–90-minute adds for more than 15 minutes is
reported FAILED (`"operation record expired"`) on the next status poll, the failure handler runs
(log-only for removes), and Karpenter's ongoing `Delete()` retries re-enqueue the operation. A
broker **restart** no longer depends on it: the next broker's `sweepOrphanedKarpenterOps` re-queues
every still-ACCEPTED op onto its edge's queue within seconds (RUNNING ops are not swept — the
catalog write may already have landed — and expire on their own TTL).

### SUCCEEDED is a tombstone — replay protection

**This is the fix for a double-decrement bug that retired healthy machines.**

`Delete()` re-sends the same deterministic `<uid>-remove` operationID for as long as Karpenter keeps
retrying — which, on the 5 s termination loop, can run for a long time (a provider restart, a slow
platform retire — up to the 60-minute settle window; note that no remove op exists at all while a
drain or a PDB is holding the node, since the termination controller only calls `Delete()` after
the drain completes). The terminal SUCCEEDED record is the **only** thing that makes those re-sends
no-ops, so it must outlive any plausible client retry horizon.

When SUCCEEDED expired with the then 60-minute RUNNING TTL:

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

**FAILED records are NOT tombstoned.** A FAILED op *must* stay retryable (`handleBatchRemove`
re-accepts it), and letting a FAILED record expire is equivalent to re-accepting it. Tombstoning
failures would strand a recoverable removal forever. Nothing past the commit point produces a
FAILED, so every FAILED remove is genuinely retryable.

## Crash / Idempotency Analysis

| Failure | Outcome |
|---------|---------|
| Provider restarts after EnqueueRemove, before ACK | `resultCh` is lost with the process; Karpenter retries `Delete()` → same `<uid>-remove` operationID → broker-side idempotency dedups (record ACCEPTED/RUNNING → skip or re-enqueue, never double-processed). |
| Provider restarts after ACK | The in-memory `succeeded` and `inFlight` sets are lost. Karpenter retries `Delete()`; the Node still carries the ProviderID, so it re-sends the removal; the broker recognises the opID as already SUCCEEDED (24-h **tombstone**) and **skips it** — no second decrement — and the next status poll re-records it, so the following `Delete()` acts on it. The settle window restarts; a retired-other verdict is read back from the `karpenter.rafay.io/remove-retired-other` annotation. Worst case: one redundant round trip, never a second node removal. |
| Provider restarts, and the SUCCEEDED tombstone has expired | Cannot happen within any realistic retry horizon (24 h, refreshed on each re-send). This is exactly the window that, at the old 60-min TTL, caused a **second catalog decrement** and retired an extra healthy machine. A NodeClaim held for a retired-other verdict never re-sends at all — the annotation persists it — so even a 24-hour-plus hold cannot resurrect the op. |
| Broker restarts after ACK, before processing | Queue item lost, but the next broker's startup sweep re-queues the still-ACCEPTED op within seconds. Fallback: the ACCEPTED record expires in ≤15 min → status poll reports FAILED → client retry re-enqueues. |
| Broker restarts mid-processBatchRemove | If the decrement was already published the ops are SUCCEEDED (written in `onPublished`, before the settle poll) — the retry is skipped by the tombstone. Only a crash inside the read-modify-write leaves a RUNNING record, which expires in ≤2 h → `"operation record expired"` on poll → the next remove retry re-applies exactly once (a decrement below the pool minimum is refused as `node not retired`). |
| Duplicate queue items (overlapping batches with the same opID) | Only the first processor wins the ACCEPTED→RUNNING CAS; the second finds no ACCEPTED nodes and exits as a no-op. Across broker replicas the per-cluster lock additionally serializes whole batches. |
| Cancel races ACCEPTED→RUNNING | Both transitions go through the WATCH-based CAS on the same key; exactly one wins, the loser leaves the record untouched. |
| **Queue-full rejection races ACCEPTED→RUNNING** | Same CAS. The rejection can only move an op that is still ACCEPTED to FAILED, so it cannot clobber an op a processor concurrently claimed (which is already mutating the catalog). |
| Karpenter re-invokes `Delete()` every 5 s for the life of the removal | The in-flight set resolves each re-enqueue immediately without sending, so a single removal cannot flood the edge's 16-slot broker queue. |
| Several callers await the same operationID | `pending` holds a **slice** of result channels per operationID; all waiters are resolved together, so none is left blocked on a channel another waiter already drained. Enqueue and ACK are atomic per operationID, so a concurrent caller during the ACK never queues a duplicate batch. |
| Client batch tracking outlives the broker records | Batches older than 3 h, or unknown at the broker for 3 consecutive polls, are dropped client-side with items treated as FAILED (`"batch expired at broker"`). |
| The platform retires a different machine than the drained one | The drained NodeClaim is held Terminating while its Node is Ready (settle window, then `RemoveRetiredOtherMachine`); the retired machine's NodeClaim, deleted by the node termination controller, reports gone without a remove (`NodeRetiredExternally`). No second machine is retired. |

## Key Files

| File | Repo | Purpose |
|------|------|---------|
| `pkg/edge/v1/edge_karpenter_batch.go` | edge-common | Remove/cancel/rejected message types + stream union fields 3/4 |
| `pkg/common/grpc.go` | edge-common | `GetEdgeClientInfo` — edge id from the mTLS client cert; returns `ErrorInvalidPeer` internally (no longer panics) on a plaintext peer, which the broker reports as `ErrorNoClientID` |
| `pkg/cloudprovider/cloudprovider.go` | provider | `Delete()`: `SucceededResult` convergence (settle window, retired-other hold, not-retired), already-gone / `retiredExternally` guard, `resolvePendingRemoval` (add-state-driven, synchronous cancel), `"-remove"` opID, EnqueueRemove, NodePool events |
| `pkg/cloudprovider/poolbackoff.go` | provider | per-op FAILED records the failure handler leaves for `Delete()`; `IsNotRetiredDetail` / `IsPermanentRefusalDetail` |
| `pkg/rafay/batcher.go` | provider | `EnqueueRemove`, `Succeeded` / `SucceededResult`, in-flight suppression, multi-waiter `pending`, kind partitioning, synchronous `Cancel`, failure feedback |
| `pkg/rafay/batch_stream.go` | provider | `sendBatchRemove`, `cancelOps` stream helpers |
| `pkg/context/karpenter_batch_stream.go` | edge-broker | `handleBatchRemove`, `handleCancelOps`, `processBatchRemove` (tombstone at publish, committed / not-retired outcomes), per-edge `KarpenterBatchQueue`, cluster lock, TTL strategy |
| `pkg/context/karpenter_batch_recovery.go` | edge-broker | `sweepOrphanedKarpenterOps` — re-queues ACCEPTED ops after a broker restart |
| `pkg/context/karpenter_backend.go` | edge-broker | `KarpenterBackend` interface, backend selection, refusal-reason constants, `karpenterNotRetiredError`, `karpenterCommittedError` |
| `pkg/context/karpenter_backend_oneclick.go` | edge-broker | `RemoveNodes` for the compute-instance catalog (commit point, busy wait, ambiguous-error re-read) |
| `pkg/context/karpenter_backend_firstclass.go` | edge-broker | `RemoveNodes` for MKS `Cluster` `scaling.desired` (live-count base, per-pool refusals) |
| `pkg/context/karpenter_workspace.go` | edge-broker | `applyWorkerNodeCatalogDeltas` — per-row trimming / refusals, live-count base |
| `pkg/context/karpenter_node_stream.go` | edge-broker | nodeop Redis records, `casKarpenterNodeOpState` (WATCH/MULTI/EXEC), cluster lock key |
