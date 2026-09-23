# Crash Safety and Idempotent Design

This document explains how the Rafay Karpenter provider handles pod restarts, in-flight requests, and partial failures without leaking nodes or getting stuck.

---

## Overview

The provider is designed so that **NodeClaims in etcd are the only source of truth**. No in-memory state is load-bearing across a restart. Every stage of the node provisioning lifecycle has a defined recovery path.

---

## Node Provisioning Lifecycle

Each step below is numbered so the failure analysis section can reference it directly.

```
[1] Create() called by Karpenter lifecycle controller
     │
[2] Enqueue() → item placed in in-memory queue channel (buffer=256)
     │              NOT durable — lost on pod restart
     │              queue full → immediate error, pending entry rolled back
     │
[3] batchSender collects items (blocks for the first item, then up to 10 items / 10s window)
     │
[4] SendBatch gRPC call to edge-broker (30s deadline, no retry)
     │              broker writes ACCEPTED op records + batchID index to Redis,
     │              enqueues the batch, THEN replies KarpenterBatchAccepted
     │
[5] Broker ACK → resultCh resolved, batch registered in inProgress map
     │         ← DURABILITY BOUNDARY
     │           (broker side: Redis records; client side: nothing more needed)
[6] Create() returns with ProviderID = "rafay://pending/<nodeclaim-uid>"
     │
[7] Karpenter sets Launched=True + synthetic ProviderID on NodeClaim in etcd
     │
[8] statusPoller polls broker every 30s (after 2-min per-batch delay)
     │    SUCCEEDED → recorded in the batcher's `succeeded` set (for an add, registration is
     │                driven by the node actually joining, not by this; for a REMOVE this is
     │                what lets Delete() converge — see the Delete Path section)
     │    FAILED    → FailureHandler deletes the pending NodeClaim → fast reprovision
     │
[9] NodeProviderIDController reconciles NodeClaim (node watch + 30s requeue)
     │
[10] Real node joins cluster (~12 min), node gets spec.providerID from Rafay platform
     │
[11] Controller patches NodeClaim.Status.ProviderID = node.Spec.ProviderID
     │
[12] Karpenter Registration reconciler finds matching node → Registered=True
```

The broker ACK at step [5] is the durability boundary. **Before it**: all state is transient on the client and recoverable by Karpenter re-calling `Create()`. **After it**: the request is recorded in the broker's Redis (per-op ACCEPTED records plus the batchID→opIDs index, written *before* the ACK is sent), and the NodeClaim's `Launched=True` + synthetic ProviderID land in etcd. Both sides can lose their process and recover.

`Create()` unblocks at the ACK — not at SUCCEEDED. On the **add** path the status poller (step [8]) exists purely for asynchronous failure feedback: a broker-reported FAILED invokes the registered `FailureHandler`, which deletes the NodeClaim (UID == operationID) if it is still pending, so Karpenter reprovisions within seconds rather than waiting out the 60-minute registration timeout. On the **remove** path the poller carries more weight — its SUCCEEDED observation is what lets `Delete()` report the instance gone and release the Node's termination finalizer (see [Delete Path](#delete-path--crash-and-race-analysis)).

---

## Failure Analysis at Each Step

### [1] Create() called — Karpenter reconcile context cancelled before Enqueue()

**Cause:** Karpenter's controller shuts down or the reconcile is preempted before `Enqueue()` is reached.

**State:** NodeClaim has `Launched=Unknown` in etcd. Nothing was sent to the broker.

**Recovery:** On the next reconcile cycle, Karpenter calls `Create()` again. Fresh request, no prior state.

---

### [2] Item placed in queue — pod restarts before batchSender picks it up

**Cause:** Pod killed while the item is sitting in the in-memory queue channel.

**State:** NodeClaim has `Launched=Unknown`. Broker has no knowledge of this request.

**Recovery:**
1. Queue is lost (in-memory).
2. Karpenter sees `Launched=Unknown` → calls `Create()` again.
3. Fresh `Enqueue()` → batch sent to broker for the first time.

No idempotency concern — broker never saw this OperationID.

A full queue (256 items) is handled without a crash: `enqueueItem` delivers an immediate error on the result channel and rolls back the pending-map entry, so the retry starts fresh.

The pending map holds a **slice** of result channels per operationID, so several callers waiting on the same operation (a retry racing the original, or Karpenter's 5s `Delete()` loop) are **all** resolved when the result arrives. With a single buffered channel per operation only the first waiter could consume the result; the others blocked until their context was cancelled.

---

### [3] batchSender collecting items — context cancelled mid-window

**Cause:** Pod shutdown signals `ctx.Done()` while `collectBatch()` is waiting for more items.

**State:** `collectBatch()` returns whatever it has collected so far (possibly a partial batch).

**Recovery:** If items were collected, `sendBatch()` is called with the partial batch before the goroutine exits. The send either reaches the broker or fails; each item's `resultCh` receives the ACK or an error accordingly, `Create()` returns, Karpenter retries on error. The broker either received the partial batch or did not:
- If received: broker deduplicates on retry.
- If not received: fresh first request on retry.

---

### [4] SendBatch gRPC call fails — network error, broker unavailable, or batch rejected

**Cause:** gRPC connection failure, the 30s `brokerCallTimeout` expiring, broker unreachable, or the broker replying `KarpenterBatchRejected` (its processing queue of 64 batches is full).

**Code path (`batcher.go`):**
```go
batchID, err := b.client.SendBatch(ctx, nodes)
if err != nil {
    b.failBatchSend(opKindAdd, batch, err)   // resolvePending: delivers the error to every waiter and clears the pending entry
    return
}
```

**State:** All items in the batch receive an error result (a rejection arrives as `ErrBatchRejected` wrapping the broker's reason). NodeClaims remain at `Launched=Unknown`. A rejection keeps the gRPC stream and connection healthy — it is an in-band answer, not a transport failure.

**Recovery:**
1. Each `Create()` call receives the error → returns `cloudprovider.NewCreateError(...)` to Karpenter.
2. Karpenter sets `ConditionTypeLaunched=Unknown` with reason `AddNodesFailed`.
3. Karpenter retries with exponential backoff (up to 5-min `launchTimeout`).
4. On retry, `Create()` enqueues a new request → next batch attempt.

If the gRPC call was partially delivered (broker wrote its Redis records but the response was lost), the broker still has the request. On retry, broker deduplicates by OperationID: ops still ACCEPTED are re-enqueued without rewriting Redis, RUNNING/SUCCEEDED ops are skipped. The queue-full case also marks the affected ops FAILED (`"batch queue full; try again"`) in Redis, which the broker treats as a retryable state — a re-send rewrites them to ACCEPTED.

---

### [5] Broker ACK delivered — pod restarts before the status poller ever runs

**Cause:** Pod killed after `registerAndAck` resolved the result channels but before (or between) status polls.

**State:** The `inProgress` map is in-memory and lost. `Create()` already returned, so depending on timing the NodeClaim either has `Launched=True` in etcd or Karpenter re-calls `Create()`. The broker **has** the request and is provisioning.

**Recovery:**
1. If `Launched=True` was persisted: nothing to do on the add path — `NodeProviderIDController` resumes from etcd (step [9]).
2. If not: Karpenter re-calls `Create()` with the same `OperationID = string(nodeClaim.UID)`; the **broker deduplicates by OperationID** — no second node provisioned — and a fresh ACK unblocks the call.
3. **Lost failure feedback:** the new process no longer knows the old batchID, so a broker-side FAILED for that batch will not reach the failure handler. The backstop is Karpenter's 60-minute `registrationTimeout`, which deletes the NodeClaim if no node ever registers. Slow, but safe.

---

### [6]–[7] Create() returns / Launched=True persisted

#### Create() context cancelled in the same instant the ACK arrives

**Cause:** Rare race where `ctx.Done()` and `resultCh` fire simultaneously and `ctx.Done()` wins the select.

**State:** The ACK is discarded. NodeClaim stays at `Launched=Unknown`. The pending-map entry was already removed when the ACK was delivered.

**Recovery:** Karpenter retries `Create()`. The batcher enqueues a fresh item with the same OperationID; the broker deduplicates (ACCEPTED → re-enqueue, RUNNING/SUCCEEDED → skip) and ACKs again. `Create()` returns the synthetic ProviderID normally.

#### Karpenter fails to write Launched=True after Create() returns

**Cause:** API server unavailable or optimistic locking conflict when patching NodeClaim status.

**Recovery:**
1. Karpenter lifecycle controller retries the patch on next reconcile.
2. If it re-calls `Create()` before the patch succeeds: batcher/broker dedup, `Create()` returns the synthetic ProviderID again, patch is retried.

Karpenter uses an in-memory cache (`l.cache.SetDefault`) to avoid calling `Create()` twice for the same NodeClaim within one process lifetime:
```go
// launch.go
if ret, ok := l.cache.Get(string(nodeClaim.UID)); ok {
    created = ret.(*v1.NodeClaim)   // uses cached result, skips Create()
}
```
On pod restart the cache is lost, but broker deduplication covers that case.

---

### [8] Status poll outcomes

#### Poll fails — broker temporarily unreachable

**Code path (`batcher.go`):**
```go
results, err := b.client.PollBatchStatus(ctx, batchID)
if err != nil {
    klog.Warningf("batcher: PollBatchStatus batchID=%s failed: %v", batchID, err)
    return  // retried on next ticker tick
}
```

`PollBatchStatus` itself retries once on `codes.Unavailable` (polls are read-only and safe to re-issue); beyond that the failure is logged and the next 30-second tick retries. Nothing blocks on the poll — `Create()` already returned at ACK.

#### Broker returns FAILED for a node in the batch

**Cause:** Broker was unable to provision the node (catalog/PaaS failure), the op was cancelled (`"cancelled by client"`), or its ACCEPTED record expired (`"operation record expired"` — e.g. broker restarted before processing; ACCEPTED records carry a 15-minute TTL precisely so this surfaces quickly).

**Code path (`batcher.go` → `cloudprovider.NewBatchFailureHandler`):**
```go
case v1.KARPENTER_NODE_OPERATION_STATE_FAILED:
    failed = append(failed, failedOp{operationID: item.operationID, detail: nr.GetDetail()})
...
fn(ctx, f.operationID, kind, f.detail)   // failure handler, outside the lock
```

**Recovery (add):** the failure handler looks up the NodeClaim whose UID equals the operationID and **deletes it** — but only if it still carries a `rafay://pending/` ProviderID and no deletion timestamp. Karpenter observes the deletion and provisions a replacement NodeClaim (fresh UID → fresh OperationID) within seconds. NodeClaims that already resolved a real node, or that are already being deleted, are left alone. One detail is special: `pool at maximum` (the broker refused the node because the pool is at its platform maximum). A replacement would be refused too, so the handler first holds the NodePool back in `PoolBackoff` (`RAFAY_POOL_AT_MAX_COOLDOWN`, default 5 min) — `GetInstanceTypes` withholds its offerings and the pods stay pending — and then deletes the NodeClaim as usual. The hold is in memory only; a restart drops it, which costs at most one more refused round trip.

**Recovery (remove):** log-only in the failure handler — but the operationID is cleared from the batcher's in-flight set, which is what makes recovery work. Karpenter keeps calling `Delete()` every 5s while the NodeClaim is terminating, so the next call re-sends the removal (the broker rewrites the FAILED record to ACCEPTED, since FAILED records stay retryable). The deterministic `<uid>-remove` OperationID keeps the retries idempotent.

#### Broker no longer knows the batch — empty status responses

The broker answers an unknown/expired batchID with an **empty** `batch_status` (the stream stays alive). After 3 consecutive empty polls — or when a batch has been tracked for over 2 hours (`maxBatchAge`) — the batcher drops the batch and routes every unresolved item through the failure handler with detail `"batch expired at broker"`. A lost broker-side record therefore converts into a fast NodeClaim replacement instead of a silent hang.

---

### [9] NodeProviderIDController reconcile fails — API server error

**Cause:** List of NodeClaims or Nodes fails due to API server unavailability.

**Code path (`controller.go`):**
```go
if err := c.apiReader.List(ctx, &claimList); err != nil {
    return reconcile.Result{}, err
}
```

**State:** NodeClaim still has synthetic ProviderID. No ProviderID was patched.

**Recovery:** Controller-runtime retries with exponential backoff. No data loss. NodeClaim is unmodified and will be reconciled again when the API server recovers.

---

### [10] Node joins but NodeProviderIDController is temporarily down

**Cause:** Pod restart happens exactly when the node is joining (unlikely but possible).

**State:** Node exists in k8s with real ProviderID. NodeClaim still has `rafay://pending/<uid>`.

**Recovery:**
1. On pod restart, controller-runtime re-enqueues all NodeClaims with `rafay://pending/` ProviderID.
2. Controller reconciles → lists nodes → finds the already-joined node → patches ProviderID.
3. No dependency on the node appearing *after* the controller starts — the node watch and the NodeClaim requeue-on-startup both trigger the reconcile.

---

### [11] NodeProviderIDController patch fails — conflict or API server error

**Cause:** Another writer (e.g. Karpenter registration reconciler) updated the NodeClaim concurrently, causing a resource version conflict.

**Code path (`controller.go`):**
```go
if kerrors.IsConflict(err) {
    return reconcile.Result{Requeue: true}, nil
}
return reconcile.Result{}, client.IgnoreNotFound(err)
```

**State:** NodeClaim ProviderID was not updated. The NodeClaim has a newer resource version.

**Recovery:**
- Conflict: immediately requeued, retried with fresh resource version.
- NotFound: the NodeClaim was deleted between the deletion check and the patch (normal disruption race) — ignored cleanly.
- Other error: returned as error, controller-runtime retries with backoff.

In both retry cases the NodeClaim is retried with the correct data on the next reconcile.

---

### [11b] Two pending NodeClaims compete for the same node

**Cause:** Two NodeClaims in the same `nodepoolname` that both accept the node's `sku_name` are pending, and only one node has appeared so far.

**State:** Both NodeClaims have `rafay://pending/<uid>`. One node is available.

**Recovery:**
- `MaxConcurrentReconciles: 1` ensures the controller processes one NodeClaim at a time.
- The first reconcile claims the node by patching its ProviderID → `usedIDs` on the next reconcile includes that ProviderID (read uncached from the API server).
- The second reconcile finds no unclaimed node → requeues for 30s.
- When the second node appears, the second NodeClaim is assigned.

No double-assignment can occur within this controller due to single-threaded reconciliation.

**How `sku_name` is matched.** A node's `sku_name` label is the **platform's** SKU/instance-type name, so it is compared against the NodeClaim's `node.kubernetes.io/instance-type` label (the type `Create()` actually selected) **or** its `spec.nodeClassRef.name` (the legacy convention where a `RafayNodeClass` is named after its one SKU). Matching on the NodeClass name *alone* silently broke every `RafayNodeClass` that lists **multiple** `instanceTypes`: its node could never be resolved, the NodeClaim churned out on the 60-minute registration timeout, and the machine that had in fact been provisioned was orphaned — once per hour, per NodeClaim.

---

### [12] Node never appears — broker succeeded but node never joins

**Cause:** Infrastructure failure after broker SUCCEEDED — VM provisioned but OS/agent fails to start, or network partition prevents the node from registering. (If the broker reports FAILED instead, the failure handler handles it within one poll interval — see step [8].)

**State:** NodeClaim has `Launched=True` + synthetic ProviderID. `NodeProviderIDController` keeps requeuing every 30s but never finds a matching node.

**Recovery:**
1. Karpenter's Registration reconciler sets `Registered=Unknown` when it cannot find a node matching the synthetic ProviderID.
2. The 60-minute `registrationTimeout` timer (forked karpenter, `liveness.go`) starts.
3. At 60 min, Karpenter deletes the NodeClaim.
4. `Delete()` is called with the synthetic ProviderID → `findNodeProviderID()` finds no real node → fires a best-effort broker cancel (a no-op at this point, the op is long past ACCEPTED) → returns `NodeClaimNotFoundError` → Karpenter removes the finalizer and cleans up.
5. Karpenter creates a new NodeClaim and tries again with a fresh OperationID.

This 60-minute path is the **backstop** for failures the broker never reports; broker-reported failures recover in seconds via the failure handler.

---

## Delete Path — Crash and Race Analysis

Karpenter's termination controller (`awaitInstanceTermination`) calls `CloudProvider.Delete()` on **every** reconcile and releases the Node's termination finalizer **only** when `Delete()` returns a `NodeClaimNotFoundError`; any other outcome requeues in **5 seconds**. `Delete()` therefore has to converge on that error, and the whole delete path is designed around a loop that runs every 5s for the life of the removal.

```
[D1] Delete() called by Karpenter (NodeClaim has a real ProviderID)
      │     ← re-entered every ~5s until it converges
[D2] Has the broker already reported <uid>-remove SUCCEEDED? (batcher.Succeeded)
      │     YES → return NodeClaimNotFoundError  ✓  CONVERGENCE — Karpenter releases
      │            the finalizer, drains, and finishes terminating the Node/NodeClaim
      │     NO  → continue
[D3] EnqueueRemove(operationID = <uid>+"-remove")
      │     already in flight at the broker? → resolve immediately, send NOTHING
      │     (otherwise) → in-memory queue
[D4] batchSender sends removes as their own batch → SendBatchRemove (no retry)
      │     broker writes ACCEPTED records (kind "delete") + index, then ACKs
[D5] Broker ACK → Delete() returns nil          ← DURABILITY BOUNDARY
      │     Karpenter requeues in 5s and re-enters at [D1]
[D6] Broker batch processor: CAS ACCEPTED→RUNNING, catalog count -1 (clamped at 0),
      Apply + Publish, poll compute instance → SUCCEEDED / FAILED
      │
[D7] statusPoller observes SUCCEEDED → records the operationID in `succeeded`
      │     → the next pass through [D2] converges
```

> **Node existence is not the completion signal — it cannot be.** It is tempting to have `Delete()` report the instance gone once no Kubernetes Node carries the ProviderID. That never converges: during termination **Karpenter itself holds the Node object alive with its own finalizer**, and only drops that finalizer once `Delete()` says the instance is gone. "Is there still a Node with this providerID?" is therefore circular. Before this was understood, `Delete()` always returned `nil` after broker ACK — so NodeClaims and Nodes stayed `Terminating` **forever**. The broker's SUCCEEDED result is an *external* signal and is the only thing that terminates the loop.

- **In-flight suppression is load-bearing here.** Because [D1] repeats every 5s, an unguarded `EnqueueRemove` would push a fresh remove batch at the broker on every reconcile, flooding its **global** 64-slot queue with hundreds of copies of one removal (and starving every other edge behind it). The batcher holds ACKed-but-not-terminal operationIDs in an `inFlight` set and resolves a re-enqueue immediately without sending. The operationID leaves the set the moment it goes terminal, so a genuine retry after a FAILED is never suppressed.
- **Crash before [D5]** (provider restart with the remove still queued or the send unACKed): `Delete()` never returned, the NodeClaim keeps its finalizer, and Karpenter calls `Delete()` again. The deterministic `<uid>-remove` OperationID makes the retry idempotent at both layers — the batcher's pending map returns the same waiter list for an in-flight retry, and the broker's idempotency switch (ACCEPTED → re-enqueue without rewrite; RUNNING → skip; SUCCEEDED → skip **and refresh the tombstone**; FAILED → retry) guarantees at most one catalog decrement.
- **Crash after [D5]:** the in-memory `succeeded` and `inFlight` sets are lost with the process. Karpenter calls `Delete()` again, which re-sends the removal; the broker recognises the operationID as already SUCCEEDED (its 24-hour tombstone, see below) and **skips it** — no second decrement — and the next status poll re-records it, so the following `Delete()` converges. Worst case is one redundant round trip, never a second node removal.
- **Broker restart after ACK, before processing:** the queue item is lost but the ACCEPTED record (15-min TTL) survives; Karpenter's `Delete()` retries re-enqueue it (the ACCEPTED branch re-enqueues without touching Redis). If no retry lands before the TTL expires, the next status poll for the batch reports FAILED `"operation record expired"` and the following `Delete()` retry rewrites the record and starts over.
- **Remove FAILED at the broker:** log-only in the failure handler, and the operationID is cleared from `inFlight`. The NodeClaim still has its finalizer, so Karpenter keeps calling `Delete()`; the next call re-sends, and the broker rewrites the FAILED record to ACCEPTED. This is exactly why **FAILED records keep the short TTL and are not tombstoned** — a FAILED op must stay retryable.
- **Which machine is removed:** the PaaS catalog is declarative counts, so the platform — not the provider — chooses the physical machine to retire (the ProviderID is carried in the protocol for a future targeted-removal API). Crash-safety-wise this is benign: the decrement is exactly-once per OperationID regardless of which machine PaaS picks.

### The SUCCEEDED tombstone — why a re-sent remove cannot decrement twice

`Delete()` re-sends the same deterministic `<uid>-remove` operationID for as long as Karpenter keeps retrying, which can be a long time (a slow drain, a stuck PDB, a provider restart). The broker's SUCCEEDED record is what makes those re-sends no-ops, so it must **outlive any plausible client retry horizon**:

- SUCCEEDED records are long-lived **tombstones** with a **24-hour TTL** (`karpenterBatchSucceededTTL`), and every duplicate send **refreshes** the TTL, so a persistently retrying client keeps its own tombstone alive.
- **Two distinct retention windows — the broker tombstone is what protects, not the client set.** The client remembers a SUCCEEDED operationID for only `succeededRetention` = **2 h** (`pkg/rafay/batcher.go`) before `pruneSucceededLocked` drops it, whereas the broker's tombstone lives **24 h** (`karpenterBatchSucceededTTL`). `Delete()`'s fast-path convergence via `batcher.Succeeded(operationID)` therefore only works for ~2 h after the poller records SUCCEEDED; once the client entry is pruned, `Delete()` re-sends the removal, and it is the **broker's tombstone — not the client `succeeded` set** — that recognises the opID and prevents a second catalog decrement.
- When the tombstone expired with the 60-minute RUNNING TTL, a re-sent remove opID looked like a **brand-new operation**: it was ACCEPTED again, and the processor **decremented the catalog a second time** — the platform retired an extra, healthy machine. Once per hour, per terminating NodeClaim.
- **FAILED** records deliberately keep the shorter TTL and are **not** tombstoned: expiring a FAILED record is equivalent to re-accepting it, which is exactly what a retryable failure needs.

### Pending-claim deletes and cancellation

When `Delete()` runs against a NodeClaim whose node has not joined (`rafay://pending/` ProviderID and `findNodeProviderID` finds nothing), it fires a **best-effort, fire-and-forget cancel** of the queued add (10s timeout, detached from the caller's context) and returns `NodeClaimNotFoundError` so Karpenter can finalize immediately.

- The broker cancels **only ops still ACCEPTED**, transitioning them to FAILED `"cancelled by client"` via a Redis `WATCH`-based compare-and-set. The CAS means a cancel racing the processor's ACCEPTED→RUNNING transition has exactly one winner: either the op is cancelled before processing starts, or the processor claimed it first and the cancel is a no-op.
- If the cancel loses the race (or is lost entirely — it is fire-and-forget), the add proceeds and a node may still join with no NodeClaim waiting for it. That node is surfaced through `CloudProvider.List()` (Kubernetes Nodes with `rafay://` ProviderIDs) and is cleaned up by normal scale-in once Karpenter observes it as unneeded — capacity drift, not a leak with no owner.
- Cancel CAS conflicts retry up to 5 times inside the broker; persistent failure is logged and the op simply runs to completion.

### `List()` must never return empty — the garbage-collection hazard

Karpenter's core garbage-collection controller treats `CloudProvider.List()` as the authoritative set of instances that exist, and **deletes any `Registered` NodeClaim whose ProviderID is absent from it**. An empty `List()` therefore does not fail safe — it tears down every managed node.

`listNodesFromKube` used to filter Nodes by comparing the first segment of their ProviderID against `RAFAY_CLUSTER_ID`. But a real ProviderID is `rafay://<nodepoolname>/<sku_name>/<hostname>` — that first segment is the **node pool**, not a cluster ID — so the comparison could never match and `List()` always returned nothing. Any node that briefly went `NotReady` (long enough for the GC controller to act) had its NodeClaim deleted underneath it.

The cluster-ID filter is removed: **every** Kubernetes Node carrying a `rafay://` ProviderID is listed. This is also what makes the orphaned-node case above self-healing — a node that joined with no NodeClaim waiting for it is visible to Karpenter and is reclaimed by normal scale-in.

---

## Crash Safety by State (Summary)

| State at pod restart | `Launched` in etcd | Broker knows? | Recovery mechanism |
|---|---|---|---|
| Item in queue, not sent | Unknown | No | Karpenter re-calls `Create()` → fresh first request to broker |
| Batch sent, ACK not received | Unknown | Maybe | Karpenter re-calls `Create()` → broker deduplicates by OperationID |
| Broker ACK'd, `Launched=True` set | True | Yes | `NodeProviderIDController` resumes from etcd; broker FAILED feedback for the lost batchID falls back to the 60-min registration timeout |
| Real ProviderID patched | True | Yes | Fully stable, no recovery needed |
| Remove ACK'd, node still present | (deleting) | Yes | Karpenter keeps calling `Delete()` until the Node is gone; `<uid>-remove` dedups every retry |

---

## Idempotency Guarantee

The broker treats the OperationID as an idempotent key: **adds** use `nodeClaim.UID` and **removes** use `nodeClaim.UID + "-remove"` — both deterministic, so any number of retries map to the same Redis record. For each key at most one catalog mutation is performed, enforced by the per-op idempotency switch on receipt (ACCEPTED → re-enqueue without rewrite, RUNNING/SUCCEEDED → skip, FAILED → retry) plus the ACCEPTED→RUNNING compare-and-set that lets exactly one processor run claim each op. This covers:

- Pod restart mid-batch (steps [2]–[5], [D2]–[D4])
- Network retry of `SendBatch` / `SendBatchRemove` (step [4], [D3])
- Karpenter re-queuing a NodeClaim that was already processed (steps [1], [7])
- `Create()`/`Delete()` context cancellation after the broker received the request
- Duplicate broker queue items after a broker restart (stale items find no ACCEPTED ops and no-op)

---

## NodeProviderIDController — Restart Recovery Detail

The controller uses controller-runtime's standard watch mechanism on `NodeClaim` objects. On startup, controller-runtime reconciles every existing object it watches. This means:

- All pending NodeClaims (`rafay://pending/` ProviderID) are automatically re-enqueued at startup.
- No explicit "resume on startup" logic is needed.
- The controller is fully stateless — every reconcile reads NodeClaims (uncached, via `apiReader`) and Nodes from the API server.

`MaxConcurrentReconciles: 1` ensures the read-decide-patch cycle for node assignment is never concurrent, preventing two NodeClaims from claiming the same node (step [11b]).

---

## Registration Timeout Window

Karpenter's `registrationTimeout` is **60 minutes** (set in the forked `sigs.k8s.io/karpenter`, `pkg/controllers/nodeclaim/lifecycle/liveness.go`). The timer starts when the `Registered` condition is first set to `Unknown` (Registration reconciler cannot find a node matching the synthetic ProviderID).

With real nodes joining at ~12 minutes:

```
t=0       Create() returns at broker ACK → Launched=True, synthetic ProviderID persisted
t=~1s     Registration reconciler runs → Registered=Unknown (60-min timer starts)
t=12m     Node joins → NodeProviderIDController patches real ProviderID
t=12m+    Registration reconciler finds node → Registered=True  ✓
t=60m     registrationTimeout backstop — only reached if the broker reported nothing
```

The 60-minute timer is deliberately a **backstop**, not the primary failure path: broker-reported FAILED results reach the failure handler within roughly one 30s poll interval and replace the NodeClaim immediately. The timeout only fires for failures the broker cannot see — typically a node that provisioned successfully at the catalog level but never registered with the cluster — or when a provider restart discarded the in-memory poll state for an in-flight batch.

If a pod restart happens anywhere in the `t=0` to `t=12m` window, the timer continues from where it left off (the `LastTransitionTime` on the `Registered` condition is persisted in etcd). The worst case adds the controller startup time to the recovery, which comfortably fits inside the 60-minute window.
