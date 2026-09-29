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
     │              broker classifies the batch (≤64 nodes, this edge's queue < 16 waiting),
     │              writes ACCEPTED op records + batchID index to Redis, enqueues the batch
     │              on THIS EDGE's queue, THEN replies KarpenterBatchAccepted
     │
[5] Broker ACK → resultCh resolved, batch registered in inProgress map
     │         ← DURABILITY BOUNDARY
     │           (broker side: Redis records; client side: the batch id is stamped on the
     │            NodeClaim in [6] so a restarted provider can resume polling)
[6] Create() returns with ProviderID = "rafay://pending/<nodeclaim-uid>"
     │         and annotation karpenter.rafay.io/batch-id = <batch>
[7] Karpenter sets Launched=True + synthetic ProviderID (+ the annotation) on NodeClaim in etcd
     │
[8] statusPoller polls broker every 30s, starting on the next tick after ACK (no initial delay)
     │    SUCCEEDED → recorded in the batcher's `succeeded` set (for an add, registration is
     │                driven by the node actually joining, not by this; for a REMOVE this is
     │                what lets Delete() converge — see the Delete Path section)
     │    FAILED    → FailureHandler records the op, deletes the pending NodeClaim → fast reprovision
     │
[9] NodeProviderIDController reconciles NodeClaim (node watch + 30s requeue)
     │
[10] Real node joins cluster (~12 min typical, up to 60 min), node gets spec.providerID from Rafay platform
     │
[11] Controller labels the Node karpenter.sh/registered=true, then patches
     │    NodeClaim.Status.ProviderID = node.Spec.ProviderID
     │
[12] Karpenter Registration reconciler finds matching node → Registered=True
```

The broker ACK at step [5] is the durability boundary. **Before it**: all state is transient on the client and recoverable by Karpenter re-calling `Create()`. **After it**: the request is recorded in the broker's Redis (per-op ACCEPTED records plus the batchID→opIDs index, written *before* the ACK is sent), and the NodeClaim's `Launched=True` + synthetic ProviderID + batch-id annotation land in etcd. Both sides can lose their process and recover.

`Create()` unblocks at the ACK — not at SUCCEEDED. On the **add** path the status poller (step [8]) exists for asynchronous failure feedback: a broker-reported FAILED invokes the registered `FailureHandler`, which records the operation (so a later `Delete()` of the same pending NodeClaim knows no machine is coming) and deletes the NodeClaim (UID == operationID) if it is still pending, so Karpenter reprovisions within seconds rather than waiting out the 60-minute registration timeout. On the **remove** path the poller carries more weight — its SUCCEEDED observation is what lets `Delete()` report the instance gone and release the Node's termination finalizer (see [Delete Path](#delete-path--crash-and-race-analysis)).

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

**Cause:** gRPC connection failure, the 30s `brokerCallTimeout` expiring, broker unreachable, or the broker replying `KarpenterBatchRejected` — this edge's queue already holds 16 waiting batches (`batch queue full`), the batch exceeds 64 nodes (`batch too large`), or the batch id belongs to another edge. A rejection is decided before anything is written.

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

If the gRPC call was partially delivered (broker wrote its Redis records but the response was lost), the broker still has the request. On retry, broker deduplicates by OperationID: ops still ACCEPTED are re-enqueued without rewriting Redis, RUNNING/SUCCEEDED ops are skipped. A queue-full rejection writes nothing; only in the residual race where the queue fills between the check and the enqueue are the ops this call itself wrote CAS'd to FAILED (`"batch queue full; try again"`), which the broker treats as a retryable state — a re-send rewrites them to ACCEPTED. The shared gRPC connection is never closed on a failure (that would abort the other goroutines' in-flight streams); gRPC reconnects by itself.

---

### [5] Broker ACK delivered — pod restarts before the status poller ever runs

**Cause:** Pod killed after `registerAndAck` resolved the result channels but before (or between) status polls.

**State:** The `inProgress` map is in-memory and lost. `Create()` already returned, so depending on timing the NodeClaim either has `Launched=True` in etcd or Karpenter re-calls `Create()`. The broker **has** the request and is provisioning.

**Recovery:**
1. If `Launched=True` was persisted: `NodeProviderIDController` resumes from etcd (step [9]), and the **`batchresume`** runnable (one leader-only pass at startup) lists every pending, non-deleting NodeClaim, groups them by the `karpenter.rafay.io/batch-id` annotation `Create()` stamped, and calls `NodeBatcher.ResumeAddBatch` per batch — the poller follows the old batch again and a broker-side FAILED still reaches the failure handler within a poll interval. A NodeClaim created by a provider from before the annotation existed falls back to Karpenter's 60-minute `registrationTimeout`, which deletes the NodeClaim if no node ever registers — slow, but safe.
2. If not: Karpenter re-calls `Create()` with the same `OperationID = string(nodeClaim.UID)`; the **broker deduplicates by OperationID** — no second node provisioned — and a fresh ACK unblocks the call.
3. A resumed batch the broker has forgotten (its 3-hour index expired) yields empty polls, which the batcher treats as expiry — the right outcome, since that add really is lost.

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

`PollBatchStatus` itself retries once on `codes.Unavailable`, on the same connection after `ResetConnectBackoff()` (polls are read-only and safe to re-issue); beyond that the failure is logged and the next 30-second tick retries. Nothing blocks on the poll — `Create()` already returned at ACK. A result in a state the provider does not recognise is logged once and treated as still in progress, never as FAILED.

#### Broker returns FAILED for a node in the batch

**Cause:** Broker was unable to provision the node (catalog/PaaS failure — an add whose platform run ends FAILED also has its catalog increment restored so the reprovision is a single increment), the op was cancelled (`"cancelled by client"`), the broker was shutting down or lost a per-cluster lock race (`"broker shutting down; retry"`, `"cluster locked by another broker; retry"`), or its ACCEPTED record expired (`"operation record expired"`). Expiry while waiting is no longer expected: the broker keeps every queued batch's ACCEPTED records alive (a per-queue keepalive re-arms the 15-minute TTL every 5 minutes while the batch sits behind the same edge's earlier 60–90-minute adds), and a broker restart is covered by the startup sweep that re-queues every still-ACCEPTED op within seconds. It can still happen when a broker dies with a queue it never recovers, which is exactly the case the failure handler exists for.

**Code path (`batcher.go` → `cloudprovider.NewBatchFailureHandler`):**
```go
case v1.KARPENTER_NODE_OPERATION_STATE_FAILED:
    failed = append(failed, failedOp{operationID: item.operationID, detail: nr.GetDetail()})
...
fn(ctx, f.operationID, kind, f.detail)   // failure handler, outside the lock
```

**Recovery (add):** the failure handler records the operation in `PoolBackoff` (`MarkFailedOp`, kept 2 h — so a `Delete()` of this pending NodeClaim returns NotFound without a cancel RPC), then looks up the NodeClaim whose UID equals the operationID and **deletes it** — but only if it still carries a `rafay://pending/` ProviderID and no deletion timestamp. Karpenter observes the deletion and provisions a replacement NodeClaim (fresh UID → fresh OperationID) within seconds. NodeClaims that already resolved a real node, or that are already being deleted, are left alone. Some details are permanent: `pool at maximum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling`, `pool precondition` (`IsPermanentRefusalDetail`). A replacement would be refused too, so the handler first holds the NodePool back in `PoolBackoff` (`RAFAY_POOL_AT_MAX_COOLDOWN`, default 5 min) — `GetInstanceTypes` withholds its offerings and the pods stay pending — records a Warning event on the NodePool (`PoolAtPlatformMaximum` or `PoolRefusedByPlatform`, message = the broker's detail), and then deletes the NodeClaim as usual. The hold is in memory only; a restart drops it, which costs at most one more refused round trip.

**Recovery (remove):** log-only in the failure handler for a transient failure — but the operationID is cleared from the batcher's in-flight set, which is what makes recovery work. Karpenter keeps calling `Delete()` every 5s while the NodeClaim is terminating, so the next call re-sends the removal (the broker rewrites the FAILED record to ACCEPTED, since FAILED records stay retryable). The deterministic `<uid>-remove` OperationID keeps the retries idempotent. A remove an **old** broker FAILED for a permanent reason (`is at its minimum`, `pool at minimum`, …) is recorded instead, and the next `Delete()` converges with a `NodeNotRetired` event rather than re-sending every 5s; a current broker reports that case as SUCCEEDED `node not retired: …` (see the Delete Path).

#### Broker no longer knows the batch — empty status responses

The broker answers an unknown/expired batchID with an **empty** `batch_status` (the stream stays alive). After 3 consecutive empty polls — or when a batch has been tracked for over 3 hours (`maxBatchAge`; deliberately above the 60-minute node-add bound and the broker's 90-minute add deadline, so a merely slow add is never reported FAILED) — the batcher drops the batch and routes every unresolved item through the failure handler with detail `"batch expired at broker"`. A lost broker-side record therefore converts into a fast NodeClaim replacement instead of a silent hang.

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
- `MaxConcurrentReconciles: 1` ensures the controller processes one NodeClaim at a time, and `cloudprovider.NodeOwnershipMu` (held from the NodeClaim LIST to the status patch) serializes it with the adoption controller and `Delete()`'s pending-claim lookup, so no two of the three read-decide-write cycles can bind one node to different NodeClaims.
- The first reconcile claims the node by patching its ProviderID → `usedIDs` on the next reconcile includes that ProviderID (read uncached from the API server); `usedIDs` also contains every `karpenter.rafay.io/adopted-provider-id`, so a node an adopted (not yet Launched) NodeClaim reserves is never bound to a pending one.
- The second reconcile finds no unclaimed node → requeues for 30s.
- When the second node appears, the second NodeClaim is assigned.

No double-assignment can occur within this controller due to single-threaded reconciliation.

**How `sku_name` is matched.** A node's `sku_name` label is the **platform's** SKU/instance-type name, so it is compared against the NodeClaim's `node.kubernetes.io/instance-type` label (the type `Create()` actually selected); only a NodeClaim with **no** instance-type label falls back to its `spec.nodeClassRef.name` (the legacy convention where a `RafayNodeClass` is named after its one SKU). Matching on the NodeClass name *alone* silently broke every `RafayNodeClass` that lists **multiple** `instanceTypes`: its node could never be resolved, the NodeClaim churned out on the 60-minute registration timeout, and the machine that had in fact been provisioned was orphaned — once per hour, per NodeClaim.

---

### [12] Node never appears — broker succeeded but node never joins

**Cause:** Infrastructure failure after broker SUCCEEDED — VM provisioned but OS/agent fails to start, or network partition prevents the node from registering. (If the broker reports FAILED instead, the failure handler handles it within one poll interval — see step [8].)

**State:** NodeClaim has `Launched=True` + synthetic ProviderID. `NodeProviderIDController` keeps requeuing every 30s but never finds a matching node.

**Recovery:**
1. Karpenter's Registration reconciler sets `Registered=Unknown` when it cannot find a node matching the synthetic ProviderID.
2. The 60-minute `registrationTimeout` timer (forked karpenter, `liveness.go`) starts.
3. At 60 min, Karpenter deletes the NodeClaim.
4. `Delete()` is called with the synthetic ProviderID and consults the add's own state (`resolvePendingRemoval`): the add **SUCCEEDED** at the broker, so the platform is counting a machine for it — `Delete()` sends an **untargeted** `<uid>-remove` (empty `provider_id`, since no Node ever registered) so the catalog count is corrected, and converges once the broker reports it SUCCEEDED. (Had the add FAILED, the failure handler's record would answer `NodeClaimNotFoundError` with no RPC; had it still been open, a synchronous cancel would decide.)
5. Karpenter creates a new NodeClaim and tries again with a fresh OperationID.

This 60-minute path is the **backstop** for failures the broker never reports; broker-reported failures recover in seconds via the failure handler. Note that adding a node legitimately takes up to 60 minutes, so the timer equals that bound with no margin — every other add-path deadline (broker 90 min, RUNNING record 2 h, batch index and client tracking 3 h) sits above it.

---

## Delete Path — Crash and Race Analysis

Karpenter's node termination controller finalizes a Node in order — `awaitDrain` → `awaitVolumeDetachment` → `awaitInstanceTermination` — and stops at the first step that has to wait. `CloudProvider.Delete()` is therefore **not** called while a node is draining or a PDB is holding it; it is called only once the drain and volume detachment are complete, and from then on on **every** reconcile. The finalizer is released **only** when `Delete()` returns a `NodeClaimNotFoundError`; any other outcome requeues in **5 seconds**. `Delete()` therefore has to converge on that error, and the whole delete path is designed around a loop that runs every 5s for the life of the removal.

```
[D0] Node drained, volumes detached (termination controller; no broker op exists yet)
      │
[D1] Delete() called by Karpenter (NodeClaim has a real ProviderID)
      │     ← re-entered every ~5s until it converges
[D2] Has the broker already reported <uid>-remove SUCCEEDED? (batcher.SucceededResult)
      │     YES, detail "node not retired: …" → NodeClaimNotFoundError ✓ + Warning NodeNotRetired
      │            (the platform refused; the machine keeps running without a Node object until
      │             its kubelet is restarted / it is re-registered, then adoption takes it back)
      │     YES, provider IDs named but not ours → retired-other hold (see below), never re-sent
      │     YES, untargeted: Node gone or NotReady → NodeClaimNotFoundError ✓ CONVERGENCE —
      │            Karpenter releases the finalizer and finishes terminating the Node/NodeClaim
      │            Node still Ready → nil, for up to RAFAY_REMOVE_SETTLE_WINDOW (60m) from the
      │            first sighting; still Ready after that → retired-other hold
      │     NO  → continue
[D2b] First remove in this process? Look up the Node carrying the ProviderID:
      │     none at all → NodeClaimNotFoundError, send NOTHING (the machine is already gone —
      │            a remove would retire one more healthy machine: the GC cascade)
      │     Terminating, kubelet stopped, deleted no later than the NodeClaim, not chosen by
      │       the disruption queue → NodeClaimNotFoundError, send NOTHING, Warning NodeRetiredExternally
      │     (otherwise) → continue
[D3] EnqueueRemove(operationID = <uid>+"-remove", providerID)
      │     already in flight at the broker? → resolve immediately, send NOTHING
      │     (otherwise) → in-memory queue
[D4] batchSender sends removes as their own batch → SendBatchRemove (no retry)
      │     broker writes ACCEPTED records (kind "delete") + index, then ACKs
[D5] Broker ACK → Delete() returns nil          ← DURABILITY BOUNDARY
      │     Karpenter requeues in 5s and re-enters at [D1]
[D6] Broker batch processor (this edge's queue, per-cluster lock): CAS ACCEPTED→RUNNING,
      catalog count -1 (refused below the pool minimum → SUCCEEDED "node not retired"),
      Apply + Publish → every op SUCCEEDED at publish (24 h tombstone); settle poll up to 60 min
      keeps SUCCEEDED either way ("catalog decremented; compute instance did not settle")
      │
[D7] statusPoller observes SUCCEEDED (next 30 s tick) → records it in `succeeded`
      │     → the next pass through [D2] acts on it; convergence follows once the NodeClaim's
      │       own machine has stopped (kubelet NotReady ~40 s after it halts)
```

> **Node existence is not the completion signal — it cannot be. Readiness is a valid signal for the opposite question.** It is tempting to have `Delete()` report the instance gone once no Kubernetes Node carries the ProviderID. That never converges: during termination **Karpenter itself holds the Node object alive with its own finalizer**, and only drops that finalizer once `Delete()` says the instance is gone. "Is there still a Node with this providerID?" is therefore circular. Before this was understood, `Delete()` always returned `nil` after broker ACK — so NodeClaims and Nodes stayed `Terminating` **forever**. The broker's SUCCEEDED result is the *external* signal that says the platform accepted the removal. But the removal is untargeted, so SUCCEEDED does not say *this* machine is gone — and the finalizer keeps the Node **object**, not the kubelet's heartbeat, so `Ready` can answer that. `Delete()` converges on SUCCEEDED **plus** the Node being absent or NotReady; the alternative dropped a Node object whose kubelet was alive, and a v1.31 kubelet never re-registers on its own.

- **In-flight suppression is load-bearing here.** Because [D1] repeats every 5s, an unguarded `EnqueueRemove` would push a fresh remove batch at the broker on every reconcile, flooding this edge's 16-slot queue with hundreds of copies of one removal. The batcher holds ACKed-but-not-terminal operationIDs in an `inFlight` set and resolves a re-enqueue immediately without sending. The operationID leaves the set the moment it goes terminal, so a genuine retry after a FAILED is never suppressed. The 5-second retries also issue no uncached LISTs — the per-UID lookups are memoised for the life of the process.
- **Crash before [D5]** (provider restart with the remove still queued or the send unACKed): `Delete()` never returned, the NodeClaim keeps its finalizer, and Karpenter calls `Delete()` again. The deterministic `<uid>-remove` OperationID makes the retry idempotent at both layers — the batcher's pending map returns the same waiter list for an in-flight retry, and the broker's idempotency switch (ACCEPTED → re-enqueue without rewrite; RUNNING → skip; SUCCEEDED → skip **and refresh the tombstone**; FAILED → retry) guarantees at most one catalog decrement.
- **Crash after [D5]:** the in-memory `succeeded` and `inFlight` sets are lost with the process. Karpenter calls `Delete()` again; the Node still carries the ProviderID (step [D2b] passes), so it re-sends the removal; the broker recognises the operationID as already SUCCEEDED (its 24-hour tombstone, see below) and **skips it** — no second decrement — and the next status poll re-records it, so the following `Delete()` acts on it. The settle window restarts with the process. A retired-other verdict is read back from the `karpenter.rafay.io/remove-retired-other` annotation, so it survives both the restart and the tombstone's expiry. Worst case is one redundant round trip, never a second node removal.
- **Broker restart after ACK, before processing:** the queue item is lost but the ACCEPTED record (15-min TTL) survives. Recovery is the **broker's** startup sweep (`sweepOrphanedKarpenterOps` re-queues every still-ACCEPTED op onto its edge's queue within seconds) — *not* Karpenter's `Delete()` retries: while the operationID sits in the provider's `inFlight` set those retries never reach the broker. If the sweep did not run, the next status poll after the 15-minute TTL reports FAILED `"operation record expired"`, the failure handler clears `inFlight`, and the following `Delete()` retry rewrites the record and starts over.
- **Broker restart mid-processing (op RUNNING):** the record is deliberately *not* swept — the catalog write may already have landed — and expires after 2 h; the poll then reports `"operation record expired"` and the retry starts over. A remove is tombstoned SUCCEEDED at publish, so this window is only the read-modify-write itself.
- **Remove FAILED at the broker:** log-only in the failure handler, and the operationID is cleared from `inFlight`. The NodeClaim still has its finalizer, so Karpenter keeps calling `Delete()`; the next call re-sends, and the broker rewrites the FAILED record to ACCEPTED. This is exactly why **FAILED records are not tombstoned** — a FAILED op must stay retryable. Nothing after the platform commit produces a FAILED (`committed; …` is SUCCEEDED), and a remove the platform refuses for good is SUCCEEDED `node not retired: …`, so the only FAILED removes are genuinely transient.
- **Which machine is removed:** the PaaS catalog is declarative counts, so the platform — not the provider — chooses the physical machine to retire (the ProviderID is carried in the protocol for a future targeted-removal API). Crash-safety-wise the decrement is exactly-once per OperationID regardless of which machine PaaS picks; the provider compensates for the *choice* by never sending a remove for a machine that is already gone ([D2b]), by waiting for the NodeClaim's own machine to stop, and by holding — never re-sending — a remove after which the platform demonstrably retired another machine (`RemoveRetiredOtherMachine`; operator runbook: remove the Node's `karpenter.sh/termination` finalizer once the machine is retired or re-registered). With today's brokers, which report no provider IDs for removes, the named-other path is latent; the settle-window path is live.

### The SUCCEEDED tombstone — why a re-sent remove cannot decrement twice

`Delete()` re-sends the same deterministic `<uid>-remove` operationID for as long as Karpenter keeps retrying, which can be a long time (a provider restart, a slow platform retire — up to the 60-minute settle window). The broker's SUCCEEDED record is what makes those re-sends no-ops, so it must **outlive any plausible client retry horizon**:

- SUCCEEDED records are long-lived **tombstones** with a **24-hour TTL** (`karpenterBatchSucceededTTL`), and every duplicate send **refreshes** the TTL, so a persistently retrying client keeps its own tombstone alive.
- **Two distinct retention windows — the broker tombstone is what protects, not the client set.** The client remembers a SUCCEEDED operationID for only `succeededRetention` = **2 h** (`pkg/rafay/batcher.go`) before `pruneSucceededLocked` drops it (the prune runs on every poller tick), whereas the broker's tombstone lives **24 h** (`karpenterBatchSucceededTTL`). `Delete()`'s fast-path convergence via `batcher.Succeeded(operationID)` therefore only works for ~2 h after the poller records SUCCEEDED; once the client entry is pruned, `Delete()` re-sends the removal, and it is the **broker's tombstone — not the client `succeeded` set** — that recognises the opID and prevents a second catalog decrement.
- When the tombstone expired with the then 60-minute RUNNING TTL, a re-sent remove opID looked like a **brand-new operation**: it was ACCEPTED again, and the processor **decremented the catalog a second time** — the platform retired an extra, healthy machine. Once per hour, per terminating NodeClaim.
- **FAILED** records are **not** tombstoned: expiring a FAILED record is equivalent to re-accepting it, which is exactly what a retryable failure needs.

### Pending-claim deletes and cancellation

When `Delete()` runs against a NodeClaim whose ProviderID is still `rafay://pending/<uid>`, only the **add's own state** decides what happens (`resolvePendingRemoval`) — never a Node that merely matches the NodeClaim's pool/SKU, which may belong to another pending NodeClaim or to an operator:

1. A remove already ACKed for this NodeClaim in this process is re-sent as is.
2. The add **SUCCEEDED** at the broker: a machine exists or is about to register, and a cancel cannot undo that. If a joined Node can be resolved (`findNodeProviderID`, under `NodeOwnershipMu`), its ID is patched onto the NodeClaim first — a reservation, so the `NodeProviderIDController` cannot bind the same node to another pending NodeClaim while the removal is in flight — and the remove is sent with it; otherwise the remove is sent **untargeted** (empty `provider_id`) so the catalog count is still corrected. Returning NotFound here would finalize the NodeClaim while its machine lands with no owner and the catalog keeps counting it.
3. The add **FAILED** (the failure handler recorded it): no machine is coming → `NodeClaimNotFoundError`, no RPC.
4. Otherwise the add is queued or running and only a **synchronous** cancel can tell which. The broker cancels **only ops still ACCEPTED**, transitioning them to FAILED `"cancelled by client"` via a Redis `WATCH`-based compare-and-set; a cancel racing the processor's ACCEPTED→RUNNING transition has exactly one winner. Cancelled → `NodeClaimNotFoundError`. Not cancellable (RUNNING or already finished) → `Delete()` returns `nil` and the NodeClaim stays Terminating until the poller reports the add's terminal state (then 2 or 3 applies); the answer is memoised, one RPC per NodeClaim. A cancel RPC error is returned so Karpenter retries in 5s. Cancel CAS conflicts retry up to 5 times inside the broker; persistent failure is logged and the op simply runs to completion.

A node that joined with **no** NodeClaim waiting for it (a lost cancel from an older, fire-and-forget provider) is **not** cleaned up by scale-in on its own: `CloudProvider.List()` visibility only stops core Karpenter's garbage collector from deleting *registered NodeClaims*, and every disruption method refuses a Node without a NodeClaim (`StateNode.ValidateNodeDisruptable`). The only path that gives such a node an owner is the node-adoption controller (`KARPENTER_ADOPT_EXISTING_NODES`, default on): it adopts the node once Ready, and `WhenEmpty` consolidation reclaims it only when it is empty. With adoption disabled it is an unowned, uncounted machine that has to be removed from the catalog by hand.

### `List()` must never return empty — the garbage-collection hazard

Karpenter's core garbage-collection controller treats `CloudProvider.List()` as the authoritative set of instances that exist, and **deletes any `Registered` NodeClaim whose ProviderID is absent from it**. An empty `List()` therefore does not fail safe — it tears down every managed node.

`listNodesFromKube` used to filter Nodes by comparing the first segment of their ProviderID against `RAFAY_CLUSTER_ID`. But a real ProviderID is `rafay://<nodepoolname>/<sku_name>/<hostname>` — that first segment is the **node pool**, not a cluster ID — so the comparison could never match and `List()` always returned nothing. Any node that briefly went `NotReady` (long enough for the GC controller to act) had its NodeClaim deleted underneath it.

The cluster-ID filter is removed: **every** Kubernetes Node carrying a `rafay://` ProviderID is listed. That keeps registered NodeClaims safe from GC; it does *not* make an unowned node reclaimable — only adoption does (see above). And because GC of a NodeClaim whose Node has vanished used to trigger an untargeted remove that retired one more healthy machine per cycle, `Delete()` now sends nothing when no Node carries the ProviderID ([D2b]) — the GC feedback cascade is closed on the delete side.

---

## Crash Safety by State (Summary)

| State at pod restart | `Launched` in etcd | Broker knows? | Recovery mechanism |
|---|---|---|---|
| Item in queue, not sent | Unknown | No | Karpenter re-calls `Create()` → fresh first request to broker |
| Batch sent, ACK not received | Unknown | Maybe | Karpenter re-calls `Create()` → broker deduplicates by OperationID |
| Broker ACK'd, `Launched=True` set | True | Yes | `NodeProviderIDController` resumes from etcd; `batchresume` re-registers the batch from `karpenter.rafay.io/batch-id`, so broker FAILED feedback still arrives within a poll interval (a NodeClaim without the annotation falls back to the 60-min registration timeout) |
| Real ProviderID patched | True | Yes | Fully stable, no recovery needed |
| Remove ACK'd, node still present | (deleting) | Yes | Karpenter keeps calling `Delete()`; `<uid>-remove` dedups every retry at the broker (24 h tombstone); `Delete()` converges once the broker reports SUCCEEDED **and** the NodeClaim's own machine is gone or NotReady (settle window restarts; a retired-other verdict is read back from the NodeClaim annotation) |

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
- No explicit "resume on startup" logic is needed **for the binding**. The add batches themselves are a different matter: the batcher's poll state is in memory, so the separate `batchresume` runnable re-registers them from the `karpenter.rafay.io/batch-id` annotation (step [5]). Removes are not resumed on purpose — Karpenter re-issues `Delete()` with the same deterministic operation ID.
- The controller is fully stateless — every reconcile reads NodeClaims (uncached, via `apiReader`) and Nodes from the API server.

`MaxConcurrentReconciles: 1` ensures the read-decide-patch cycle for node assignment is never concurrent, preventing two NodeClaims from claiming the same node (step [11b]); `cloudprovider.NodeOwnershipMu` extends that guarantee across the adoption controller and `Delete()`.

---

## Registration Timeout Window

Karpenter's `registrationTimeout` is **60 minutes** (set in the forked `sigs.k8s.io/karpenter`, `github.com/RafaySystems/karpenter-rafay` branch `rafay-release-v1.14.x`, `pkg/controllers/nodeclaim/lifecycle/liveness.go` — outside both repos). The timer starts when the `Registered` condition is first set to `Unknown` (Registration reconciler cannot find a node matching the synthetic ProviderID).

With real nodes typically joining at ~12 minutes (and up to 60 — the platform builds the whole machine):

```
t=0       Create() returns at broker ACK → Launched=True, synthetic ProviderID persisted
t=~1s     Registration reconciler runs → Registered=Unknown (60-min timer starts)
t≤30s     first status poll on the next ticker tick (a permanent refusal is handled here)
t=12m     Node joins (typical) → NodeProviderIDController patches real ProviderID
t=12m+    Registration reconciler finds node → Registered=True  ✓
t=60m     registrationTimeout backstop — only reached if the broker reported nothing
```

The 60-minute timer is deliberately a **backstop**, not the primary failure path: broker-reported FAILED results reach the failure handler within roughly one 30s poll interval and replace the NodeClaim immediately. The timeout only fires for failures the broker cannot see — typically a node that provisioned successfully at the catalog level but never registered with the cluster (its NodeClaim then gets an untargeted remove, step [12]) — or, for a NodeClaim created before the batch-id annotation existed, when a provider restart discarded the in-memory poll state for an in-flight batch.

If a pod restart happens anywhere in the `t=0` to `t=12m` window, the timer continues from where it left off (the `LastTransitionTime` on the `Registered` condition is persisted in etcd). The worst case adds the controller startup time to the recovery, which comfortably fits inside the 60-minute window.
