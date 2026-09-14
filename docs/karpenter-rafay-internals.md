# Karpenter + Rafay Provider: Internals & Bugs Fixed

This document captures architectural decisions, non-obvious behavior, and bugs discovered and fixed
in the Rafay Karpenter provider. Reading this before touching the provisioning path will save hours.

---

## Table of Contents

1. [Karpenter Provisioning Loop](#1-karpenter-provisioning-loop)
2. [NodeClaim Lifecycle](#2-nodeclaim-lifecycle)
3. [Why cluster.Synced() Blocks Everything](#3-why-clustersynced-blocks-everything)
4. [NodeBatcher: Batch Send Architecture](#4-nodebatcher-batch-send-architecture)
5. [Bug Fix: Create() Was Blocking for 10 Minutes](#5-bug-fix-create-was-blocking-for-10-minutes)
6. [Synthetic Pending ProviderID](#6-synthetic-pending-providerid)
7. [NodeProviderIDController](#7-nodeprovideridcontroller)
8. [Node Initialization Sequence](#8-node-initialization-sequence)
9. [Bug Fix: Runaway NodeClaim Creation](#9-bug-fix-runaway-nodeclaim-creation)
10. [Pod Scheduling: What Karpenter Does vs kube-scheduler](#10-pod-scheduling-what-karpenter-does-vs-kube-scheduler)
11. [What Happens When a NodeClaim Is Ready but Pods Are Still Pending](#11-what-happens-when-a-nodeclaim-is-ready-but-pods-are-still-pending)
12. [Bug Fix: Delete() Never Converged — Nodes Stuck Terminating Forever](#12-bug-fix-delete-never-converged--nodes-stuck-terminating-forever)
13. [Bug Fix: List() Always Returned Empty — GC Deleted Healthy NodeClaims](#13-bug-fix-list-always-returned-empty--gc-deleted-healthy-nodeclaims)
14. [Adopting Pre-Existing Nodes Without Provisioning One](#14-adopting-pre-existing-nodes-without-provisioning-one)

---

## 1. Karpenter Provisioning Loop

The provisioner is a singleton controller (`MaxConcurrentReconciles=1`). Its reconcile loop:

```
Reconcile():
  if !cluster.Synced()  → return RequeueImmediately (no scheduling)
  pods = GetProvisionablePods()
  if len(pods) == 0     → return (nothing to do)
  schedule = scheduler.Solve(pods)       // bin-pack pods into NodeClaims (in-memory only)
  CreateNodeClaims(schedule.NodeClaims)  // kubeClient.Create() each NodeClaim CR
```

**Key point:** `Provisioner.Create()` only does `kubeClient.Create(nodeClaim)` — it does NOT call
`CloudProvider.Create()`. That happens later in the lifecycle controller.

**Pod → NodeClaim mapping** is in-memory only (`cluster.podToNodeClaim` sync.Map). There is no pod
list on the NodeClaim CR. The actual pod binding is done by kube-scheduler, not Karpenter.

**`scheduler.Solve()`** tries for each pod, in order:
1. `addToExistingNode` — fit pod to an existing/in-flight node already in cluster state
2. `addToInflightNode` — fit pod to a NodeClaim being created in this same scheduling pass
3. `addToNewNodeClaim` — create a brand-new NodeClaim

---

## 2. NodeClaim Lifecycle

After a NodeClaim CR is created, four reconcilers run sequentially via status conditions:

```
Launch        → CloudProvider.Create() → sets Status.ProviderID, Capacity, Allocatable
Registration  → finds Node via spec.providerID match → removes UnregisteredNoExecuteTaint
Initialization → checks Node is Ready, taints cleared, resources registered → sets initialized=true label
Liveness      → deletes NodeClaim if not registered within 60 min (or launched within 5 min)
```

Each reconciler is gated on the previous one's condition:
- `Launch.Reconcile`: runs when `Launched` condition is Unknown
- `Registration.Reconcile`: runs when `Registered` condition is Unknown
- `Initialization.Reconcile`: runs when `Initialized` condition is Unknown AND `Registered=True`

**Timeouts** (defined in `lifecycle/liveness.go`):
- `launchTimeout = 5 min` — NodeClaim deleted if Launch doesn't succeed
- `registrationTimeout = 60 min` — NodeClaim deleted if node doesn't register

---

## 3. Why cluster.Synced() Blocks Everything

`cluster.Synced()` in `state/cluster.go` returns `false` if ANY tracked NodeClaim has an **empty**
`Status.ProviderID`:

```go
for _, providerID := range c.nodeClaimNameToProviderID {
    if providerID == "" {
        return false
    }
}
```

**`Synced()=false` completely halts the provisioning loop** — no new NodeClaims are created for any
pending pod until every in-flight NodeClaim has a non-empty ProviderID.

The ProviderID is set by the lifecycle controller's `Launch.Reconcile` when `CloudProvider.Create()`
returns. Before our fix, `Create()` blocked for ~10 minutes waiting for the broker to report the
node as fully provisioned. This meant:

- Every NodeClaim held `Synced()=false` for ~10 minutes
- No new pods could be provisioned during that window
- If a pod didn't fit the in-flight node, it could not get a new NodeClaim for 10 minutes

After our fix, `Create()` returns in seconds after broker ACK, ProviderID is set quickly, and
`Synced()` becomes `true` within seconds.

---

## 4. NodeBatcher: Batch Send Architecture

`pkg/rafay/batcher.go` — NodeBatcher collects individual `Create()` AND `Delete()` calls and sends
them as batches to edge-broker via `BatchStreamOperations`. Every broker RPC (send, status poll,
cancel) carries a 30 s deadline (`brokerCallTimeout` in `brokerclient.go`).

```
Create() → Enqueue(uid, req)                     → blocks on resultCh
Delete() → EnqueueRemove(uid+"-remove", req)     → blocks on resultCh
                                                        ↑
batchSender goroutine:
  collectBatch: blocks until the FIRST item arrives, then collects for up to
                batchWindow (10 s) or maxBatchSize (10 items)
  → partitions by kind: adds → SendBatch, removes → SendBatchRemove
  → on broker ACK: sends BatchResult{} to each resultCh  ← unblocks Create()/Delete()
  → stores batch in inProgress map (pollAfter = send + 120 s)

statusPoller goroutine:
  every 30s: PollBatchStatus(batchID) for each in-progress batch
  → SUCCEEDED: record operationID in `succeeded`, clear from `inFlight`
               (removes: this is what lets Delete() converge — see below)
  → FAILED:    invoke FailureHandler(operationID, kind, detail), clear from `inFlight`
               (so a retry is able to re-send the operation)
  → expiry:    batches older than 2 h (maxBatchAge), or unknown at the broker
               (3 consecutive empty status responses), are dropped; remaining
               items are treated as FAILED ("batch expired at broker")
```

**First-item blocking window:** `collectBatch` blocks indefinitely for the first item and only
then opens the 10 s window. An idle batcher never spins, and the first request of a burst is
delayed by at most one full window.

**Broker ACK unblocks callers.** For adds, the real ProviderID is resolved later by
`NodeProviderIDController` when the node joins — broker SUCCEEDED is not on the registration
critical path. For removes, `Delete()` returns `nil` at ACK, but Karpenter's termination
controller re-invokes it **every 5 s** and releases the Node's finalizer only on a
`NodeClaimNotFoundError`. The batcher's `Succeeded(operationID)` is the convergence signal:
once the poller records `<uid>-remove` as SUCCEEDED, the next `Delete()` reports the instance
gone and termination completes.

> **Node existence cannot be the completion signal.** During termination Karpenter holds the
> Node object alive with **its own** finalizer, which it drops only once `Delete()` reports the
> instance gone. "Is there still a Node with this providerID?" is therefore circular and never
> converges. `Delete()` used to return `nil` forever after ACK, leaving NodeClaims and Nodes
> `Terminating` indefinitely.

**In-flight suppression.** Because `Delete()` is re-invoked every 5 s for the whole life of a
removal, every ACKed-but-not-terminal operationID is held in an `inFlight` set; re-enqueueing one
resolves the caller immediately instead of sending a duplicate batch. Without it, each reconcile
pushed another remove batch onto the broker's **global** 64-slot queue.

**Multi-waiter fan-out.** `pending` maps an operationID to a **slice** of result channels, so
every caller waiting on the same operation is resolved together. With one buffered channel per
operation, only the first waiter could consume the result and the rest blocked until their
context was cancelled.

**FAILED feedback:** `SetFailureHandler` registers a callback invoked by the status poller for
every terminal FAILED result (and for items of expired batches). The wired handler
(`cloudprovider.NewBatchFailureHandler`, registered in `main.go` before `StartBatcher`) deletes
the NodeClaim whose UID matches a failed **add** operationID — but only while it still carries a
pending ProviderID and has no deletion timestamp — so a failed provision is replaced in seconds
instead of waiting out the 60-minute registration timeout (§2). **Remove** failures are logged
only: Karpenter retries `Delete()` as long as the node object exists.

**Cancellation:** `Cancel(operationID)` sends a fire-and-forget `cancel_ops` to the broker
(10 s timeout, errors logged only). Only operations still queued (ACCEPTED) at the broker are
transitioned — via a Redis compare-and-set — to FAILED `"cancelled by client"`; RUNNING and
terminal operations are untouched. `Delete()` uses this for NodeClaims whose node never joined.

**Deduplication:** `Enqueue`/`EnqueueRemove` are idempotent per `operationID` (= NodeClaim UID,
or UID + `"-remove"` for removes). If `Create()`/`Delete()` is cancelled and retried with the same
operationID, the retry registers an **additional** result channel joining the operationID's waiter
slice (rather than enqueuing a duplicate batch item); all waiters are resolved together when the
result arrives.

**Queue full:** Client side, if the 256-item queue is full, an error is sent immediately to
`resultCh` and the operationID is removed from `pending`, allowing the caller to retry cleanly.
Broker side, a full batch queue is rejected in-band with `KarpenterBatchRejected` (surfaced as
`ErrBatchRejected`) and the stream stays alive.

Detailed design docs: [implementation/batch-node-addition.md](implementation/batch-node-addition.md)
and [implementation/batch-node-removal.md](implementation/batch-node-removal.md).

---

## 5. Bug Fix: Create() Was Blocking for 10 Minutes

**Problem:** `sendBatch()` originally only sent to `resultCh` when `PollBatchStatus` returned
`SUCCEEDED` or `FAILED` — roughly 10 minutes after the batch was sent (broker provisions the node).
During those 10 minutes, `CloudProvider.Create()` was blocked, `Status.ProviderID` remained empty,
and `cluster.Synced()=false` halted the entire provisioning loop.

**Fix:** `sendBatch()` now unblocks each `resultCh` immediately after `SendBatch()` succeeds (broker
ACK). The `pollBatch()` status poller continues to run but only logs outcomes — it no longer signals
`Create()` callers.

```go
// In sendBatch(), after b.client.SendBatch() succeeds:
for _, item := range batch {
    item.resultCh <- BatchResult{}   // unblock Create() immediately on broker ACK
    b.removePending(item.operationID)
}
```

`pollBatch()` SUCCEEDED/FAILED cases now just log — the `item.resultCh` sends were removed since
`resultCh` has already been consumed at this point.

> **Update:** the poller is no longer log-only.
> - Terminal **FAILED** results invoke a registered `FailureHandler` that deletes the matching
>   still-pending NodeClaim (see §4), so a failed provision recovers in seconds instead of via the
>   60-minute registration timeout.
> - Terminal **SUCCEEDED** results are **recorded** in the batcher's `succeeded` set. For adds this
>   is not on the registration critical path (the node joining is). For **removes** it is the whole
>   ball game: it is what lets `Delete()` return `NodeClaimNotFoundError` and release the Node's
>   termination finalizer — see §12.

**Impact:**
- `CloudProvider.Create()` returns in seconds instead of ~10 minutes
- `Status.ProviderID = "rafay://pending/<uid>"` is set seconds after NodeClaim creation
- `cluster.Synced()` becomes `true` within seconds, unblocking the provisioning loop

---

## 6. Synthetic Pending ProviderID

Because the real node ProviderID (assigned by the cloud/broker) is not known until the node
actually joins the cluster, `CloudProvider.Create()` returns a **synthetic pending ID**:

```
rafay://pending/<nodeclaim-uid>
```

This is a valid non-empty ProviderID that:
- Satisfies `cluster.Synced()` (non-empty string)
- Is recognizable by `CloudProvider.Get()` (returns the NodeClaim as "exists, provisioning")
- Is recognizable by `CloudProvider.Delete()` (triggers best-effort broker cancellation)

The synthetic ID is later replaced by `NodeProviderIDController` when the real node joins.

`NodeForNodeClaim()` in `utils/nodeclaim/nodeclaim.go` does an exact `spec.providerID` field-index
match. While the NodeClaim has the synthetic pending ID, no real node matches it, so `Registration`
reconciler returns `NodeNotFound` repeatedly — this is expected and correct.

---

## 7. NodeProviderIDController

`pkg/controllers/nodeproviderid/controller.go` — watches for new Nodes and patches the NodeClaim
`Status.ProviderID` from synthetic → real.

**Triggers:**
- New Node with `nodepoolname` + `sku_name` labels appears

**Logic:**
1. Receive a single NodeClaim from the watch; skip it unless it carries the `rafay://pending/`
   prefix (`controller.go:139`). List all NodeClaims uncached (`apiReader`) and record the
   ProviderIDs of the non-pending ones as `usedIDs`.
2. List Nodes by the `nodepoolname` label, then match `sku_name` **in code** (it cannot be a
   server-side selector — a NodeClaim may accept more than one value; see below)
3. Find the NodeClaim whose creation timestamp is before the node's creation timestamp
4. Verify the node is not already claimed by another NodeClaim
5. Patch: `NodeClaim.Status.ProviderID = node.Spec.ProviderID`

**How `sku_name` is matched (`NodeClaimSKUs`).** A node's `sku_name` label is the **platform's**
SKU / instance-type name, *not* the RafayNodeClass name. A node matches if its `sku_name` equals
either:

- the NodeClaim's `node.kubernetes.io/instance-type` label — the instance type `Create()`
  actually selected, stamped onto the NodeClaim via `requirementsToLabels`; this is the
  authoritative match, **or**
- the NodeClaim's `spec.nodeClassRef.name` — back-compat with the legacy single-SKU convention
  where a `RafayNodeClass` is named after its one SKU.

Comparing `sku_name` against `NodeClassRef.Name` **alone** meant that any `RafayNodeClass` listing
several `instanceTypes` — exactly the configuration cheapest-fit selection exists to serve — could
never resolve its node: the NodeClaim churned out on the hourly registration timeout, orphaning the
machine that had in fact been provisioned. `CloudProvider.findNodeProviderID` applies the same rule.

**`MaxConcurrentReconciles=1`** — prevents two NodeClaims from racing to claim the same node.

**Uses `apiReader` (uncached client)** to read NodeClaim list — prevents a stale-cache race where
two goroutines see the same NodeClaim and both try to patch it.

Once the patch succeeds, `Registration.Reconcile` can now find the node via exact `spec.providerID`
match and proceed with registration.

---

## 8. Node Initialization Sequence

Full chain from `Create()` return to `karpenter.sh/initialized=true` on the Node:

```
1. CloudProvider.Create() returns
   └─ NodeClaim.Status.ProviderID = "rafay://pending/<uid>"
   └─ NodeClaim.Status.Capacity = selected instance capacity
   └─ NodeClaim.Status.Allocatable = selected instance allocatable
   └─ NodeClaim condition: Launched=True

2. Real node joins cluster (rafay agent / kubelet registers)
   └─ Node object appears with nodepoolname + sku_name labels

3. NodeProviderIDController patches NodeClaim
   └─ NodeClaim.Status.ProviderID: "rafay://pending/<uid>" → "rafay://<real-id>"

4. Registration reconciler runs (lifecycle/registration.go)
   └─ NodeForNodeClaim() finds node via exact spec.providerID match
   └─ Removes UnregisteredNoExecuteTaint from node
   └─ Sets node.Labels["karpenter.sh/registered"] = "true"
   └─ NodeClaim condition: Registered=True

5. Initialization reconciler runs (lifecycle/initialization.go)
   └─ Checks: Registered=True
   └─ Checks: node.Status.Conditions[NodeReady] == True
   └─ Checks: all spec.startupTaints removed from node
   └─ Checks: all KnownEphemeralTaints removed
   └─ Checks: all requested extended resources registered (non-zero Allocatable)
   └─ Patches node.Labels["karpenter.sh/initialized"] = "true"
   └─ NodeClaim condition: Initialized=True
```

After step 5, kube-scheduler sees the node as fully schedulable and binds pending pods to it.

---

## 9. Bug Fix: Runaway NodeClaim Creation

**Symptom observed:** 2 pending pods → 10+ NodeClaims created (2 new ones every ~15 seconds),
all stuck at `rafay://pending/<uid>` ProviderID.

**Root cause:** `CloudProvider.Create()` was setting `Status.Capacity` but NOT `Status.Allocatable`.

The scheduler uses this chain to decide if a pod fits an in-flight node:

```
ExistingNode.CanAdd(pod)
  └─ resources.Fits(pod.requests, n.remainingResources)
       └─ n.remainingResources = Available() - daemonResources
            └─ Available() = Allocatable() - PodRequests()
                 └─ Allocatable() = NodeClaim.Status.Allocatable   ← was EMPTY
```

`Status.Capacity` is used for `cluster.remainingResources` (NodePool-level limit tracking) —
this was correct. But `Status.Allocatable` is what `ExistingNode.CanAdd()` checks when deciding
whether a pod fits a specific in-flight node. With it empty:

- Every pending pod failed `resources.Fits()` against every in-flight NodeClaim
- `addToExistingNode` rejected all in-flight nodes
- `addToNewNodeClaim` created a fresh NodeClaim for each pod every provisioning pass
- Since `Create()` now returns in seconds (see §5), the loop ran very fast → ~2 new NodeClaims per
  `batchWindow` (10s) → runaway growth

**Fix** in `pkg/cloudprovider/cloudprovider.go`:

```go
out.Status.Capacity = selected.Capacity
out.Status.Allocatable = selected.Allocatable()   // ← added
```

`selected.Allocatable()` = `Capacity - Overhead.Total()`. Since our `InstanceTypeOverhead` is zero
(private cloud, no system-reserved subtraction), this equals `Capacity` — but it must be set in
`Status.Allocatable` for the scheduler to see it.

**Why this happens without the fix:**
The `NodeClaim.Status.Capacity` and `NodeClaim.Status.Allocatable` are separate fields. Karpenter
uses `Capacity` for NodePool-level accounting and `Allocatable` for per-node pod-fitting. Both must
be populated by `CloudProvider.Create()` for in-flight NodeClaims to be usable by the scheduler.

---

## 10. Pod Scheduling: What Karpenter Does vs kube-scheduler

These two systems have distinct, non-overlapping responsibilities:

| Concern | Karpenter | kube-scheduler |
|---|---|---|
| Decide which nodes to create | ✓ | |
| Create NodeClaim CRs | ✓ | |
| Provision real nodes | ✓ (via broker) | |
| Bind pods to nodes | | ✓ |
| Set `pod.spec.nodeName` | | ✓ |

Karpenter's `scheduler.Solve()` runs a **simulation** of which pods would fit where. This determines
what NodeClaims to create. The actual pod→node binding is done entirely by kube-scheduler once the
real node becomes schedulable.

The in-memory `cluster.podToNodeClaim` map tracks which NodeClaim Karpenter simulated a pod onto.
It is used for metrics and deduplication within a scheduling pass. It does NOT gate provisioning and
is cleared when a pod gets a scheduling error in the next pass or when the pod is deleted.

---

## 11. What Happens When a NodeClaim Is Ready but Pods Are Still Pending

When `Initialized=True` (node has `karpenter.sh/initialized=true` label):
- `UnregisteredNoExecuteTaint` has been removed
- All startup taints removed
- Node is `Ready=True`

**kube-scheduler is event-driven.** The taint removal triggers it to re-evaluate all `Unschedulable`
pods. If a pod fits the node, kube-scheduler binds it → pod leaves Pending state.

If kube-scheduler **cannot** schedule the pod to the ready node (wrong zone, wrong instance type,
resource mismatch, affinity rules):
- Pod remains `PodScheduled=Unschedulable` → `IsProvisionable()=true`
- Karpenter provisioning loop re-runs (now `Synced()=true`)
- `addToExistingNode` tries the Initialized node — if it fits, kube-scheduler will also bind it
- If no existing node fits → new NodeClaim created with correct requirements

**Self-healing:** The Liveness controller deletes NodeClaims that don't register within 60 minutes.
If node provisioning silently fails (broker accepts but node never joins), the NodeClaim is
automatically deleted and the provisioning loop creates a replacement.

---

## 12. Bug Fix: Delete() Never Converged — Nodes Stuck Terminating Forever

**Problem:** Karpenter core's `awaitInstanceTermination`
(`../karpenter/pkg/controllers/node/termination/controller.go`) calls `cloudProvider.Delete()` on
**every** reconcile and releases the Node's termination finalizer **only** when `Delete()` returns a
`NodeClaimNotFoundError`. Anything else — including `nil` — requeues after 5 seconds:

```go
deleteErr := c.cloudProvider.Delete(ctx, nodeClaim)
if cloudprovider.IgnoreNodeClaimNotFoundError(deleteErr) != nil {
    return reconcile.Result{}, deleteErr
}
if !cloudprovider.IsNodeClaimNotFoundError(deleteErr) {
    return reconcile.Result{RequeueAfter: 5 * time.Second}, nil   // ← forever, if Delete returns nil
}
```

`Delete()` always returned `nil` after broker ACK, so it never produced the one error that ends the
loop. **NodeClaims and Nodes stayed `Terminating` indefinitely.** Worse, each 5 s reconcile called
`EnqueueRemove` again, pushing another remove batch onto the broker's **global** 64-slot queue.

**Fix — three parts:**

1. **A real completion signal.** The status poller records every operationID the broker reports
   SUCCEEDED (`NodeBatcher.Succeeded`), and `CloudProvider.Delete` returns `NodeClaimNotFoundError`
   once the remove op `<uid>-remove` has SUCCEEDED.
2. **In-flight suppression.** ACKed-but-not-terminal operationIDs are held in an `inFlight` set;
   re-enqueueing one resolves immediately instead of sending a duplicate batch, so the 5 s retry
   loop costs nothing at the broker.
3. **Multi-waiter fan-out.** `pending` became `map[string][]chan BatchResult` so all callers waiting
   on one operationID are resolved together (previously only one waiter got the single buffered
   result; the others blocked until their context was cancelled).

**Why node existence cannot be the signal.** The obvious alternative — "return NotFound once no Node
carries this providerID" — is circular and never converges: during termination **Karpenter itself
holds the Node object alive with its own finalizer**, and only drops that finalizer once `Delete()`
reports the instance gone. The broker's SUCCEEDED result is an *external* signal, which is exactly
why it works.

> **Broker-side companion fix.** Because `Delete()` re-sends the same deterministic `<uid>-remove`
> operationID for as long as Karpenter retries, the broker's terminal SUCCEEDED record has to
> outlive that retry horizon. SUCCEEDED records are now long-lived **tombstones**
> (`karpenterBatchSucceededTTL` = **24 h**, refreshed on each duplicate send). Previously they
> expired with the 60-minute RUNNING TTL, so a re-sent remove opID looked brand new, was ACCEPTED
> again, and the catalog was **decremented a second time** — retiring an extra healthy machine,
> once per hour per terminating NodeClaim. FAILED records deliberately keep the short TTL: a FAILED
> op **must** stay retryable.

---

## 13. Bug Fix: List() Always Returned Empty — GC Deleted Healthy NodeClaims

**Problem:** `listNodesFromKube` filtered Kubernetes Nodes by comparing the first segment of their
`rafay://` ProviderID against `RAFAY_CLUSTER_ID`. But the real, platform-stamped format is:

```
rafay://<nodepoolname>/<sku_name>/<hostname>
e.g. rafay://worker-pool-amd/oci-inst/host-w1-e6a5c
```

The first segment is the **node pool**, not a cluster ID — so the comparison could never match and
`CloudProvider.List()` always returned an empty slice. That does not fail safe: Karpenter's core
garbage-collection controller **deletes any `Registered` NodeClaim whose ProviderID is absent from
`List()`**, so any node that briefly went `NotReady` had its NodeClaim torn out from under it.

**Fix:** the cluster-ID filter is **removed** — every Node carrying a `rafay://` ProviderID is
listed. `ParseProviderID`'s first return value was renamed `clusterID` → `nodePoolName` to stop the
misreading at the source.

**Related: the `sku_name` mismatch.** The same misunderstanding broke Node↔NodeClaim matching, which
compared a node's `sku_name` label against `NodeClaim.Spec.NodeClassRef.Name`. `sku_name` is the
**platform's** SKU/instance-type name, so any `RafayNodeClass` listing **multiple** `instanceTypes`
— exactly what cheapest-fit selection exists to serve — never resolved its node: hourly
registration-timeout churn, plus an orphaned machine each cycle. A node now matches if its
`sku_name` equals **either** the NodeClaim's `node.kubernetes.io/instance-type` label (the selected
instance type) **or** the NodeClass name (legacy single-SKU convention) — see §7. Both
`nodeproviderid.Reconcile` and `cloudprovider.findNodeProviderID` apply this rule, and the Node list
is now selected by `nodepoolname` only, with `sku_name` filtered in code.

---

## 14. Adopting Pre-Existing Nodes Without Provisioning One

A Rafay cluster arrives with worker nodes already built from its catalog (`noOfSku: 3`). They carry
`nodepoolname` / `sku_name` labels but no NodeClaim, and Karpenter counts **only** NodeClaims — so
`NodePool.status.nodes` reads `0`, its `limits` are computed as if the cluster were empty, and
`StateNode.ValidateNodeDisruptable` refuses to consolidate them (`"node isn't managed by
karpenter"`, statenode.go). The fix is to create a NodeClaim per node. The interesting part is doing
that **without** ordering a second machine.

### Why a plain NodeClaim create provisions a node

`lifecycle.Launch.Reconcile` gates entirely on one condition:

```go
if cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched); !cond.IsUnknown() {
    ...                       // already launched (or failed) — nothing to do
    return reconcile.Result{}, nil
}
...
created, err = l.launchNodeClaim(ctx, nodeClaim)   // → CloudProvider.Create()
```

A freshly created NodeClaim has no conditions, so `Launched` is `Unknown` and `Create()` **is
called**. Hand-creating a NodeClaim for an existing node therefore asks the broker for another one.

### Why the marker is an annotation, not a status pre-patch

The obvious fix — create the NodeClaim, then immediately patch `status.providerID` and
`Launched=True` — does not hold. `status` is a separate subresource, so it is a second round trip,
and `nodeclaim.lifecycle` watches NodeClaims: between the create and the status patch it can
reconcile the object, see `Launched=Unknown`, and call `Create()`. The window is small and the
consequence is a stray machine plus a catalog row that no longer matches the cluster.

`karpenter.rafay.io/adopted-provider-id` is set **on the object being created**, so it is present in
the very first `Launch` reconcile no matter how the two controllers interleave. `Create()` reads it
and returns without touching the batcher:

```go
if adoptedID := nodeClaim.Annotations[AdoptedProviderIDAnnotationKey]; adoptedID != "" {
    out := nodeClaim.DeepCopy()
    out.Status.ProviderID = adoptedID
    out.Status.Capacity, out.Status.Allocatable = selected.Capacity, selected.Allocatable()
    return out, nil            // no broker call
}
```

Karpenter's own machinery then completes the lifecycle: `Launch` sets `Launched=True` from the
returned object, `Registration` matches the Node by `status.providerID` and sets `Registered=True`,
`Initialization` sets `Initialized=True`. It also makes the path restart-safe — a controller restart
re-reads the annotation from etcd, where a lost in-memory "already adopted" flag would not survive.

### Three Karpenter behaviours that shape the adopted NodeClaim

| Karpenter code | What it does | Consequence for adoption |
|---|---|---|
| `Registration.syncNode` → `node.Labels = lo.Assign(node.Labels, nodeClaim.Labels)` | Copies the NodeClaim's labels **onto the Node** | A label inferred from the SKU that the node does not have gets written to a running node. A `RafayNodeClass` with no `zone` yields the synthetic zone `default`, which would overwrite real topology — so well-known labels are read off the Node, and topology labels are **omitted** when it has none. `Requirements.Compatible` skips well-known keys the NodeClaim does not define and `Intersects` only compares keys present in both, so an absent zone is treated as unconstrained rather than as a mismatch. |
| `Registration.syncNode` → `Taints.Merge(nodeClaim.Spec.Taints)` | Merges the NodeClaim's taints onto the Node | Copying the pool's taints onto an adopted node would apply a `NoExecute` taint to a node already running pods and evict them. Taints are dropped from adopted NodeClaims; the platform already applied the catalog's taints to the nodes it built. |
| `Drift.areStaticFieldsDrifted` returns `""` unless **both** objects carry `karpenter.sh/nodepool-hash` | Static drift needs the hash on the NodeClaim | Omitting the hash means a later NodePool template edit does not mark every adopted node drifted and roll nodes the operator never asked Karpenter to create. `areRequirementsDrifted` and `instanceTypeNotFound` are still live, which is why adoption pre-checks both and skips a node that would fail them. |

### Why adoption waits for `NodeReady`

`Liveness.Reconcile` reaps a NodeClaim on two timers only — `launchTimeout` (5 min, if `Launched`
never went True) and `registrationTimeout` (60 min, if `Registered` never went True):

```go
registered := nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered)
if registered.IsTrue() {
    return reconcile.Result{}, nil      // ← nothing below ever runs again
}
```

There is **no** initialization timeout. An adopted NotReady node registers fine (Registration only
needs a Node matching `status.providerID`), so `Registered=True` and Liveness returns early forever,
while `Initialization` refuses to advance:

```go
if nodeutils.GetCondition(node, corev1.NodeReady).Status != corev1.ConditionTrue {
    nodeClaim.StatusConditions().SetUnknownWithReason(v1.ConditionTypeInitialized, "NodeNotReady", ...)
    return reconcile.Result{}, nil
}
```

The claim parks at `Registered=True` / `Initialized=Unknown`, and nothing reaps it short of
`spec.expireAfter` — 720h from the pool template, so a node that never comes up leaves a phantom
NodeClaim for 30 days. Garbage collection does not help: it only considers claims **absent** from
`CloudProvider.List()`, and `listNodesFromKube` returns every Node carrying a `rafay://` providerID —
which adoption has just stamped on this one.

It is not inert while parked either. `StateNode.Capacity()` fills in from the **NodeClaim** while
uninitialized:

```go
func (in *StateNode) Capacity() corev1.ResourceList {
    if !in.Initialized() && in.NodeClaim != nil {
        if in.Node != nil {
            ret := lo.Assign(in.Node.Status.Capacity)
            for resourceName, quantity := range in.NodeClaim.Status.Capacity {
                if resources.IsZero(ret[resourceName]) {   // ← SKU fills any resource the node doesn't report
                    ret[resourceName] = quantity
```

So for a node reporting real figures it is mostly the node's; but for one whose kubelet never
reported, **or whose Node object is deleted while the claim lives** — `Cluster.cleanupNode` keeps the
StateNode and nils out its `Node` rather than dropping it — the SKU's declared CPU/memory is the
*whole* capacity, and the pool's `status.resources` and `limits` are charged for a machine that is
not there.

Gating on Ready avoids all of it, and costs nothing: the controller's Node watch fires on the status
update that flips the condition, so the node is adopted seconds later. The gate is one-way — a node
that is Ready when adopted and goes NotReady afterwards keeps its claim, because by then it is a
normal managed node and Karpenter's **disruption** path owns it. Not its node-repair path: that
controller is only registered when `CloudProvider.RepairPolicies()` is non-empty, and this provider
returns `nil`.

### Why an adopted node cannot be one a scale-out is waiting for

`nodeproviderid` binds a joined node to a pending NodeClaim only when
`node.CreationTimestamp.After(nodeClaim.CreationTimestamp)` — the guard that stops a pending claim
from grabbing a pre-existing node (§7). Adoption is the exact complement: it skips any node **newer**
than a pending claim for the same pool+SKU. Adopting such a node would leave that claim unresolved
until `registrationTimeout` (60 min) deleted it. The two controllers partition every node in a pool
between them, and `NodeClaimSKUs` is shared so they cannot disagree about which SKUs a claim accepts.

---

## Key Files

| File | Purpose |
|---|---|
| `pkg/rafay/batcher.go` | NodeBatcher: collects Create()/Delete() calls, sends add/remove batches to broker, polls status, FAILED feedback, cancel |
| `pkg/rafay/batch_stream.go` | Short-lived gRPC stream helpers: sendBatch, sendBatchRemove, pollBatchStatus, cancelOps |
| `pkg/rafay/brokerclient.go` | gRPC connection management; 30 s per-RPC deadlines |
| `pkg/cloudprovider/cloudprovider.go` | CloudProvider: Create/Delete/Get/List; NewBatchFailureHandler |
| `pkg/controllers/nodeproviderid/controller.go` | Patches real ProviderID when node joins |
| `pkg/controllers/nodeadoption/controller.go` | Creates a NodeClaim per pre-existing worker node in a pool; fills in empty `spec.providerID` |
| `karpenter/pkg/controllers/state/cluster.go` | `Synced()`, `UpdateNodeClaim()`, `podToNodeClaim` |
| `karpenter/pkg/controllers/provisioning/provisioner.go` | Provisioning loop, `Reconcile()` |
| `karpenter/pkg/controllers/provisioning/scheduling/scheduler.go` | `Solve()`, `addToExistingNode`, `addToInflightNode` |
| `karpenter/pkg/controllers/provisioning/scheduling/existingnode.go` | `ExistingNode.CanAdd()` |
| `karpenter/pkg/controllers/state/statenode.go` | `Capacity()`, `Allocatable()`, `Available()` |
| `karpenter/pkg/controllers/nodeclaim/lifecycle/launch.go` | Calls `CloudProvider.Create()` |
| `karpenter/pkg/controllers/nodeclaim/lifecycle/registration.go` | Removes taint, sets Registered=True |
| `karpenter/pkg/controllers/nodeclaim/lifecycle/initialization.go` | Sets initialized=true label |
| `karpenter/pkg/controllers/nodeclaim/lifecycle/liveness.go` | Timeout/deletion of stuck NodeClaims |

**Deleted files:** `pkg/rafay/karpenter_node_stream.go` and `pkg/brokerproto/` (the old single-op
`KarpenterNodeService` client path with `AddNodes`/`RemoveNode`) no longer exist — all node
add/remove traffic goes through the NodeBatcher and `KarpenterBatchService.BatchStreamOperations`.
