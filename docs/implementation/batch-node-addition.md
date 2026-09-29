# Batch Node Addition

> **Status note:** this is the original design document for the batch-add path, kept for the
> reasoning; several details have since moved on. `Create()` is unblocked by the broker **ACK**
> (`KarpenterBatchAccepted`), not by a SUCCEEDED status poll; terminal FAILED results are fed back
> through a `FailureHandler` that deletes the still-pending NodeClaim so Karpenter reprovisions
> immediately (a permanent refusal — `pool at maximum`, `pool not found`, `pool sku mismatch`,
> `pool not auto-scaling`, `pool precondition` — also holds the NodePool back for a cooldown
> first); the status poller has no initial delay; the broker's queue is **per edge** and its
> processing is split into a `KarpenterBackend` per cluster type. The batcher also handles node
> **removal** and **cancellation** — see [batch-node-removal.md](batch-node-removal.md). For the
> current batcher behavior see
> [karpenter-rafay-internals.md §4](../karpenter-rafay-internals.md#4-nodebatcher-batch-send-architecture);
> for current numbers and names, `docs/architecture.md` is authoritative.

## Overview

Node provisioning through the Rafay PaaS layer takes ~10 minutes per batch, and can take up to
60 minutes (the platform builds the whole machine). The broker also enforces that only one
catalog update runs at a time **per edge** (one queue and one processor goroutine per cluster,
different clusters in parallel; a per-cluster Redis lock serializes broker replicas). Sending
individual node-add requests one-by-one would therefore serialize all provisioning and leave
Karpenter pods pending far longer than necessary.

The batch node addition system collects individual `Create()` calls from Karpenter, groups them
into batches of up to 10 nodes (or a 10-second collection window), and sends a single request to
edge-broker. The broker queues the batch on that edge's FIFO, applies a single catalog increment
per `{pool, SKU}` group in each batch, and polls for completion before processing the edge's
next batch. From Karpenter's perspective `Create()` blocks only until the broker ACKs.

---

## Files Changed

### Shared types — `edge-common/pkg/edge/v1/edge_karpenter_batch.go`

Added to both the `edge-common` source repo and its copy in `edge-broker/vendor/`.

New `KarpenterBatchService` gRPC service with hand-written proto-compatible Go types:

| Type | Purpose |
|------|---------|
| `KarpenterBatchNodeAddItem` | One node in a batch — operationId, cluster/project/pool/sku |
| `KarpenterBatchNodeAddRequest` | Batch payload: batchId + nodes list |
| `KarpenterBatchAccepted` | Broker's immediate acknowledgement (returns batchId) |
| `KarpenterBatchStatusPoll` | Client request to poll status for a batchId |
| `KarpenterBatchStatusResponse` | Per-node ACCEPTED / RUNNING / SUCCEEDED / FAILED results |
| `KarpenterBatchStreamClientToBroker` | Stream message: `batch_add` or `status_poll` |
| `KarpenterBatchStreamBrokerToClient` | Stream message: `batch_accepted` or `batch_status` |

Full gRPC boilerplate: `KarpenterBatchServiceServer` / `KarpenterBatchServiceClient` interfaces,
`RegisterKarpenterBatchServiceServer`, stream send/recv wrappers, service descriptor.

---

### edge-broker

#### `pkg/context/karpenter_workspace.go` — new helpers

| Function | Purpose |
|----------|---------|
| `applyWorkerNodeCatalogDeltas(raw, deltas, opts)` (originally `bulkIncrementWorkerNodeCatalogValue`) | Single-pass multi-pool/sku catalog mutation. Accepts a `[]PoolSkuDelta` so all groups in a batch update the JSON in one call before Apply+Publish; reports per-row outcomes (applied / trimmed to `maxNodeCount` / refused — no row, other SKU, not opted in, below the minimum) and never appends a row. |
| `WorkerHostnamesAboveIndexFromCSV(nodesCSV, minIndex)` (+ `…WithPrefix`) | Returns all worker hostnames whose last `-w<n>-` index is strictly greater than `minIndex` — used to identify newly provisioned nodes after a batch completes. The `WithPrefix` variants strip the cluster name first so a cluster name containing such a marker cannot hide every worker. |
| `WorkerHostnamesAboveIndexFromStatus(ws, minIndex)` | Wraps the CSV function against compute instance status output. |
| `MaxWorkerIndexFromCSV(nodesCSV)` (+ `…WithPrefix`), `WorkerCountFromCSV` (+ `…WithPrefix`) | Highest worker index / live worker count currently present — snapshotted before the batch to determine the baseline (the live count is the base of every change on a single-row catalog). |
| `MaxWorkerIndexFromStatus(ws)` | Wraps `MaxWorkerIndexFromCSV` against status output. |

#### `pkg/context/karpenter_batch_stream.go` — new file

**`BatchStreamOperations`** — implements `rep.edge.v1.KarpenterBatchService.BatchStreamOperations`.
The stream is long-lived; the client sends alternating `batch_add` and `status_poll` messages.

- `handleBatchAdd`: classifies the request before writing anything (≤ 64 nodes, the edge's queue
  not full, the batch id not owned by another edge — otherwise `KarpenterBatchRejected` in-band
  with nothing written), writes all operation IDs to Redis (state = ACCEPTED, stamped with
  `edge_id` / `project_id` / `cluster_id`), stores the `batchId → []operationId` mapping,
  enqueues the batch onto **this edge's** FIFO in `KarpenterBatchQueue`, replies with
  `KarpenterBatchAccepted`.
- `handleBatchStatusPoll`: reads the batch record to get operation IDs, reads each per-node Redis
  record, and replies with `KarpenterBatchStatusResponse`.

**`KarpenterBatchQueue`** — one in-memory FIFO per edge id (capacity 16 waiting batches,
`karpenterBatchQueuePerEdgeMax`; the batch in flight is not counted), each drained by its own
processor goroutine, started lazily on the edge's first `Enqueue` and reaped after 1 h idle
(`karpenterBatchEdgeIdleReap`). There is no global slot count any more.

**`RunProcessor(ctx, ebc)`** — the supervisor goroutine; per-edge processors call
`ebc.processBatch()` one batch at a time within their edge, different edges in parallel. Runs for
the lifetime of the process; `EdgeBrokerContext.Stop(ctx)` cancels it and waits for every edge's
in-flight batch on SIGTERM.

**`processBatch` / `processBatchAdd`** — takes the per-cluster Redis lock
(`/edge/karpenter/lock/<edgeID>`, so two broker replicas never overlap on one cluster), CASes
every still-ACCEPTED node to RUNNING (TTL → 2 h), groups items by `{pool, sku}`, builds the
deltas, delegates to the edge's `KarpenterBackend.AddNodes` under the **90-minute**
`karpenterBatchAddTimeout`, then writes SUCCEEDED (with provider ID) or FAILED for every
operation ID in the batch — FAILED with a permanent-refusal prefix for ops the platform cannot
ever accept, SUCCEEDED `committed; …` for a failure after the platform commit. A panic in a batch
is recovered (its RUNNING ops become FAILED `batch processor panicked: …; retry`) and the
processor keeps serving.

#### `pkg/context/karpenter_backend_oneclick.go` — `oneclickKarpenterBackend.AddNodes`

(The former `karpenter_node_lifecycle.go` / `ApplyBatchKarpenterStreamNodeAdd` were replaced by the
`KarpenterBackend` split: `karpenter_backend.go` is the interface, `karpenter_backend_oneclick.go`
edits the compute-instance catalog, `karpenter_backend_firstclass.go` edits the MKS `Cluster`'s
`scaling.desired`.)

1. Resolve edge → workspace compute instance (searching only the projects the edge record names;
   the client's `project_id` hint never widens the search) + tenant IDs.
2. Snapshot the worker hostnames from `status.output` before the increment (the live count is the
   base of every change on a single-row catalog).
3. Call `applyWorkerNodeCatalogDeltas` on the `Worker Node SKU` variable — one pass applies all
   pool+sku groups by their respective counts, trimmed to each row's `maxNodeCount`; a row that is
   missing, has another SKU or is not opted in refuses its ops (`pool not found` /
   `pool sku mismatch` / `pool not auto-scaling`). No row is ever appended.
4. `ApplyWorkspaceComputeInstance` + `PublishWorkspaceComputeInstance` (once for the whole batch).
   `Apply`+`Publish` success is the **commit point**; an ambiguous transport error on either is
   resolved by re-reading the instance. A busy instance (`cannot be updated in status …`) is waited
   out (poll every 30 s), never failed.
5. Poll every 30 s until a terminal state, bounded only by the batch context (90 min).
6. On `SUCCESS`: diff the worker hostnames (post − pre) and assign them to operation IDs in order.
   Fewer hostnames than nodes applied ⇒ the missing ops are refused `pool precondition` and their
   increment is taken back; hostnames unknown ⇒ synthetic
   `rafay://<pool>/<sku>/synthetic-<8 hex>` IDs. On a failed run the increment for the machines
   that were not built is restored so the reprovision is a single increment.
7. Return `map[operationID]providerID` (best effort — the provider binds NodeClaims from Node
   labels, not from this id).

#### `pkg/context/edge.go` — modifications

- Added `batchQueue *KarpenterBatchQueue` field to `EdgeBrokerContext`.
- `NewEdgeBrokerContext` initialises the queue and starts the `RunProcessor` supervisor on a
  cancellable context; `Stop(ctx)` winds it down.
- Added compile-time assertion `var _ v1.KarpenterBatchServiceServer = (*EdgeBrokerContext)(nil)`.

#### `main.go` — modifications

`RegisterKarpenterBatchServiceServer` (and `RegisterKarpenterConfigServiceServer`) called on both
the mTLS listener (port 5448) and the internal plaintext listener (`EDGE_BROKER_INTERNAL_PORT`,
default 5449).

> ⚠️ **Registered on `:5449` does not mean usable on `:5449`.** Every `BatchStreamOperations`
> handler needs the caller's edge id, and that id is derived from the **mTLS client certificate**
> (`common.GetEdgeClientInfo` reads the peer's `credentials.TLSInfo`). A plaintext connection has no
> peer certificate, so the stream is rejected with `ErrorNoClientID` (`"NO CLIENT ID"` — the broker
> maps every `GetEdgeClientInfo` failure to it and does not surface edge-common's internal
> `ErrorInvalidPeer`). Batch operations are therefore only servable over the mTLS listener
> (`5448`) — `EDGE_BROKER_GRPC_INSECURE=true` pointed at `:5449` cannot work for them. It
> previously **panicked** instead: `GetEdgeClientInfo` did an unchecked
> `p.AuthInfo.(credentials.TLSInfo)` assertion and `AuthInfo` is `nil` on a plaintext connection,
> so opening the stream on `:5449` crashed the **whole broker process** (gRPC did not recover
> handler panics) — an unauthenticated DoS. It now fails cleanly, and both servers carry
> panic-recovery interceptors.

---

### karpenter-provider-rafay

#### `pkg/rafay/batcher.go` — new file

**`NodeBatcher`** — collects `batchItem` entries from a buffered channel and drives two goroutines.

`batchSender` goroutine:
- Calls `collectBatch` which blocks for up to `batchWindow` (10 s) or until `maxBatchSize` (10)
  items are queued, whichever comes first.
- Builds `[]*KarpenterBatchNodeAddItem` and calls `BrokerClient.SendBatch`.
- On success: stores the batch in the `inProgress` map (polled on the next ticker tick — there is
  deliberately no initial delay) and immediately loops back to collect the next batch.
- On failure: resolves all items in the batch with an error.

`statusPoller` goroutine:
- Ticks every `pollInterval` (30 s).
- For each in-progress batch, calls `BrokerClient.PollBatchStatus`.
- For each node result: SUCCEEDED → record the operationID (with detail and provider IDs) in the
  `succeeded` set and clear it from `inFlight` (`resultCh` was already resolved at broker ACK; for
  an **add** this is not on the registration critical path — the node joining is — but for a
  **remove** it is what lets `Delete()` converge, see
  [batch-node-removal.md](batch-node-removal.md#delete-must-converge--and-node-existence-cannot-be-how));
  FAILED → invoke the registered `FailureHandler` (which records the op, deletes the still-pending
  NodeClaim, and for a permanent-refusal detail first holds the NodePool back — see
  `docs/architecture.md`, "Pool maximum: three layers") and clear `inFlight` so a retry can
  re-send; ACCEPTED/RUNNING → leave in the pending list; an unrecognised state → logged once,
  treated as in progress.
- Removes fully resolved batches from `inProgress`. Batches older than `maxBatchAge` (3 h — above
  the 60-minute node-add bound and the broker's 90-minute add deadline), or unknown at the broker
  (3 consecutive empty status responses), are dropped with every remaining item treated as FAILED
  (`"batch expired at broker"`).

#### `pkg/rafay/batch_stream.go` — new file

Short-lived gRPC stream helpers (one stream opened per call, closed after response):

| Function | Purpose |
|----------|---------|
| `sendBatch(ctx, conn, nodes)` | Generates a UUID batch ID, sends `KarpenterBatchNodeAddRequest`, returns received `batchId` from `KarpenterBatchAccepted`. |
| `pollBatchStatus(ctx, conn, batchID)` | Sends `KarpenterBatchStatusPoll`, returns `[]*KarpenterBatchNodeResult` from `KarpenterBatchStatusResponse`. |

#### `pkg/rafay/brokerclient.go` — modifications

- Added `batcher *NodeBatcher` field; initialised in `NewBrokerClient`.
- `StartBatcher(ctx)` — launches batcher goroutines; call once from `main`.
- `Batcher()` — accessor for passing the batcher to `CloudProvider`.
- `SendBatch(ctx, nodes)` — calls `sendBatch` via `callBrokerOnce` (no retry, same reasoning as single-node add; the shared connection is never closed on a failure).
- `PollBatchStatus(ctx, batchID)` — calls `pollBatchStatus` via `callBroker` (retries once on `Unavailable`, on the same connection after `ResetConnectBackoff()` — safe for reads).

#### `pkg/cloudprovider/cloudprovider.go` — modifications

- Added `batcher *rafay.NodeBatcher` field to `CloudProvider`.
- `NewCloudProvider` signature updated to accept `batcher`.
- `Create()` replaces the direct `AddNodes` call with:
  ```go
  resultCh := c.batcher.Enqueue(string(nodeClaim.UID), req)
  select {
  case <-ctx.Done(): return nil, ctx.Err()
  case result := <-resultCh: // hydrate NodeClaim and return
  }
  ```
  Karpenter still sees a blocking call; batching is transparent.

#### `cmd/controller/main.go` — modifications

```go
rafayClient.Batcher().SetFailureHandler(cloudprovider.NewBatchFailureHandler(op.GetClient(), op.Manager.GetAPIReader(), poolBackoff, op.EventRecorder))
rafayClient.StartBatcher(ctx)
cp := cloudprovider.NewCloudProvider(op.GetClient(), op.Manager.GetAPIReader(), rafayClient, clusterID, projectID, rafayClient.Batcher(), poolBackoff,
    cloudprovider.WithEventRecorder(op.EventRecorder),
    cloudprovider.WithRemoveSettleWindow(envDurationOrDefault("RAFAY_REMOVE_SETTLE_WINDOW", cloudprovider.DefaultRemoveSettleWindow)))
```

---

## Data Flow

```
Karpenter (N concurrent goroutines)
  │
  ├─ Create(nodeClaim-1) ──┐
  ├─ Create(nodeClaim-2) ──┤  batcher.Enqueue() → block on resultCh
  └─ Create(nodeClaim-N) ──┘
                            │
                  batchSender goroutine
                  waits 10 s or 10 items
                            │
                  BrokerClient.SendBatch()
                  ┌─────────────────────────────────┐
                  │  KarpenterBatchNodeAddRequest    │
                  │  batchId = <uuid>                │
                  │  nodes: [{op1,pool,sku}, ...]    │
                  └─────────────────────────────────┘
                            │  (gRPC bidi stream)
                  edge-broker BatchStreamOperations
                  ├─ classifies the batch (size, edge queue, batch-id owner)
                  ├─ writes all opIDs to Redis (ACCEPTED, 15 min)
                  ├─ enqueues batch onto THIS EDGE's FIFO in KarpenterBatchQueue
                  └─ replies KarpenterBatchAccepted{batchId}
                            │
                  batchSender stores batch in inProgress and
                  resolves every resultCh (broker ACK)
                            │
              Create() unblocks → returns NodeClaim with
              pending providerID "rafay://pending/<uid>" and the
              karpenter.rafay.io/batch-id annotation
              (real ID patched later by NodeProviderIDController);
              batchSender immediately collects the next batch
                            │
              ┌─────────────┴─────────────┐
              │  statusPoller (every 30s)  │
              │  from the next tick on     │
              └─────────────┬─────────────┘
                            │
                  BrokerClient.PollBatchStatus(batchId)
                  → KarpenterBatchStatusResponse
                     {op1: RUNNING, op2: SUCCEEDED(pid), ...}
                            │
              SUCCEEDED → recorded in `succeeded` (adds: registration is
                          driven by the node joining, not by this;
                          removes: this is what lets Delete() converge)
              FAILED    → FailureHandler records the op and deletes the
                          NodeClaim (UID == operationID) if it still has
                          a pending providerID → Karpenter reprovisions
                          immediately (after a pool hold + NodePool event
                          for a permanent refusal)
```

```
edge-broker per-edge processor goroutine (one batch at a time within the edge):
  1. Take the per-cluster redis lock (/edge/karpenter/lock/<edgeID>, 2 h)
  2. CAS all still-ACCEPTED opIDs → RUNNING in Redis (TTL → 2 h)
  3. Group items by {poolName, instanceType}
  4. KarpenterBackend.AddNodes, bounded by karpenterBatchAddTimeout (90 min):
     oneclick: applyWorkerNodeCatalogDeltas (one JSON pass, all groups, trimmed to
               maxNodeCount / refused per row) → ApplyWorkspaceComputeInstance (one PaaS
               call) → PublishWorkspaceComputeInstance (one PaaS call) ← COMMIT POINT
               → poll GetWorkspaceComputeInstance every 30 s until a terminal state
               → diff worker hostnames → assign providerIDs to opIDs
     first-class: desired = live count + n on the MKS Cluster → ApplyCluster ← COMMIT POINT
               → poll GetClusterStatus every 30 s until the hostnames appear
  5. Write SUCCEEDED + providerID (24 h tombstone) — or FAILED with the platform detail /
     a permanent-refusal prefix, or SUCCEEDED "committed; …" past the commit point — per opID
  6. Release the lock; pick the edge's next batch
```

---

## Redis Key Schema

| Key | Format | TTL | Content |
|-----|--------|-----|---------|
| `/edge/karpenter/nodeop/<operationID>` | existing | 15 min while ACCEPTED; **2 h** on RUNNING and on a terminal **FAILED** (except the `batch queue full; try again` / `broker shutting down; retry` / `cluster locked by another broker; retry` writes, which keep the 15-min TTL); **24 h on a terminal SUCCEEDED** (tombstone, refreshed on re-send) | Per-node state (kind, state, detail, provider_ids, **edge_id**, project_id, cluster_id) |
| `/edge/karpenter/batchop/<batchID>` | new | 3 h | `{"operation_ids": ["op1", "op2", ...], "edge_id": "…"}` |
| `/edge/karpenter/lock/<edgeID>` | new | 2 h | owner token `brokerID/batchID/nanos` — one batch per cluster across replicas |

The staged nodeop TTL is deliberate:

- **ACCEPTED is short (15 min)** — a broker restart no longer strands accepted ops (the next
  broker's `sweepOrphanedKarpenterOps` re-queues them within seconds); the TTL now bounds how long
  an op can sit queued behind the same edge's earlier 60–90-minute adds before the polling client
  sees FAILED (`"operation record expired"`) and re-sends.
- **RUNNING is 2 h** (`karpenterBatchAddTimeout` + 30 min) so active processing — up to 90
  minutes for an add, because adding a node can take up to 60 — is never cut short.
- **SUCCEEDED is a long-lived tombstone (24 h), refreshed on every duplicate send.** Clients re-send
  a deterministic operationID for the life of their retry loop, so a terminal record that expired
  would make the next retry look like a **brand-new** operation — it would be ACCEPTED again and the
  catalog delta applied a **second** time. (This bites the remove path hardest, where it retired an
  extra healthy machine; see below.)
- **FAILED is NOT tombstoned** — a FAILED op must stay retryable, and letting it expire is
  equivalent to re-accepting it.

Every record also carries the authenticated **`edge_id`**; status polls and cancels ignore records
belonging to another edge. See the
[TTL strategy and replay protection in batch-node-removal.md](batch-node-removal.md#succeeded-is-a-tombstone--replay-protection)
for the full rationale.

---

## Configuration

No batch-specific environment variables. Batch parameters are compile-time constants in
[pkg/rafay/batcher.go](../../pkg/rafay/batcher.go) and the broker's
`pkg/context/karpenter_batch_stream.go`:

| Constant | Default | Meaning |
|----------|---------|---------|
| `defaultMaxBatchSize` | 10 | Flush after this many items |
| `defaultBatchWindow` | 10 s | Flush after this long even if batch is not full |
| `defaultPollInterval` | 30 s | How often to poll broker for status (first poll on the next tick after ACK — no initial delay) |
| `maxBatchAge` | 3 h | Client-side tracking horizon; never shorten (a node add can take 60 min) |
| `karpenterBatchQueuePerEdgeMax` | 16 | Server-side waiting batches per edge (batches, not nodes) |
| `karpenterBatchMaxNodes` | 64 | Server-side maximum nodes per batch (`batch too large` beyond) |
| `karpenterBatchAddTimeout` / `karpenterBatchRemoveTimeout` | 90 min / 60 min | Server-side processing deadline per batch, settle poll included |
