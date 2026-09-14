# Batch Node Addition

> **Status note:** this is the original design document for the batch-add path; the broker-side
> design is current, but the client-side `resultCh` flow has since changed. `Create()` is now
> unblocked by the broker **ACK** (`KarpenterBatchAccepted`), not by a SUCCEEDED status poll, and
> terminal FAILED results are fed back through a `FailureHandler` that deletes the still-pending
> NodeClaim so Karpenter reprovisions immediately. The batcher also handles node **removal** and
> **cancellation** — see [batch-node-removal.md](batch-node-removal.md). For the current batcher
> behavior see [karpenter-rafay-internals.md §4](../karpenter-rafay-internals.md#4-nodebatcher-batch-send-architecture).

## Overview

Node provisioning through the Rafay PaaS layer takes ~10 minutes per batch. The broker also
enforces that only one `WorkspaceComputeInstance` update runs at a time per cluster. Sending
individual node-add requests one-by-one would therefore serialize all provisioning and leave
Karpenter pods pending far longer than necessary.

The batch node addition system collects individual `Create()` calls from Karpenter, groups them
into batches of up to 10 nodes (or a 10-second collection window), and sends a single request to
edge-broker. The broker queues batches sequentially, applies a single catalog increment per
`{pool, SKU}` group in each batch, and polls for completion before processing the next batch.
From Karpenter's perspective each `Create()` call still blocks until its node is ready.

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
| `bulkIncrementWorkerNodeCatalogValue(raw, deltas)` | Single-pass multi-pool/sku catalog increment. Accepts a `[]PoolSkuDelta` so all groups in a batch update the JSON in one call before Apply+Publish. |
| `WorkerHostnamesAboveIndexFromCSV(nodesCSV, minIndex)` | Returns all worker hostnames whose `-w<n>-` index is strictly greater than `minIndex` — used to identify newly provisioned nodes after a batch completes. |
| `WorkerHostnamesAboveIndexFromStatus(ws, minIndex)` | Wraps the CSV function against compute instance status output. |
| `MaxWorkerIndexFromCSV(nodesCSV)` | Returns the highest worker index currently present — snapshotted before the batch to determine the baseline. |
| `MaxWorkerIndexFromStatus(ws)` | Wraps `MaxWorkerIndexFromCSV` against status output. |

#### `pkg/context/karpenter_batch_stream.go` — new file

**`BatchStreamOperations`** — implements `rep.edge.v1.KarpenterBatchService.BatchStreamOperations`.
The stream is long-lived; the client sends alternating `batch_add` and `status_poll` messages.

- `handleBatchAdd`: writes all operation IDs to Redis (state = ACCEPTED), stores the
  `batchId → []operationId` mapping, enqueues the batch into `KarpenterBatchQueue`, replies with
  `KarpenterBatchAccepted`.
- `handleBatchStatusPoll`: reads the batch record to get operation IDs, reads each per-node Redis
  record, and replies with `KarpenterBatchStatusResponse`.

**`KarpenterBatchQueue`** — buffered channel (`cap = 64`) of `*karpenterBatchQueueItem`.

**`RunProcessor(ctx, ebc)`** — goroutine that drains the queue one batch at a time, calling
`ebc.processBatch()` for each. Runs for the lifetime of the process.

**`processBatch`** — marks all nodes RUNNING, groups items by `{pool, sku}`, builds a
`[]PoolSkuDelta`, delegates to `karpenterNodeLifecycle.ApplyBatchKarpenterStreamNodeAdd`, then
writes SUCCEEDED (with provider ID) or FAILED for every operation ID in the batch.

#### `pkg/context/karpenter_node_lifecycle.go` — new method

**`ApplyBatchKarpenterStreamNodeAdd(ctx, edgeID, streamID, batchID, items, deltas)`**

1. Resolve edge → workspace compute instance name + tenant IDs (same as single-node path).
2. Snapshot `MaxWorkerIndexFromStatus` before the increment.
3. Call `bulkIncrementWorkerNodeCatalogValue` on the `Worker Node SKU` variable — one call
   increments all pool+sku groups by their respective counts.
4. `ApplyWorkspaceComputeInstance` + `PublishWorkspaceComputeInstance` (once for the whole batch).
5. Poll every 30 s until `SUCCESS` or `FAILED`.
6. On `SUCCESS`: call `WorkerHostnamesAboveIndexFromStatus` to get new hostnames; assign them to
   operation IDs in order. If fewer real hostnames than nodes (e.g. status doesn't expose them),
   generate synthetic `rafay://cluster/pool/sku/node-<uuid>` IDs as fallback.
7. Return `map[operationID]providerID`.

#### `pkg/context/edge.go` — modifications

- Added `batchQueue *KarpenterBatchQueue` field to `EdgeBrokerContext`.
- `NewEdgeBrokerContext` initialises the queue and starts `batchQueue.RunProcessor` in a goroutine.
- Added compile-time assertion `var _ v1.KarpenterBatchServiceServer = (*EdgeBrokerContext)(nil)`.

#### `main.go` — modifications

`RegisterKarpenterBatchServiceServer` called on both the mTLS listener (port 5448) and the
internal plaintext listener (port 5449).

> ⚠️ **Registered on `:5449` does not mean usable on `:5449`.** Every `BatchStreamOperations`
> handler needs the caller's edge id, and that id is derived from the **mTLS client certificate**
> (`common.GetEdgeClientInfo` reads the peer's `credentials.TLSInfo`). A plaintext connection has no
> peer certificate, so the stream is rejected with `ErrorInvalidPeer`. Batch operations are
> therefore only servable over the mTLS listener (`5448`) — `EDGE_BROKER_GRPC_INSECURE=true`
> pointed at `:5449` cannot work for them. It previously **panicked** instead: `GetEdgeClientInfo`
> did an unchecked `p.AuthInfo.(credentials.TLSInfo)` assertion and `AuthInfo` is `nil` on a
> plaintext connection, so opening the stream on `:5449` crashed the **whole broker process** (gRPC
> does not recover handler panics) — an unauthenticated DoS. It now fails cleanly.

---

### karpenter-provider-rafay

#### `pkg/rafay/batcher.go` — new file

**`NodeBatcher`** — collects `batchItem` entries from a buffered channel and drives two goroutines.

`batchSender` goroutine:
- Calls `collectBatch` which blocks for up to `batchWindow` (10 s) or until `maxBatchSize` (10)
  items are queued, whichever comes first.
- Builds `[]*KarpenterBatchNodeAddItem` and calls `BrokerClient.SendBatch`.
- On success: stores the batch in `inProgress` map with a `pollAfter` timestamp
  (`send_time + 120 s` initial delay) and immediately loops back to collect the next batch.
- On failure: resolves all items in the batch with an error.

`statusPoller` goroutine:
- Ticks every `pollInterval` (30 s).
- For each in-progress batch whose `pollAfter` time has passed, calls `BrokerClient.PollBatchStatus`.
- For each node result: SUCCEEDED → record the operationID in the `succeeded` set and clear it from
  `inFlight` (`resultCh` was already resolved at broker ACK; for an **add** this is not on the
  registration critical path — the node joining is — but for a **remove** it is what lets `Delete()`
  converge, see [batch-node-removal.md](batch-node-removal.md#delete-must-converge--and-node-existence-cannot-be-how));
  FAILED → invoke the registered `FailureHandler` (which deletes the still-pending NodeClaim) and
  clear `inFlight` so a retry can re-send; ACCEPTED/RUNNING → leave in the pending list.
- Removes fully resolved batches from `inProgress`. Batches older than `maxBatchAge` (2 h), or
  unknown at the broker (3 consecutive empty status responses), are dropped with every remaining
  item treated as FAILED (`"batch expired at broker"`).

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
- `SendBatch(ctx, nodes)` — calls `sendBatch` via `callBrokerOnce` (no retry, same reasoning as single-node add).
- `PollBatchStatus(ctx, batchID)` — calls `pollBatchStatus` via `callBroker` (retries on transient errors are safe for reads).

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
rafayClient.StartBatcher(ctx)
cp := cloudprovider.NewCloudProvider(op.GetClient(), rafayClient, clusterID, projectID, rafayClient.Batcher())
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
                  ├─ writes all opIDs to Redis (ACCEPTED)
                  ├─ enqueues batch into KarpenterBatchQueue
                  └─ replies KarpenterBatchAccepted{batchId}
                            │
                  batchSender stores batch in inProgress and
                  resolves every resultCh (broker ACK)
                            │
              Create() unblocks → returns NodeClaim with
              pending providerID "rafay://pending/<uid>"
              (real ID patched later by NodeProviderIDController);
              batchSender immediately collects the next batch
                            │
              ┌─────────────┴─────────────┐
              │  statusPoller (every 30s)  │
              │  after 120s initial delay  │
              └─────────────┬─────────────┘
                            │
                  BrokerClient.PollBatchStatus(batchId)
                  → KarpenterBatchStatusResponse
                     {op1: RUNNING, op2: SUCCEEDED(pid), ...}
                            │
              SUCCEEDED → recorded in `succeeded` (adds: registration is
                          driven by the node joining, not by this;
                          removes: this is what lets Delete() converge)
              FAILED    → FailureHandler deletes the NodeClaim
                          (UID == operationID) if it still has a
                          pending providerID → Karpenter reprovisions
                          immediately
```

```
edge-broker batchProcessor goroutine (one batch at a time):
  1. Mark all opIDs RUNNING in Redis
  2. Group items by {poolName, instanceType}
  3. bulkIncrementWorkerNodeCatalogValue  (one JSON pass, all groups)
  4. ApplyWorkspaceComputeInstance        (one PaaS call)
  5. PublishWorkspaceComputeInstance      (one PaaS call)
  6. Poll GetWorkspaceComputeInstance every 30 s until SUCCESS | FAILED
  7. WorkerHostnamesAboveIndexFromStatus  → assign providerIDs to opIDs
  8. Write SUCCEEDED + providerID (or FAILED) to Redis for each opID
  9. Pick next batch from queue
```

---

## Redis Key Schema

| Key | Format | TTL | Content |
|-----|--------|-----|---------|
| `/edge/karpenter/nodeop/<operationID>` | existing | 15 min while ACCEPTED; 60 min on RUNNING and on a terminal **FAILED** (except the queue-full-rejection FAILED write, which keeps the 15-min accepted TTL); **24 h on a terminal SUCCEEDED** (tombstone, refreshed on re-send) | Per-node state (kind, state, detail, provider_ids, **edge_id**) |
| `/edge/karpenter/batchop/<batchID>` | new | 90 min | `{"operation_ids": ["op1", "op2", ...], "edge_id": "…"}` |

The staged nodeop TTL is deliberate:

- **ACCEPTED is short (15 min)** — if the broker restarts after accepting a batch but before
  processing it, the polling client sees FAILED (`"operation record expired"`) within 15 minutes
  instead of 60.
- **SUCCEEDED is a long-lived tombstone (24 h), refreshed on every duplicate send.** Clients re-send
  a deterministic operationID for the life of their retry loop, so a terminal record that expired
  would make the next retry look like a **brand-new** operation — it would be ACCEPTED again and the
  catalog delta applied a **second** time. (This bites the remove path hardest, where it retired an
  extra healthy machine; see below.)
- **FAILED keeps the short TTL and is NOT tombstoned** — a FAILED op must stay retryable, and
  letting it expire is equivalent to re-accepting it.

Every record also carries the authenticated **`edge_id`**; status polls and cancels ignore records
belonging to another edge. See the
[TTL strategy and replay protection in batch-node-removal.md](batch-node-removal.md#succeeded-is-a-tombstone--replay-protection)
for the full rationale.

---

## Configuration

No new environment variables. Batch parameters are compile-time constants in
[pkg/rafay/batcher.go](../../pkg/rafay/batcher.go):

| Constant | Default | Meaning |
|----------|---------|---------|
| `defaultMaxBatchSize` | 10 | Flush after this many items |
| `defaultBatchWindow` | 10 s | Flush after this long even if batch is not full |
| `defaultPollInterval` | 30 s | How often to poll broker for status |
| `defaultInitialDelay` | 120 s | Wait before first status poll after a batch is sent |
| `karpenterBatchQueueSize` | 64 | Server-side batch queue buffer (batches, not nodes) |
