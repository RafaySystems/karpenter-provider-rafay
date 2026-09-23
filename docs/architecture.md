# Architecture — karpenter-provider-rafay

> A [Karpenter](https://karpenter.sh/) cloud provider for the **Rafay private cloud / managed Kubernetes** platform.
> All node lifecycle operations are routed through Rafay's **edge-broker** gRPC relay — the same channel used by the `edge-client` agent deployed inside every Rafay-managed cluster.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Repository Layout](#2-repository-layout)
3. [System Architecture](#3-system-architecture)
4. [Component Descriptions](#4-component-descriptions)
5. [Core Packages](#5-core-packages)
6. [Data Flows](#6-data-flows)
7. [Worker Node SKU Catalog](#7-worker-node-sku-catalog)
8. [Operation State Machine](#8-operation-state-machine)
9. [Identity and Security](#9-identity-and-security)
10. [Provider ID Format](#10-provider-id-format)
11. [Idempotency and Resilience](#11-idempotency-and-resilience)
12. [Timeouts and Polling Model](#12-timeouts-and-polling-model)
13. [Configuration Reference](#13-configuration-reference)
14. [Key Dependencies](#14-key-dependencies)
15. [Design Decisions](#15-design-decisions)

---

## 1. Overview

`karpenter-provider-rafay` runs upstream [Karpenter](https://karpenter.sh/) against Rafay's private-cloud PaaS. It is **not** a public cloud provider — there are no AWS/GCP/Azure API calls. Three repositories cooperate to make it work:

- **karpenter-provider-rafay** (this repo) — the Karpenter `CloudProvider` implementation plus four helper controllers (`nodeproviderid`, `rafaynodeclass`, `headroom`, `nodeconfig`), wired in `cmd/controller/main.go`. It bootstraps the cluster's `RafayNodeClass` / `NodePool` objects from edge-broker, then translates Karpenter's per-`NodeClaim` `Create`/`Delete` calls into node add/remove requests, batches them, and drives them to completion.
- **edge-broker** — a broker process reachable over mTLS gRPC that owns a single **global** batch queue and processor. It turns batch add/remove requests into edits of the edge's **workspace compute-instance worker-node catalog** (a declarative PaaS object holding per-pool+SKU counts), then polls the PaaS until the compute instance converges.
- **edge-common** — shared gRPC plumbing and the hand-written `KarpenterBatchService` message/stream types used on the wire between the provider and the broker.

The model is **batch-over-broker with a declarative catalog**: the provider never calls a cloud "create instance" API directly. It edits counts in the PaaS catalog (increment to add, decrement to remove) via the broker; the PaaS materializes or retires physical machines to match. Because the catalog is count-based, the platform — **not** Karpenter — chooses which machine a decrement retires (see the targeted-removal limitation in [§4.2](#42-edge-broker-control-plane)).

All node lifecycle operations are routed through `rep.edge.v1.KarpenterBatchService.BatchStreamOperations`, using the same mTLS certificate material and dial conventions as the `edge-client` agent deployed inside every Rafay-managed cluster. The same stream also carries best-effort cancellation of queued operations. **NodeClaims in etcd are the sole source of truth**; no in-memory state is load-bearing across a pod restart, and idempotency is enforced at three layers (deterministic operationIDs, the batcher's in-flight set, and the broker's 24 h SUCCEEDED tombstone).

### Key Properties

| Property | Detail |
|----------|--------|
| **Add protocol** | `BatchStreamOperations` `batch_add` — up to 10 nodes per batch, 10s collection window |
| **Delete protocol** | `BatchStreamOperations` `batch_remove` — same batching, batched separately from adds |
| **Cancel protocol** | `BatchStreamOperations` `cancel_ops` — best-effort, only still-queued (ACCEPTED) operations |
| **Transport security** | mTLS — client certificate required; edge identity derived from cert Subject O |
| **State store (broker)** | Redis (per-op records: 15-min TTL while ACCEPTED, 60-min once RUNNING/terminal-FAILED, **24-h tombstone once SUCCEEDED**; batch index: 90-min TTL). One exception: a queue-full-rejection FAILED write keeps the 15-min ACCEPTED TTL |
| **Provisioning backend** | PaaS WorkspaceComputeInstance via `WorkspaceRPCService` (paas-api) |
| **Idempotency** | Operation ID — NodeClaim UID for adds, NodeClaim UID + `"-remove"` for deletes — prevents duplicate catalog mutations |
| **ProviderID resolution** | `NodeProviderIDController` patches real `spec.providerID` from the joined node |
| **Targeted removal** | Not supported by PaaS today — removes are catalog count decrements; the platform chooses which physical machine is retired (the node's ProviderID is carried in the protocol for future targeted removal) |

```
Karpenter autoscaler
       │
       │  CloudProvider interface
       ▼
karpenter-provider-rafay
       │  Add:    batch_add    (BatchStreamOperations, over mTLS)
       │  Del:    batch_remove (same stream, separate batches)
       │  Cancel: cancel_ops   (best-effort, pending NodeClaims only)
       │           → broker ACK → Create()/Delete() unblock immediately
       ▼
  Rafay edge-broker
       │
       ▼
  Rafay control plane  →  node provisioned / deprovisioned
       │
       ▼
  Node joins cluster with labels nodepoolname + sku_name
  and spec.providerID = rafay://nodepoolname/sku_name/hostname
       │
       ▼
  NodeProviderIDController patches NodeClaim.Status.ProviderID = node.Spec.ProviderID
  Karpenter registration reconciler → NodeClaim Registered=True
```

See [docs/crash_safety.md](crash_safety.md) for the full failure analysis of the "NodeClaims in etcd are the sole source of truth" property stated above.

---

## 2. Repository Layout

```
karpenter-provider-rafay/
├── cmd/
│   └── controller/              # Binary entry point (main.go)
│
├── config/                      # Also mirrored into infra-states — see docs/autoscaling-lifecycle.md
│   ├── crd/                     # CRD YAML: RafayNodeClass + upstream Karpenter
│   ├── deploy/                  # Deployment, RBAC, SA, Secret manifests
│   └── kustomization.yaml       # kubectl apply -k config
│
├── docs/
│   ├── architecture.md          # This document
│   ├── autoscaling-lifecycle.md # How the platform deploys/removes this controller (day0 + day2)
│   ├── crash_safety.md          # Crash-proof and idempotent design analysis
│   └── karpenter-rafay-internals.md  # Karpenter internals + bug-fix history
│
├── examples/                    # Sample RafayNodeClass, NodePool, headroom policy, test pods
│
└── pkg/
    ├── apis/v1alpha1/           # RafayNodeClass type, spec/status, DeepCopy
    ├── broker/                  # gRPC dial helpers, TLS credential loading, session ID
    ├── cloudprovider/           # Karpenter CloudProvider implementation + batch failure handler
    ├── controllers/
    │   ├── headroom/            # Per-pool overprovisioning Deployments of pause pods
    │   ├── nodeadoption/        # Creates a NodeClaim per pre-existing worker node in a pool
    │   ├── nodeconfig/          # Applies RafayNodeClass/NodePool fetched from edge-broker at startup
    │   ├── nodeproviderid/      # Resolves real ProviderID on NodeClaims after node joins
    │   └── rafaynodeclass/      # Reconciler: validates spec and sets Ready on RafayNodeClass
    ├── operator/                # Scheme registration (init side-effect)
    └── rafay/                   # Client interface, BrokerClient, NodeBatcher, stream helpers
        ├── batcher.go           # NodeBatcher: batches Create()/Delete() requests, polls broker
        ├── batch_stream.go      # sendBatch / sendBatchRemove / pollBatchStatus / cancelOps gRPC helpers
        ├── brokerclient.go      # BrokerClient: shared gRPC conn + all broker calls
        ├── client.go            # Client interface + AddNodesRequest/RemoveNodesRequest types
        ├── providerid.go        # ParseProviderID / BuildProviderID helpers
        └── errors.go            # Sentinel errors (incl. ErrBatchRejected)
```

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Managed Kubernetes Cluster                             │
│                                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────────┐   │
│  │                       karpenter-provider-rafay Pod                           │   │
│  │                                                                              │   │
│  │  ┌───────────────────┐    ┌──────────────────────────────────────────────┐   │   │
│  │  │  Karpenter Core   │    │              CloudProvider                   │   │   │
│  │  │  Controllers      │───▶│  Create  Delete  Get  List  GetInstanceTypes │   │   │
│  │  │  (upstream)       │    │                                              │   │   │
│  │  └───────────────────┘    │  Create  → Enqueue       → broker ACK        │   │   │
│  │                           │  Delete  → EnqueueRemove → broker ACK        │   │   │
│  │  ┌───────────────────┐    └─────────────────┬──────────────────────────┘   │   │
│  │  │  NodeProviderID   │                      │ Enqueue / EnqueueRemove       │   │
│  │  │  Controller       │    ┌─────────────────▼──────────────────────────┐   │   │
│  │  │  Watches NodeClaims│    │              NodeBatcher                   │   │   │
│  │  │  + Nodes           │    │  batchSender: collect 10 items / 10s      │   │   │
│  │  │  Patches real PID  │    │  statusPoller: poll every 30s             │   │   │
│  │  └───────────────────┘    │  FailureHandler: FAILED add → delete claim │   │   │
│  │                           └────────────────┬────────────────────────┘   │   │
│  │  ┌───────────────────┐                     │ SendBatch / SendBatchRemove  │   │
│  │  │  RafayNodeClass   │                     │ PollBatchStatus / CancelOps  │   │
│  │  │  Controller       │    ┌────────────────▼──────────────────────────┐   │   │
│  │  │  (readiness +     │    │              BrokerClient                  │   │   │
│  │  │   validation)     │    │  One *grpc.ClientConn (lazy, mutex-prot.) │   │   │
│  │  └───────────────────┘    │  30s deadline on every broker RPC         │   │   │
│  │  ┌───────────────────┐    └────────────────┬────────────────────────┘   │   │
│  │  │  Headroom         │                     │                              │   │
│  │  │  Controller       │                     │                              │   │
│  │  └───────────────────┘                     │                              │   │
│  └─────────────────────────────────────────────┼──────────────────────────────┘   │
└───────────────────────────────────────────────┼──────────────────────────────────┘
                                                │
                            gRPC over mTLS (port 5448)
                            metadata: sessionid = STREAM_ID
                                                │
                       ┌────────────────────────▼──────────────────────┐
                       │              Rafay edge-broker                  │
                       │  KarpenterBatchService.BatchStreamOperations    │
                       │  Serialized batch queue (one batch at a time)   │
                       │  Redis: operation state (15/60-min/24h TTLs)    │
                       └────────────────────────┬──────────────────────┘
                                                │
                       ┌────────────────────────▼──────────────────────┐
                       │  computeInstanceKarpenterNodeLifecycle          │
                       │  ├── edgesrv (edge registry)                   │
                       │  ├── paas-api (WorkspaceRPCService)             │
                       │  └── IaaS / Cloud (VM/node pool)                │
                       └───────────────────────────────────────────────┘
```

---

## 4. Component Descriptions

### 4.1 karpenter-provider-rafay (in-cluster)

Runs as a Kubernetes Deployment inside the managed cluster. Implements the Karpenter `CloudProvider` interface.

| Sub-component | Package | Responsibility |
|---------------|---------|----------------|
| **CloudProvider** | `pkg/cloudprovider` | `Create`, `Delete`, `Get`, `List`, `GetInstanceTypes`. `Create` and `Delete` both return immediately after broker ACK. `Create` selects the **cheapest compatible instance type** (synthetic price) and returns a synthetic pending ProviderID. |
| **Batch failure handler** | `pkg/cloudprovider` | `NewBatchFailureHandler` — invoked by the batcher on a terminal FAILED result. On a FAILED **add** it deletes the matching pending NodeClaim so Karpenter reprovisions immediately instead of waiting out the 60-min registration timeout; FAILED **remove** operations are log-only (Karpenter keeps calling `Delete`, which re-sends the removal). |
| **NodeProviderIDController** | `pkg/controllers/nodeproviderid` | Watches NodeClaims with `rafay://pending/` ProviderID + real Rafay nodes. Patches real ProviderID once node joins. Single-threaded (`MaxConcurrentReconciles: 1`). |
| **BrokerClient** | `pkg/rafay` | One shared `*grpc.ClientConn`. `SendBatch`, `SendBatchRemove`, `PollBatchStatus`, `CancelOperations` — each a short-lived stream on `BatchStreamOperations` with a 30s deadline — plus `GetKarpenterConfig`, a unary call on `KarpenterConfigService` (90s deadline: the broker fans out to PaaS per node SKU). |
| **Node config controller** | `pkg/controllers/nodeconfig` | Leader-elected runnable. Fetches this cluster's `RafayNodeClass` / `NodePool` manifests from edge-broker and server-side-applies them (classes first) at startup, then every `KARPENTER_CONFIG_SYNC_INTERVAL`. Skips an unchanged revision, applies nothing when the compute instance has autoscaling off, and **never prunes**. A `NotFound` from the broker ("this cluster has no Karpenter config" — no workspace compute instance, or no worker-pool catalog) is a quiet no-op sync re-checked at the normal interval, not an error-backoff retry. See [§7.1](#71-catalog--rafaynodeclass-bootstrap). |
| **NodeBatcher** | `pkg/rafay` | Collects `Create()`/`Delete()` requests into batches (up to 10 / 10s, adds and removes partitioned into separate batches), unblocks callers at broker ACK, polls status for FAILED feedback. Deduplicates retries by `operationID`. |
| **RafayNodeClass controller** | `pkg/controllers/rafaynodeclass` | Validates `spec.instanceTypes` and sets `status.conditions[Ready]` (True, or False with reason `ValidationFailed`) so Karpenter's NodePool controller can schedule. |
| **Headroom controller** | `pkg/controllers/headroom` | Maintains one Deployment of low-priority pause pods per configured NodePool (`headroom-<pool>`) for proactive scale-out. See [§5](#pkgcontrollersheadroom--overprovisioning-buffer-controller). |
| **RafayNodeClass CRD** | `pkg/apis/v1alpha1` | Defines the node shapes available for provisioning (`spec.instanceTypes`). It carries **no** Rafay cluster/project identity — that comes only from the controller's `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` env. |

### 4.2 edge-broker (control plane)

Central gRPC relay in the Rafay control plane.

**Two listeners:**

| Port | TLS | Karpenter service |
|------|-----|-------------------|
| `5448` | mTLS (client cert required) | `KarpenterBatchService` — **the only usable port for batch operations** |
| `5449` | Plaintext (internal only) | `EdgeBrokerService`; `KarpenterBatchService` is registered but **cannot serve batch operations** — see below |

> ⚠️ **`KarpenterBatchService` fundamentally cannot be served over the plaintext listener.** Every `BatchStreamOperations` handler needs the caller's **edge id**, and that id is derived from the **mTLS client certificate** (Subject Organization — `common.GetEdgeClientInfo`). On a plaintext connection there is no peer certificate, so the edge id cannot be established and the handler rejects the stream with `ErrorNoClientID` (`"NO CLIENT ID"`) — it maps *any* `GetEdgeClientInfo` failure to `ErrorNoClientID` and does not surface the underlying peer error (`ErrorInvalidPeer`) to the client. Batch operations must use the mTLS listener (`5448`); `EDGE_BROKER_GRPC_INSECURE=true` pointed at `:5449` will not work for them.
>
> This used to be worse than a clean failure: `GetEdgeClientInfo` performed an unchecked `p.AuthInfo.(credentials.TLSInfo)` type assertion, and on a plaintext connection `AuthInfo` is `nil` — so opening `BatchStreamOperations` on `:5449` **panicked and crashed the whole broker process** (gRPC does not recover handler panics), an unauthenticated DoS. It now returns `ErrorNoClientID` cleanly.

The old per-operation `KarpenterNodeService.StreamOperations` protocol has been **removed** — adds, removes, status polls, and cancels all flow through `BatchStreamOperations`.

**`BatchStreamOperations` request dispatch (client→broker union, field tags 1–4):**

| Frame | Tag | Handler |
|-------|-----|---------|
| `batch_add` (`KarpenterBatchNodeAddRequest`) | 1 | First runs a `batchOpOwnedBy` check: a batch whose `batch_id` is owned by a **different edge** is rejected up front with `batch_rejected` (reason `"batch id belongs to another edge"`) and nothing is written. Otherwise: per-op idempotency check against Redis, writes ACCEPTED records (15-min TTL), writes the batchID→opIDs index, enqueues the new/retry nodes on the serialized batch queue, replies `batch_accepted`. Queue full → marks the new ops FAILED (`"batch queue full; try again"`) and replies `batch_rejected` **in-band, keeping the stream alive**. |
| `status_poll` (`KarpenterBatchStatusPoll`) | 2 | Reads the batch index + per-op records, replies `batch_status` with `nodeResults[]`. Unknown/expired batchID → **empty `batch_status`** (stream stays alive); an expired op record inside a known batch → FAILED `"operation record expired"`. |
| `batch_remove` (`KarpenterBatchNodeRemoveRequest`) | 3 | Mirrors `batch_add` exactly, with op records written as kind `"delete"`. Same idempotency rules, same serialized queue, same `batch_accepted`/`batch_rejected` replies. |
| `cancel_ops` (`KarpenterBatchOperationCancel`) | 4 | Best-effort: a Redis compare-and-set transitions each op from ACCEPTED → FAILED (`"cancelled by client"`). RUNNING and terminal ops are untouched. Replies `cancel_ack` listing the ops actually cancelled. |

**Broker→client union (field tags 1–4):** `batch_accepted`, `batch_status`, `batch_rejected`, `cancel_ack`.

**Batch processor:** a single goroutine per broker process drains a channel-backed queue (buffer 64) one batch at a time. For each batch it CASes every still-ACCEPTED op to RUNNING (claiming it and extending the Redis TTL), groups ops by `{pool, sku}`, applies **one bulk catalog mutation** — positive deltas for adds, **negative deltas for removes (clamped at 0)** — then Apply + Publish and polls the compute instance every 30s until SUCCESS or FAILED. Two distinct deadlines bound this: the **batch-processor context** is capped at **50 min**, while the **compute-instance poll loop** runs on its own **60-min deadline** (30s interval). Ops are then patched SUCCEEDED (adds carry provider IDs; removes carry none) or FAILED with the error detail.

> **Platform limitation — no targeted removal.** The PaaS worker-node catalog is declarative (per pool+SKU counts), so a remove is a count decrement and **PaaS chooses which physical machine is retired** — not necessarily the node Karpenter selected. The `provider_id` field on `KarpenterBatchNodeRemoveItem` is carried so a future targeted-removal API can be adopted without a protocol change.

### 4.3 Redis — Operation State Store

Two key families:

**Per-operation records** under `/edge/karpenter/nodeop/<operationID>`:

```json
{
  "kind":         "add | delete",
  "state":        1,
  "detail":       "human-readable status message",
  "provider_ids": ["rafay://nodepoolname/sku_name/hostname"],
  "edge_id":      "7dkgjkx"
}
```

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `UNKNOWN` | Zero value, never written |
| 1 | `ACCEPTED` | Request received, queued for processing |
| 2 | `RUNNING` | Batch processor active, catalog mutation in progress |
| 3 | `SUCCEEDED` | Operation complete (adds: provider IDs available) |
| 4 | `FAILED` | Operation failed, detail contains error (also used for client cancellations) |

TTL strategy:

- **ACCEPTED** records use a **short 15-minute TTL** (`karpenterBatchAcceptedTTL`) — if the broker restarts after accepting a batch but before processing it, the polling client sees FAILED (`"operation record expired"`) within 15 minutes instead of hanging. The processor extends the TTL to **60 minutes** (`karpenterBatchRunningTTL`) when it CASes ACCEPTED→RUNNING.
- **SUCCEEDED** records are long-lived **tombstones** with a **24-hour TTL** (`karpenterBatchSucceededTTL`), and every duplicate send of an already-SUCCEEDED operationID **refreshes that TTL**. This is a correctness requirement, not a cache: clients re-send a deterministic operationID (`<uid>-remove`) for the whole life of their retry loop, so a tombstone that expired would make the next retry look like a brand-new operation — it would be ACCEPTED again and the processor would apply the catalog delta a **second** time. For a removal that retires an extra, healthy machine.
- **FAILED** records deliberately keep the **short** TTL and are **not** tombstoned: a FAILED operationID must stay retryable (a re-send rewrites it to ACCEPTED), and letting it expire is equivalent to re-accepting it.

**Batch index records** under `/edge/karpenter/batchop/<batchID>` (`{"operation_ids": [...], "edge_id": "…"}`, **90-minute TTL**) map a batch ID to its operation IDs so status polls can report every node in the batch.

**Edge scoping**: every `nodeop` and `batchop` record carries the `edge_id` of the authenticated stream that created it. Status polls and cancels only act on records belonging to the calling edge — another edge's batch ID is answered exactly like an unknown one (empty result list), and its operations are never cancelled. Records with an **empty** `edge_id` were written by an older broker and stay visible to everyone, so operations in flight across a broker upgrade are unaffected (the Redis key schema is unchanged).

State transitions that race other writers go through a **compare-and-set helper** built on a go-redis `WATCH/MULTI/EXEC` optimistic transaction, so a concurrent writer invalidates the transaction instead of being clobbered. Both racy paths use it: `cancel_ops` and the **queue-full rejection** (which marks ops FAILED only if they are still ACCEPTED, so it cannot clobber an op a processor concurrently claimed as RUNNING).

### 4.4 computeInstanceKarpenterNodeLifecycle — Provisioning Backend

Translates batch add/remove requests into **WorkspaceComputeInstance** catalog mutations via `WorkspaceRPCService` (paas-api): one `Get → bulk increment/decrement → Apply → Publish` per batch, then a 30s poll loop until the compute instance reaches SUCCESS or FAILED. See [§7 Worker Node SKU Catalog](#7-worker-node-sku-catalog) for the catalog data model.

### 4.5 edgesrv — Edge Registry

Resolves the edge record (cluster metadata) from the edge ID in the mTLS client certificate. Provides cluster display name and tenant scope (orgID, partnerID, projectID) for PaaS calls.

Two resolution paths:
- **v2private clusters**: `infra-apiserver.GetClusterFromID` → `edgesrv.GetEdge(name, projectID)`
- **Standard clusters**: `edgesrv.GetEdge(edgeID)` directly

### 4.6 paas-api — WorkspaceRPCService

| RPC | Purpose |
|-----|---------|
| `GetWorkspaceComputeInstance` | Read current spec + status |
| `ApplyWorkspaceComputeInstance` | Write updated spec (modified catalog variable) |
| `PublishWorkspaceComputeInstance` | Trigger IaaS reconciliation |

---

## 5. Core Packages

### `cmd/controller` — Entry Point

`main.go` wires the complete operator at startup:

| Step | What happens |
|------|-------------|
| 1 | `coreoperator.NewOperator()` sets up controller-runtime Manager, leader election, metrics |
| 2 | Reads all config from env vars; warns if `RAFAY_CLUSTER_ID` or `RAFAY_PROJECT_ID` are unset; invalid port values log a warning and fall back to the default instead of being silently ignored |
| 3 | Resolves **edge identity** from TLS client cert Subject Organization (O) |
| 4 | Constructs `rafay.BrokerClient` with resolved connection params |
| 5 | Registers the **batch failure handler** (`cloudprovider.NewBatchFailureHandler`) on the batcher — must happen **before** the batcher starts polling |
| 6 | Calls `rafayClient.StartBatcher(ctx)` — launches `NodeBatcher` sender + poller goroutines |
| 7 | Builds `CloudProvider` (with both the cached client and the uncached `apiReader`), wraps with `metrics.Decorate`, builds `state.NewCluster` |
| 8 | Resolves headroom namespaces from `HEADROOM_NAMESPACE` / `HEADROOM_CONFIG_NAMESPACE` |
| 9 | Registers all controllers: `rafaynodeclass`, `nodeproviderid`, `headroom`, `nodeadoption` (unless `KARPENTER_ADOPT_EXISTING_NODES=false`), `nodeconfig` (unless `KARPENTER_CONFIG_BOOTSTRAP=false`), plus upstream Karpenter controllers |
| 10 | `op.WithControllers(...).Start(ctx)` — runs until signal |

---

### `pkg/apis/v1alpha1` — Custom API Type

`RafayNodeClass` is a cluster-scoped CRD:

```
RafayNodeClass
├── spec
│   └── instanceTypes        []InstanceTypeSpec
│       ├── name             string             # e.g. "oci-inst" (node.kubernetes.io/instance-type)
│       ├── cpu              string             # e.g. "4" or "4000m"
│       ├── memory           string             # e.g. "8Gi"
│       ├── nvidia.com/gpu   string             # TEMPORARY accelerator capacity, e.g. "8"; omit for non-GPU SKUs
│       ├── zone             string             # topology.kubernetes.io/zone (default: "default")
│       ├── architectures    []string           # kubernetes.io/arch values, e.g. ["amd64"]
│       └── operatingSystems []string           # kubernetes.io/os values, e.g. ["linux"]
└── status
    └── conditions           []Condition        # Ready=True once validated; Ready=False/ValidationFailed otherwise
```

`spec.instanceTypes` is **required** — the readiness controller marks the NodeClass `Ready=False` (reason `ValidationFailed`) when it is empty or any entry has an empty name or unparseable cpu/memory/`nvidia.com/gpu`, and `GetInstanceTypes` returns an error for an empty list.

---

### `pkg/rafay` — Client Abstraction, Broker Streams, and NodeBatcher

#### `Client` interface

```go
type Client interface {
    GetNode(ctx context.Context, providerID string) (*NodeInfo, error)
    ListNodes(ctx context.Context, clusterID string) ([]*NodeInfo, error)
}
```

Node add/remove no longer go through this interface — they go through the `NodeBatcher` (`Enqueue` / `EnqueueRemove`). `GetNode` and `ListNodes` return `ErrGetNodeUnsupported` / `ErrListNodesUnsupported` on `BrokerClient` — the broker has no query API. `CloudProvider` catches these sentinels and falls back to reading Kubernetes `Node` objects directly.

#### `NodeBatcher` (`batcher.go`)

Collects individual `Create()` and `Delete()` requests and sends them to edge-broker in batches.

```
NodeBatcher internals
├── queue          chan batchItem            # incoming Create()/Delete() requests (buffered 256)
│                                            #   queue full → immediate error to the caller
├── inProgress     map[batchID]→batch        # batches awaiting status confirmation
├── pending        map[operationID]→[]chan   # every caller waiting on an operation (multi-waiter)
├── inFlight       set[operationID]          # ACKed at broker, not yet terminal → suppress re-sends
├── succeeded      map[operationID]→time     # broker-confirmed SUCCEEDED; drives Delete() convergence
├── failureHandler FailureHandler            # invoked on terminal FAILED / batch expiry
│
├── batchSender goroutine
│    ├── collectBatch: blocks for the FIRST item, then collects up to 10s / 10 items
│    ├── partitions items by kind: adds → SendBatch, removes → SendBatchRemove
│    │     (each partition is its own broker batch)
│    └── on broker ACK: mark every operationID in-flight, then resolve every waiter
│         (callers unblock NOW) and register the batch in inProgress for status polling
│
└── statusPoller goroutine (ticker: 30s; per-batch initial delay: 120s after send)
     └── pollAll → BrokerClient.PollBatchStatus per in-progress batch
          ├── SUCCEEDED       → record in `succeeded`, clear in-flight
          │                       (this is how Delete() learns a removal actually completed)
          ├── FAILED          → invoke FailureHandler(operationID, kind, detail), clear in-flight
          │                       (so a retry can re-send the operation)
          ├── ACCEPTED/RUNNING → keep in inProgress, poll next tick
          ├── 3 consecutive empty responses (unknown batchID at broker)
          │     → drop batch, every remaining item FAILED "batch expired at broker"
          └── batch older than 2h → same expiry treatment
```

**ACK is the unblocking event**: `Create()`/`Delete()` return as soon as the broker acknowledges the batch — they do **not** block until SUCCEEDED. But for removes the batcher does not forget the operation at ACK: `Succeeded(operationID)` reports whether the broker has confirmed the operation reached SUCCEEDED, and `CloudProvider.Delete` polls it on each reconcile to decide when the removal is complete (see [§6.2](#62-scale-in-node-deprovisioning)). Entries are pruned after `succeededRetention` (2h).

**In-flight suppression**: Karpenter's termination controller re-invokes `Delete()` **every 5 seconds** for the entire life of a node removal. Every operationID the broker has ACKed and not yet driven to a terminal state is held in the `inFlight` set; enqueueing one again resolves the caller **immediately** instead of sending a duplicate batch. Without this, each 5s reconcile would push another remove batch at the broker, flooding its global 64-slot queue with hundreds of copies of the same removal. The caller's contract — "queued at the broker" — is already satisfied, so resolving immediately is correct. An operation is dropped from the set as soon as it goes terminal (SUCCEEDED or FAILED) or its batch expires, so a genuine retry after a failure is never suppressed.

**Deduplication and multi-waiter fan-out**: `Enqueue(operationID, req)` / `EnqueueRemove(...)` check the `pending` map first. `pending` maps an operationID to a **slice** of result channels: the first caller queues the `batchItem`, and every concurrent or retrying caller for the same operationID **appends its own channel** and is resolved together with the others when the result arrives. (With a single shared channel only one waiter could consume the buffered result; the rest would block until their context was cancelled.) The pending entry is removed *before* the results are delivered, so a fast retry starts fresh rather than joining a list that is already being drained. Lock order when both locks are needed: `mu → pendingMu`.

**Cancellation**: `Cancel(operationID)` is fire-and-forget — a goroutine sends `CancelOperations` with a 10s timeout (detached from the caller's context) and logs the outcome. Only ops still ACCEPTED at the broker are cancelled.

**Testability**: the batcher holds its broker as the small unexported `batchBroker` interface (`SendBatch`, `SendBatchRemove`, `PollBatchStatus`, `CancelOperations`) — a subset of `*BrokerClient`. Production code still passes the concrete `*BrokerClient` via `NewNodeBatcher`; unit tests (`batcher_test.go`) substitute a scripted mock so no real gRPC is needed.

#### `BrokerClient` (`brokerclient.go`)

One shared `*grpc.ClientConn` (lazy-initialized, mutex-protected). **Every broker RPC carries a 30-second deadline** (`brokerCallTimeout`) — these are short request/response exchanges; long-running work is tracked via status polling.

- **`SendBatch` / `SendBatchRemove`** — open a `BatchStreamOperations` stream, send the add/remove request, receive `KarpenterBatchAccepted{batchId}` (or `KarpenterBatchRejected` → `ErrBatchRejected`), return batchID. **No retry** (`callBrokerOnce`) — re-issuing could duplicate catalog mutations.
- **`PollBatchStatus`** — sends `KarpenterBatchStatusPoll{batchId}`, receives `KarpenterBatchStatusResponse{nodeResults[]}`. Retries **once** on `codes.Unavailable` (`callBroker`) — polls are read-only and safe to retry.
- **`CancelOperations`** — sends `KarpenterBatchOperationCancel{operationIds[]}`, receives `KarpenterBatchCancelAck{cancelledOperationIds[]}`. Retries once on `codes.Unavailable` — cancels are idempotent at the broker.
- **Session routing** via gRPC metadata key `sessionid` = `STREAM_ID`.
- On `codes.Unavailable`, the stale connection is closed so the next call re-dials fresh.

#### Batch Stream Protocol (`batch_stream.go`)

```
sendBatch / sendBatchRemove
  ├─ Open BatchStreamOperations stream
  ├─ Send: batch_add{batchId, nodes[{operationId, clusterID, projectID, instanceType, nodePool}]}
  │        or batch_remove{batchId, nodes[... + providerId]}
  ├─ Recv: batch_accepted{batchId}   →  return batchId ✓
  │        batch_rejected{reason}    →  return ErrBatchRejected (broker queue full; retry later)
  └─ CloseSend

pollBatchStatus
  ├─ Open BatchStreamOperations stream
  ├─ Send: status_poll{batchId}
  ├─ Recv: batch_status{nodeResults[]}  →  return results ✓
  │        (unknown/expired batchID → empty nodeResults, not an error)
  └─ CloseSend

cancelOps
  ├─ Open BatchStreamOperations stream
  ├─ Send: cancel_ops{operationIds[]}
  ├─ Recv: cancel_ack{cancelledOperationIds[]}  →  return cancelled IDs ✓
  └─ CloseSend

Per-node result states
  ├─ SUCCEEDED  →  providerIds[] available for adds (removes carry none)
  ├─ FAILED     →  detail string contains error (incl. "cancelled by client",
  │                "operation record expired", "batch queue full; try again")
  └─ ACCEPTED / RUNNING  →  not done yet; poll again
```

---

### `pkg/broker` — gRPC Connection Helpers

| Function | Purpose |
|----------|---------|
| `NewSecureGrpcClientConn(host, port, creds)` | TLS dial via `grpc.NewClient` (lazy connect) |
| `NewInsecureGrpcClientConn(host, port)` | Plaintext dial (dev / rcloud internal listener) |
| `GetServerHostFromCert(certPath)` | Reads broker hostname from cert Subject OrganizationalUnit[0] |
| `GetEdgeClientCredentials(certPath, keyPath, caPath)` | Loads mTLS material |
| `GetEdgeIDFromClientCert(certPath)` | Edge identity from cert Subject Organization[0] (first DNS label) |
| `EdgeHashIDFromOrganization(org)` | Extracts first DNS label, e.g. `"7dkgjkx"` from `"7dkgjkx.cluster.example.com"` |
| `NewSessionID()` | Generates UUID v4 for `STREAM_ID` / gRPC `sessionid` metadata |

Keepalive: **5-minute ping / 30s timeout**, `PermitWithoutStream: false`. Max message size: **20 MB**.

---

### `pkg/cloudprovider` — Karpenter Interface Implementation

Implements `sigs.k8s.io/karpenter/pkg/cloudprovider.CloudProvider`:

| Method | Behavior |
|--------|---------|
| `Create(NodeClaim)` | Resolves `RafayNodeClass` → filters compatible instance types → **picks the cheapest** (synthetic price; stable sort keeps NodeClass spec order on ties) → enqueues to `NodeBatcher` (operationID = NodeClaim UID) → **blocks until broker ACK** → returns `NodeClaim` with `status.providerID = "rafay://pending/<uid>"`, capacity, and labels populated. The `NodeProviderIDController` later patches the real ProviderID. **Adopted NodeClaims short-circuit**: a NodeClaim annotated `karpenter.rafay.io/adopted-provider-id` describes a machine that is already running, so no broker call is made and that ProviderID is returned directly (see [§5](#pkgcontrollersnodeadoption--existing-node-adoption-controller)). |
| `Delete(NodeClaim)` | **Drives the removal to completion across repeated calls.** If the batcher reports the remove operation (`<uid>-remove`) SUCCEEDED at the broker, returns `NodeClaimNotFoundError` — the signal that releases the node's termination finalizer. Otherwise: if ProviderID is still pending, tries to find the real node by labels first, and if none has joined fires a best-effort broker cancellation of the queued add (`batcher.Cancel(nodeClaim.UID)`) and returns `NodeClaimNotFoundError`. Otherwise enqueues a batch remove (operationID = NodeClaim UID + `"-remove"`), **returns `nil` at broker ACK**, and Karpenter requeues and calls `Delete` again in 5s. In-flight suppression in the batcher makes those repeat calls cheap (no duplicate batch is sent). |
| `Get(providerID)` | For `rafay://pending/` prefix: returns minimal NodeClaim (provisioning in progress). Otherwise: falls back to scanning k8s `NodeList` by `spec.providerID`. **Not** the termination completion signal — see [§6.2](#62-scale-in-node-deprovisioning). |
| `List()` | `ListNodes` unsupported → falls back to listing **every** k8s Node whose `spec.providerID` carries the `rafay://` prefix. There is deliberately **no cluster-ID filter** — see the note below. |
| `GetInstanceTypes(NodePool)` | Returns `RafayNodeClass.spec.instanceTypes` parsed and validated. Each offering carries a **synthetic price = 1.0 per vCPU + 0.125 per GiB of memory** so smallest-fit selection and consolidation have a gradient. |
| `IsDrifted(NodeClaim)` | Always `("", nil)` — no drift detection. |
| `Name()` | `"rafay"` |

Cluster/project IDs for broker requests have a single source: `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID`, read at startup and captured into `CloudProvider.clusterID` / `.projectID` by `NewCloudProvider`. Both `Create` and `Delete` stamp those values on every `AddNodesRequest` / `RemoveNodesRequest` verbatim. There is no per-NodeClass override and no precedence rule to reason about.

> **Why `List()` must not filter by cluster ID.** A real ProviderID is stamped by the Rafay platform as `rafay://<nodepoolname>/<sku_name>/<hostname>` — its first segment is the **node pool**, not a cluster ID (see [§10](#10-provider-id-format)). Comparing that segment against `RAFAY_CLUSTER_ID` therefore never matches, and `List()` would return an empty slice. That is dangerous, not merely useless: Karpenter's core garbage-collection controller **deletes any `Registered` NodeClaim whose ProviderID is absent from `List()`**, so an empty list tears down every managed node the moment it briefly goes `NotReady`.

**`findNodeProviderID`** (used by `Delete` for pending NodeClaims) reads through the **uncached `apiReader`** — a stale informer cache here could match the wrong node:
```
findNodeProviderID(ctx, nodeClaim)
  ├─ Build usedIDs: ProviderIDs from all non-self non-pending NodeClaims (apiReader)
  ├─ List Nodes with label:  nodepoolname=<pool>        (apiReader; sku filtered in code)
  └─ Return first node where:
       ├─ node.sku_name ∈ NodeClaimSKUs(nodeClaim)   (see below)
       ├─ spec.providerID != "" and not in usedIDs
       └─ creationTimestamp > nodeClaim.creationTimestamp
```

**`NodeClaimSKUs` — how a node's `sku_name` is matched to a NodeClaim.** `sku_name` on a node is the **platform's SKU / instance-type name**, not the RafayNodeClass name. A node matches if its `sku_name` equals **either**:

1. the NodeClaim's `node.kubernetes.io/instance-type` label — the instance type `Create()` actually selected, stamped onto the NodeClaim via `requirementsToLabels`. This is the authoritative match; **or**
2. the NodeClaim's `spec.nodeClassRef.name` — back-compat with the legacy single-SKU convention where a `RafayNodeClass` is named after its one SKU (and a fallback if the label is not yet visible).

Because a NodeClaim can accept more than one `sku_name` value, `sku_name` **cannot** be a server-side label selector; the Node list is selected by `nodepoolname` only and `sku_name` is filtered in code. Matching on the NodeClass name *alone* would never resolve a node for any `RafayNodeClass` that lists **several** `instanceTypes` — exactly the configuration cheapest-fit selection exists to serve — leaving such NodeClaims unmatched until the 60-minute registration timeout deleted them, orphaning the machine each cycle.

`NodeClaimSKUs` is exported because the same matching rule has to hold in three places that must not disagree: `Delete`'s `findNodeProviderID`, the [`NodeProviderIDController`](#pkgcontrollersnodeproviderid--providerid-resolution-controller), and the [`NodeAdoptionController`](#pkgcontrollersnodeadoption--existing-node-adoption-controller) — the last two being complementary halves of one decision about which NodeClaim owns a node.

**`NewBatchFailureHandler(kubeClient, apiReader)`** returns the `rafay.FailureHandler` wired in `main.go`:
- kind `"add"`: finds the NodeClaim whose UID equals the operationID and **deletes it** — but only if it still carries a pending ProviderID and has no deletion timestamp. Karpenter then reprovisions immediately (seconds) instead of waiting out the 60-minute registration timeout.
- kind `"remove"`: log-only — nothing to clean up client-side. The operationID is cleared from the in-flight set, and Karpenter keeps calling `Delete` (every 5s, while the NodeClaim is terminating), so the next call re-sends the removal and the broker rewrites the FAILED record to ACCEPTED.

---

### `pkg/controllers/nodeproviderid` — ProviderID Resolution Controller

**Purpose**: Resolves the real Kubernetes `spec.providerID` for NodeClaims that have a synthetic pending ProviderID, and patches `NodeClaim.Status.ProviderID` with the real value.

**Why it exists**: `Create()` returns immediately after broker ACK to avoid blocking past Karpenter's 5-minute `launchTimeout`. Nodes take ~12 minutes to join. The `NodeProviderIDController` bridges this gap well within Karpenter's 60-minute `registrationTimeout` window.

```
Controller struct {
    kubeClient client.Client   # cached informer client
    apiReader  client.Reader   # direct API-server reader (bypasses cache for usedIDs)
}
```

**`Reconcile` flow:**

```
Skip if ProviderID does not start with "rafay://pending/" or NodeClaim is being deleted

nodePoolName ← nodeClaim.Labels[karpenter.sh/nodepool]
skus         ← NodeClaimSKUs(nodeClaim)
                 = { nodeClaim.Labels[node.kubernetes.io/instance-type],   # selected SKU
                     nodeClaim.Spec.NodeClassRef.Name }                    # legacy single-SKU

Read NodeClaimList directly from API server (apiReader, not cache) → build usedIDs
  (bypasses informer cache to prevent stale-cache double-assignment race)

List Nodes with label: nodepoolname=<nodePoolName>
  (sku_name is NOT a server-side selector — a NodeClaim may accept several values)

Find first unclaimed node where:
  ├─ node.Labels[sku_name] ∈ skus
  ├─ spec.providerID != "" and not in usedIDs
  └─ creationTimestamp > nodeClaim.creationTimestamp

If not found → requeue after 30s

Patch NodeClaim.Status.ProviderID = node.Spec.ProviderID (optimistic locking)
  ├─ Conflict → Requeue: true (retry with fresh resource version)
  └─ NotFound → ignore (NodeClaim deleted during disruption)
```

**Key properties:**
- **`sku_name` matches the selected instance type, not just the NodeClass name.** `node.sku_name` is the platform's SKU name; a NodeClass listing several `instanceTypes` provisions a node whose `sku_name` is the *selected* one, which does not equal the NodeClass name. Matching on `NodeClassRef.Name` alone therefore never resolved such NodeClaims — they churned on the hourly registration timeout, orphaning a machine each cycle. See `NodeClaimSKUs` under [`pkg/cloudprovider`](#pkgcloudprovider--karpenter-interface-implementation).
- `MaxConcurrentReconciles: 1` — read-decide-patch is never concurrent, preventing two NodeClaims from claiming the same node.
- **Secondary watch on Nodes**: when a real Rafay node appears (filtered by `rafayNodePredicate`: non-empty `spec.providerID` + Rafay labels), matching pending NodeClaims are immediately enqueued without waiting for the 30s requeue timer.
- **Stateless across restarts**: controller-runtime re-enqueues all existing NodeClaims with `rafay://pending/` ProviderID on pod startup. No explicit resume logic needed.
- **`apiReader` vs `kubeClient`**: `usedIDs` reads from the API server directly to avoid a stale-cache race where a just-patched ProviderID is not yet visible in the informer cache when the next reconcile runs.

---

### `pkg/controllers/nodeadoption` — Existing-Node Adoption Controller

**Purpose**: Gives every worker node that already exists in the cluster a NodeClaim, so Karpenter manages the whole node pool rather than only the nodes it provisioned itself.

**Why it exists**: A Rafay cluster is created *with* worker nodes in it. The compute instance's "Worker Node Pool" catalog says `pool1` has 3 nodes, and the platform builds them long before this controller runs. Those nodes carry the pool's identity as labels — `nodepoolname=pool1`, `sku_name=oci-inst` — but no NodeClaim, and a node without a NodeClaim is invisible to Karpenter in two ways that matter:

| Without a NodeClaim | Consequence |
|---|---|
| `NodePool.status.resources` / `.status.nodes` count NodeClaims only | A pool that already has 3 nodes reports **0**, and its `limits` are computed as if the cluster were empty — so Karpenter will provision up to the full limit *on top of* the existing nodes |
| `StateNode.ValidateNodeDisruptable` requires a NodeClaim (`"node isn't managed by karpenter"`) | Capacity the catalog provisioned can **never** be consolidated or reclaimed — only capacity Karpenter itself added |

**`Reconcile` flow** (primary watch: `NodePool`; secondary watch: `Node`):

```
Skip if the NodePool is being deleted, or its nodeClassRef is not karpenter.rafay.io/RafayNodeClass
Skip if the NodePool is annotated karpenter.rafay.io/auto-scaling=false
  # edge-broker's inert rendering of a catalog row that did not opt into autoscaling (§7.1):
  # the pool is visible in-cluster, but its nodes must never get NodeClaims — a claim would
  # hand Karpenter ownership (limit accounting, termination on claim delete) of a pool the
  # user sized by hand. Hand-written pools carry no annotation and adopt as before.

poolNodes  ← Nodes with label nodepoolname=<nodePool.Name>            (informer cache)
allNodes   ← every Node, for the worker-node count in the log line    (informer cache)
claimList  ← every NodeClaim, from the API server (apiReader, not the cache)

claimedIDs ← { status.providerID } ∪ { karpenter.rafay.io/adopted-provider-id }
pending    ← NodeClaims for this pool still carrying rafay://pending/

log: pool <name> has N node(s) and M nodeclaim(s) (K worker node(s) across all pools)

for each node in poolNodes:
  skip if deleting, control-plane, no sku_name label, or a non-rafay:// providerID
  skip if the NodeReady condition is not True                  # wait for it; see below
  skip if claimedIDs[node.spec.providerID]                    # already has a NodeClaim
  skip if claimedByPendingClaim(pending, node, sku)           # a scale-out is waiting for it
  skip if sku_name ∉ the pool's RafayNodeClass instanceTypes  # would be marked drifted
  skip if validateAdoptable fails                             # labels vs pool reqs + SKU offerings

  patch Node: spec.providerID (if empty) = rafay://<pool>/<sku>/<name>, karpenter.sh/registered=true
  create NodeClaim annotated karpenter.rafay.io/adopted-provider-id=<providerID>

requeue after 5m
```

**How adoption avoids provisioning a machine.** Karpenter's `Launch` reconciler calls `CloudProvider.Create()` for every NodeClaim whose `Launched` condition is not yet `True` — so a hand-created NodeClaim would order a **second** machine for a node the cluster already has. The `karpenter.rafay.io/adopted-provider-id` annotation is what prevents that: `Create()` sees it and returns that ProviderID (plus the SKU's capacity) without touching the broker. Karpenter then registers and initializes the NodeClaim against the existing node through its normal path.

The marker is an **annotation on the object at creation time**, not a status pre-patch by this controller, because status is a separate subresource: a create-then-patch sequence leaves a window in which `Launch` sees a NodeClaim with no ProviderID and calls the broker. The annotation is visible on the very first `Launch` reconcile and survives controller restarts.

**Key properties:**

- **Only Ready nodes are adopted** — `nodeutils.GetCondition(node, NodeReady).Status == True`, the same test Karpenter's own `Initialization` reconciler applies. A NodeClaim for a NotReady node is worse than none at all: it is **counted** (while uninitialized, `StateNode.Capacity()` fills any resource the node does not report from the NodeClaim's — so for a node whose kubelet never reported, or whose Node object is later deleted while the claim lives, the SKU's *declared* figures become the whole capacity and the pool's limits are charged for a machine that is not there), and it is **stuck** — `Initialization` requires `NodeReady`, so the claim sits at `Registered=True` / `Initialized=Unknown("NodeNotReady")` while `Liveness` only reaps claims that failed to *register*, and garbage collection only considers claims absent from `List()` — which this one is not, since adoption just stamped a `rafay://` providerID on its node. Nothing reaps it short of `spec.expireAfter` (720h). A machine mid-teardown presents identically, and adopting it writes an **immutable** `spec.providerID` onto an object already on its way out. Waiting costs nothing: the Node watch fires on the status update that flips the condition, so adoption follows within seconds rather than at the next resync. A node that is Ready and *later* goes NotReady keeps its NodeClaim — this gate governs only when to take a node on; from then on Karpenter's **disruption** path owns it (not node repair: that controller is only registered when `RepairPolicies()` is non-empty, and this provider returns `nil`).
- **The readiness gate is evaluated before the ownership checks**, so that a node still coming up — kubelet registered, but `sku_name` or `spec.providerID` not yet stamped by the platform — gets a quiet `V(2)` deferral rather than recurring warnings, and so the lazy `GetInstanceTypes` resolution (which hard-errors the reconcile on an unusable `RafayNodeClass`) is not hoisted above it. The consequence is that an **already-adopted** node that goes NotReady reaches the gate too, which is why the `notReady` counter excludes nodes whose providerID is already claimed — otherwise the log would report a node as "waiting to become Ready" while the same line said nodes == nodeclaims.
- **Idempotent through the status gap.** An adopted NodeClaim has an empty `status` until `Launch` runs, so `claimedIDs` is built from the adoption annotation *as well as* `status.providerID`. Reading only the status would let a second pass adopt the same node again — two NodeClaims for one machine, the second of which Karpenter eventually terminates, taking the node with it. NodeClaims are read through `apiReader` for the same reason: a just-created NodeClaim is not in the informer cache yet.
- **Never steals a pending NodeClaim's node.** `claimedByPendingClaim` is the mirror of the [`NodeProviderIDController`](#pkgcontrollersnodeproviderid--providerid-resolution-controller) rule: that controller binds a node to a pending NodeClaim only when the node is *newer* than the claim, so a node newer than any pending claim for the same pool+SKU is that claim's to take. Adopting it would leave the claim unresolved until its 60-minute registration timeout.
- **Refuses to adopt what Karpenter would immediately replace.** `validateAdoptable` mirrors the drift sub-controller exactly — `areRequirementsDrifted` (pool requirements vs NodeClaim labels) and `instanceTypeNotFound` (a SKU offering compatible with those labels). An arm64 node labelled into an amd64 pool fails both; adopting it would mark the NodeClaim drifted and drain a running node. Such nodes are named in a warning and left unmanaged.
- **Well-known labels come from the Node, not the SKU.** `Registration` copies a NodeClaim's labels *onto* its node, so a value inferred from the SKU that disagrees with the node would overwrite what the kubelet reported. Topology labels are omitted entirely when the node has none — a `RafayNodeClass` with no `zone` yields the synthetic zone `default`, and stamping that onto a running node would replace real topology information. Omitting the key is safe: `zone` is a well-known label, so Karpenter treats a NodeClaim that lacks it as *unconstrained* when matching pool requirements and SKU offerings (`Requirements.Compatible` skips well-known keys the NodeClaim does not define, and `Intersects` only compares keys present in both).
- **Pool taints are not copied.** `Registration` merges a NodeClaim's taints onto its node; a `NoExecute` taint the node does not already have would evict the pods running on it. The platform has already applied the pool's catalog taints to the nodes it built.
- **No `karpenter.sh/nodepool-hash` annotation.** Static drift is only evaluated when *both* NodePool and NodeClaim carry the hash, so leaving it off means a later edit to the pool template does not mark every adopted node drifted and roll nodes the operator never asked Karpenter to create.
- **Control-plane nodes are refused** even if they carry a `nodepoolname` label: a NodeClaim would let Karpenter drain and remove the cluster's own control plane.
- **`spec.providerID` is filled in only when empty.** Kubernetes makes the field immutable once set, which is also why a wrong value would be unrecoverable — `rafay.BuildProviderID` renders exactly the format the platform itself writes (`rafay://<nodepoolname>/<sku_name>/<hostname>`, see [§10](#10-provider-id-format)).
- **`karpenter.sh/registered=true` is pre-set on the node.** The `Registration` reconciler logs a registration error and emits an event for any node carrying neither that label nor the `karpenter.sh/unregistered` startup taint. An already-joined node genuinely is registered, and the taint is not an option — it is `NoExecute` and would evict the pods already there.
- **`spec.expireAfter` comes from the pool template**, exactly as for a NodeClaim Karpenter provisions — so an adopted node is eventually rotated on the pool's own expiry policy (the CRD default is `720h` when the template sets none). Diverging here would make adopted and provisioned nodes in the same pool age differently.
- **Disabled with `KARPENTER_ADOPT_EXISTING_NODES=false`.** Adoption means Karpenter may consolidate a pre-existing node once it is empty. That is the point — the pool becomes Karpenter's to size — but it is a real behaviour change for a cluster whose nodes were previously untouchable.

---

### `pkg/controllers/rafaynodeclass` — Readiness + Validation Controller

On every `RafayNodeClass` create/update, the reconciler validates the spec and patches the `Ready` condition:

- **Valid** (at least one instance type; every entry has a non-empty name and parseable `cpu`/`memory` quantities) → `Ready=True`. This unblocks the upstream Karpenter `NodePool` controller, which gates all provisioning on the referenced `NodeClass` being `Ready`.
- **Invalid** → `Ready=False` with reason `ValidationFailed` and a message naming the first offending entry, so misconfiguration is visible on the NodeClass instead of failing deep inside `Create()`.

- Named `rafaynodeclass.readiness`
- `MaxConcurrentReconciles: 10`

---

### `pkg/controllers/headroom` — Overprovisioning Buffer Controller

Maintains one Deployment of low-priority pause pods per configured Karpenter NodePool (`headroom-<pool>`, the proven cluster-overprovisioner pattern). When a real workload preempts a pause pod, the ReplicaSet replaces it; the replacement goes Pending with `nodeSelector: karpenter.sh/nodepool=<name>`, so Karpenter scales that exact pool out and the buffer is restored.

- **Config**: `headroom-policy` ConfigMap, per pool: buffer fractions `cpu`/`memory`/`gpu` (percentages), per-pod slice `podCPU`/`podMemory`/`podGPU` (defaults 500m / 512Mi; GPU has no default), `minPods` cold-start floor, and optional `tolerations`. Data keys are scanned in a **deterministic order** — the well-known keys `policy` then `config`, then every other key sorted by name — because Go map iteration is randomized and an unsorted scan would let the applied policy flap between reconciles when two keys both declare pools. **Duplicate pool names are rejected** at parse time (they would apply two conflicting specs to the same Deployment on every sync).
- **A ConfigMap parse error keeps the buffer up.** A YAML typo returns an **error** from `Reconcile`, which short-circuits *before* any GC runs. It must never read as "no pools configured": falling through with an empty pool list would make `cleanupStaleDeployments` tear down **every** headroom Deployment in the cluster over a single bad character. A ConfigMap that parses cleanly and declares *no* pools is a legitimate "headroom off" policy and does tear the Deployments down.
- **Replica math**: `replicas = max over configured resources of ceil(fraction × Σ ready-node allocatable / per-pod size)`, floored at `minPods` (with zero ready nodes the result is exactly `minPods`), then **clamped to `[0, maxHeadroomReplicas]` (5000)**. The per-pod size is the divisor, so a typo there (or a huge `minPods`) can ask for an absurd number of pause pods; the clamp turns that into a loud warning and a bounded pod count instead of an `int32` overflow or a pod flood. Per-pod sizes below `1m` CPU / `1Mi` memory are rejected at parse time for the same reason.
- **Watch-based reconciler** (not a ticker): ConfigMap, pool-Node, and headroom-Deployment events all funnel into one synthetic full-sync request, plus a 5-minute safety resync. `MaxConcurrentReconciles: 1`.
- **Pod spec**: pause image, PriorityClass `rafay-headroom` (value −1000, `PreemptionPolicy: Never`), Guaranteed QoS (requests == limits), hostname topology spread, and **only the tolerations from config** — no blanket `Exists` toleration, so headroom pods respect taints that real workloads cannot cross.
- **Steady-state reconciles issue no Updates.** The reconciler mutates only the fields it owns, **in place on the fetched Deployment template**. Assigning a whole `PodTemplateSpec` would drop everything the API server defaults (`restartPolicy`, `dnsPolicy`, `schedulerName`, `securityContext`, …), so the object would differ from the stored one on *every* reconcile and `CreateOrUpdate` would issue a pointless Update — forever, every 5 minutes.
- **PriorityClass ensured on every Reconcile**, not just at startup: the startup runnable gets exactly one attempt, and without the PriorityClass the pause pods are rejected at admission, silently disabling headroom until the next restart. The create is idempotent (normally a 409). A PriorityClass's `Value` is immutable and the controller holds no update verb, so a **pre-existing `rafay-headroom` class with a different value or `preemptionPolicy` cannot be repaired** — it is reported with a **loud warning on every reconcile** instead of passing silently (a non-negative value makes headroom pods un-preemptable, so the buffer stops handing capacity back; `preemptionPolicy != Never` lets them evict real workloads). Fixing it means deleting the class so it is recreated.
- **GC**: legacy bare headroom pods and Deployments for pools no longer in the config are deleted on every sync — but only after the config parsed successfully (see above).
- **Namespaces** from `HEADROOM_NAMESPACE` (Deployments) and `HEADROOM_CONFIG_NAMESPACE` (ConfigMap); see [§13](#13-configuration-reference).
- **RBAC**: needs write on `apps/deployments` and create/get on `scheduling.k8s.io/priorityclasses` (granted in `config/deploy/rbac.yaml`).

> **GPU headroom reserves capacity on existing GPU nodes; it cannot cold-start a pool.** The GPU resource name is **derived from the GPU nodes the pool already has**; on a pool with no GPU nodes the `podGPU` request is **dropped with a warning** rather than guessed (a guessed request would only park a pause pod in `Pending` forever, and on an `amd.com/gpu` pool the guess would be wrong outright). Use `minPods` with a cpu/memory-sized pod to cold-start a GPU pool; the GPU buffer applies once nodes exist.
>
> This is stricter than the cloudprovider now needs. Instance types advertise a **temporary** hardcoded `nvidia.com/gpu` capacity (see [the RafayNodeClass reference](#rafaynodeclass-crd-portable--same-manifest-on-every-cluster)), so Karpenter *can* now provision for a pending GPU pod — but the headroom controller still keys off existing nodes and is unchanged. Revisit it when that temporary field is replaced by real per-SKU accelerator capacity; sourcing `gpuKey` from the instance type would then let a GPU buffer drive scale-out too.

> **Requires `consolidationPolicy: WhenEmpty`** on participating NodePools. Pause pods keep nodes non-empty by design; with `WhenEmptyOrUnderutilized` Karpenter would repeatedly consolidate nodes that hold only headroom pods, causing churn. The examples in `examples/nodepool.yaml` are set accordingly.

---

## 6. Data Flows

### 6.1 Scale-Out (Node Provisioning)

```
Pod unschedulable
  └─▶ Karpenter provisioner creates NodeClaim (Launched=Unknown)
        └─▶ CloudProvider.Create(nodeClaim)
              ├─ GET RafayNodeClass                       (k8s API)
              ├─ Validate, filter compatible, pick cheapest InstanceType
              └─▶ NodeBatcher.Enqueue(operationID=nodeClaim.UID, req)
                    │  (blocks on resultCh)
                    │
                    ├─ [batchSender] collects up to 10 items / 10s
                    ├─ BrokerClient.SendBatch → KarpenterBatchAccepted{batchId}
                    └─ broker ACK resolves resultCh → Create() unblocks NOW

  └─▶ Create() returns NodeClaim{
            status.providerID = "rafay://pending/<nodeclaim-uid>"
            status.capacity   = selected instance type capacity
        }
  └─▶ Karpenter lifecycle controller patches NodeClaim.status (Launched=True, synthetic ProviderID written to etcd)
  └─▶ Karpenter registration reconciler sets Registered=Unknown (60-min timer starts)

  [in the background]
  └─▶ [statusPoller] waits 120s, then polls every 30s
        ├─ SUCCEEDED → recorded in the batcher's `succeeded` set (adds: not on the
        │               registration critical path — the joined node is)
        └─ FAILED    → FailureHandler deletes the pending NodeClaim
                        └─▶ Karpenter reprovisions immediately (fresh NodeClaim, fresh UID)

  [~12 minutes later]

  └─▶ Node joins cluster with labels nodepoolname + sku_name, spec.providerID set by Rafay platform
        └─▶ NodeProviderIDController reconciles (triggered by node watch or 30s requeue)
              ├─ Lists NodeClaims from API server (no cache)  →  build usedIDs
              ├─ Lists Nodes with matching labels
              └─ Patch NodeClaim.Status.ProviderID = node.Spec.ProviderID
  └─▶ Karpenter registration reconciler finds node matching real ProviderID → Registered=True
```

### 6.2 Scale-In (Node Deprovisioning)

Karpenter's termination controller (`awaitInstanceTermination`) calls `CloudProvider.Delete()` on **every** reconcile and releases the Node's termination finalizer **only** when `Delete()` returns a `NodeClaimNotFoundError`; anything else requeues in **5 seconds**. `Delete()` must therefore eventually converge on that error, or the NodeClaim and Node stay `Terminating` forever.

```
Disruption controller: consolidate / expire NodeClaim
  └─▶ CloudProvider.Delete(nodeClaim)                       [called every ~5s until it converges]
        ├─ If batcher.Succeeded("<uid>-remove"):
        │    └─▶ return NodeClaimNotFoundError  ← THE convergence signal; Karpenter releases
        │                                          the finalizer and finishes termination ✓
        ├─ If ProviderID is "rafay://pending/<uid>":
        │    ├─ findNodeProviderID → look for joined node by labels (apiReader)
        │    ├─ If found: use real ProviderID
        │    └─ If not found: best-effort batcher.Cancel(uid)  →  return NodeClaimNotFoundError
        │         (only still-ACCEPTED ops are cancelled at the broker)
        └─▶ NodeBatcher.EnqueueRemove(operationID=<uid>+"-remove", req{
                  clusterID, projectID,                  # from RAFAY_CLUSTER_ID / RAFAY_PROJECT_ID env
                  instanceType, nodePoolName, providerID })
              ├─ already in flight at the broker?  →  resolve immediately, send nothing
              │     (suppresses the duplicate batch each 5s reconcile would otherwise push)
              ├─ [batchSender] batches removes (separately from adds)
              ├─ BrokerClient.SendBatchRemove → KarpenterBatchAccepted{batchId}
              └─ broker ACK resolves resultCh → Delete() returns nil
                    → Karpenter requeues in 5s and calls Delete() again
  └─▶ Broker: catalog count for {pool, sku} decremented by 1 (clamped at 0), Apply + Publish
        ⚠ PaaS chooses which physical machine is retired (no targeted removal today);
          the providerID is carried in the protocol for a future targeted-removal API
  └─▶ [statusPoller] observes the op SUCCEEDED → records it in `succeeded`
        └─▶ the NEXT Delete() call (≤5s later) takes the branch at the top and converges ✓
```

> **Why node existence cannot be the completion signal.** It is tempting to have `Delete()` report "gone" once no Kubernetes Node carries the ProviderID. That never converges: during termination **Karpenter itself holds the Node object alive with its own finalizer**, and it only drops that finalizer once `Delete()` reports the instance gone. "Is there still a Node with this providerID?" is therefore circular. The broker's SUCCEEDED result is an *external* signal and is the only one that terminates the loop.

### 6.3 Pod Restart Recovery

```
karpenter-provider-rafay pod restarts
  │
  ├─ NodeProviderIDController starts
  │    └─ controller-runtime re-enqueues ALL NodeClaims with "rafay://pending/" ProviderID
  │         └─ Each reconcile: list nodes → patch ProviderID if node has joined
  │
  ├─ For NodeClaims where Launched=Unknown (broker not yet ACK'd):
  │    └─ Karpenter lifecycle controller re-calls Create()
  │         └─ NodeBatcher.Enqueue(same UID) → broker deduplicates by operationID
  │              └─ No second node provisioned
  │
  └─ inProgress poll state is lost (in-memory): in-flight batches are no longer polled,
     so FAILED feedback for them falls back to the 60-min registration timeout backstop
```

See [docs/crash_safety.md](crash_safety.md) for a complete step-by-step failure analysis.

### 6.4 NodeClass Readiness

```
RafayNodeClass created / updated
  └─▶ rafaynodeclass.Controller.Reconcile()
        ├─ valid spec   → status.conditions[Ready]=True
        └─ invalid spec → status.conditions[Ready]=False (reason ValidationFailed)
  └─▶ NodePool readiness gate cleared (when Ready=True)
  └─▶ NodePool active for provisioning
```

### 6.5 Connection Lifecycle

```
BrokerClient
  ├─ conn: *grpc.ClientConn   (nil until first use)
  ├─ mu:   sync.Mutex         (protects conn field only)
  │
  ├─ getConn(ctx)
  │    ├─ conn != nil  →  return existing
  │    └─ conn == nil  →  dialBroker(ctx)  →  grpc.NewClient(addr, opts...)
  │
  ├─ SendBatch / SendBatchRemove  →  callBrokerOnce (NO retry)
  │    ├─ acquire conn, apply 30s deadline
  │    ├─ run streaming fn once
  │    └─ on codes.Unavailable: closeConn() so next call re-dials
  │         (no re-issue — would duplicate the catalog mutation)
  │
  └─ PollBatchStatus / CancelOperations  →  callBroker (retries ONCE on codes.Unavailable)
       └─ safe to retry: polls are read-only, cancels are idempotent at the broker
```

---

### 6.6 Adopting a Pool's Pre-Existing Nodes

The cluster's first worker nodes are built by the platform from the catalog's `noOfSku`, before Karpenter exists. Adoption is what brings them under Karpenter's accounting, and it runs entirely in-cluster — no broker traffic at any step.

```
nodeconfig applies NodePool "pool1"  (or an operator applies it by hand)
        │
        ▼
NodeAdoptionController.Reconcile(pool1)
        │  Nodes labelled nodepoolname=pool1  →  3 found  (all Ready; NotReady ones wait)
        │  NodeClaims labelled karpenter.sh/nodepool=pool1  →  0 found
        │
        ├─ per node: patch spec.providerID = rafay://pool1/oci-inst/<hostname>   (only if empty)
        │            patch karpenter.sh/registered = true
        └─ per node: create NodeClaim
                       annotations: karpenter.rafay.io/adopted-provider-id=rafay://pool1/…
                       labels:      karpenter.sh/nodepool=pool1, instance-type=oci-inst,
                                    arch/os copied from the node, NO zone unless the node has one
                       ownerRef:    NodePool pool1
                       spec:        pool template requirements, instance-type pinned to oci-inst,
                                    NO taints, NO nodepool-hash annotation
        │
        ▼
Karpenter nodeclaim.lifecycle
        │
        ├─ Launch        → CloudProvider.Create() sees the adoption annotation,
        │                  returns that ProviderID + the SKU's capacity, NO broker call
        │                  → Launched=True
        ├─ Registration  → finds the Node by status.providerID, syncs labels/finalizer/ownerRef
        │                  → Registered=True, status.nodeName set
        └─ Initialization→ node Ready, no startup/ephemeral taints
                           → Initialized=True, karpenter.sh/initialized=true on the Node
        │
        ▼
NodePool.status.nodes = 3, status.resources reflects the real capacity
Consolidation and limits now see the whole pool, not just what Karpenter added
```

The pool's size is read from the **cluster** (Nodes carrying `nodepoolname=<pool>`), not from the catalog's `noOfSku`: the config RPC returns rendered manifests, not counts (see [§7.1](#71-catalog--rafaynodeclass-bootstrap)), and the cluster is the authority on which machines actually joined.

---

## 7. Worker Node SKU Catalog

The **Worker Node SKU** is a special variable (`"Worker Node SKU"`) on the `WorkspaceComputeInstance` spec. Its value is a JSON array where each row describes a pool / SKU combination and its desired node count:

```json
[
  {
    "poolname":  "worker-pool-amd",
    "skuname":   "oci-inst",
    "noOfSku":   3
  },
  {
    "poolname":  "worker-pool-arm",
    "skuname":   "oci-inst-arm",
    "noOfSku":   1
  }
]
```

| Field | Source | Meaning |
|-------|--------|---------|
| `poolname` | `NodePool.metadata.name` (from `NodePoolName` in the batch item) | Identifies which node pool this row applies to |
| `skuname` | `InstanceTypeSpec.name` from `RafayNodeClass` | The instance type / SKU to provision |
| `noOfSku` | Managed by edge-broker | Desired number of worker nodes for this pool+SKU |

The broker applies one **bulk** mutation per batch (`bulkIncrementWorkerNodeCatalogValue`): all per-`{poolname, skuname}` deltas from the batch in a single pass, then Apply + Publish.

**Scale-out:** `noOfSku += N` for the matching row; if no row exists for the requested `{poolname, skuname}`, a **new row is appended**.
**Scale-in:** `noOfSku -= N` for the matching row, **clamped at 0** (never negative); a decrement for a row that does not exist is a no-op (skipped, never appended as a negative row).

Because the catalog is declarative counts, scale-in cannot name the machine to remove — see the platform limitation note in [§4.2](#42-edge-broker-control-plane).

### Mapping: RafayNodeClass → Catalog

```
RafayNodeClass.spec.instanceTypes[i].name  ──────────▶  catalog row "skuname"
NodePool.metadata.name (via NodeClaim label)  ──────────▶  catalog row "poolname"
```

### 7.1 Catalog → RafayNodeClass (bootstrap)

That mapping also runs in reverse, once, at startup. `pkg/controllers/nodeconfig` calls
**`KarpenterConfigService.GetKarpenterConfig`** on edge-broker; the broker reads the same catalog
rows plus the `ComputeProfile` behind each `skuname` and returns rendered manifests, which the
controller server-side-applies (node classes first):

```
catalog row "poolname"                       ──────────▶  NodePool.metadata.name
catalog row "skuname"                        ──────────▶  RafayNodeClass.metadata.name
                                                          + spec.instanceTypes[0].name
catalog row labels / annotations / taints    ──────────▶  NodePool.spec.template
ComputeProfile ocpus / memory_in_gbs / shape ──────────▶  instance type cpu / memory / architectures
```

The round trip is what makes the names above a **contract**: a generated `NodePool` renamed by
hand would send a `poolname` the catalog does not have, and scale-out would append a new row
instead of growing the real pool.

`noOfSku` is deliberately not consumed — it is the pool's size *now*, and from bootstrap onward
Karpenter owns that number.

**Autoscaling is strictly opt-in per pool — but every pool is visible.** A catalog row carries
`autoScaling` (with `minNodeCount`/`maxNodeCount` bounds), and the broker derives the response's
`auto_scaling` flag from the rows (no row opted in ⇒ false ⇒ this controller applies nothing).
When **any** row opts in, the broker renders a NodePool for **every** valid row, so
`kubectl get nodepools` shows the whole catalog — but only the opted-in rows render as live
pools. A row without the field — or with it null/false — renders **inert**: identical labels,
taints, requirements and nodeClassRef, but `spec.limits.nodes: "0"` (the scheduler excludes a
pool with no remaining node budget, so it can never scale out), a single all-reasons disruption
budget of `nodes: "0"` (consolidation and drift can never pick its nodes), and
`template.spec.expireAfter: Never` (no forced expiration from this template). Every rendered
pool is stamped `karpenter.rafay.io/auto-scaling: "true"|"false"`; the node-adoption controller
skips pools marked `"false"`, so a disabled pool's nodes never get NodeClaims and Karpenter
cannot count, drain, or terminate them. There is no cluster-level toggle in this path — the old
"Auto Scaling" compute-instance variable is not consulted. For opted-in pools the max bound
lands as `spec.limits.nodes` plus a `karpenter.rafay.io/max-nodes` annotation; the min is
annotation-only (`karpenter.rafay.io/min-nodes`) — a NodePool has no floor, headroom is the
mechanism that holds warm capacity. A disabled row's bounds are not rendered at all (the driver
never validated them).

Turning a pool's `autoScaling` **off** now takes effect on the next resync: the server-side
apply rewrites its NodePool into the inert shape, so scale-out and consolidation stop without
anyone deleting the object. One caveat: NodeClaims created while the pool was enabled keep their
originally stamped `expireAfter` (NodeClaim spec is immutable), so such a node can still be
force-expired at that horizon — and, the pool now being inert, it is not replaced.

Operational notes: objects carry `karpenter.rafay.io/managed-by=edge-broker` and
`karpenter.rafay.io/config-revision`; a resync whose revision is unchanged is skipped; nothing is
ever pruned (the broker also omits a pool when its `ComputeProfile` read fails, and deleting a
`NodePool` drains its nodes — a pool **removed from the catalog** keeps its stale NodePool
in-cluster until someone removes it); `KARPENTER_CONFIG_BOOTSTRAP=false` turns the whole
thing off. See the broker-side rendering rules in `edge-broker/docs/karpenter-node-lifecycle.md`.

---

## 8. Operation State Machine

```
                    ┌───────────────────────┐
                    │   (new operation_id)   │
                    └───────────┬───────────┘
                                │ batch_add / batch_remove
                                ▼
                         ┌────────────┐  cancel_ops (CAS, only from ACCEPTED)
                         │  ACCEPTED  │──────────────────────────┐
                         │ (TTL 15m)  │◀── idempotent re-send    │
                         └─────┬──────┘    (existing op, non-    │
                               │            FAILED: no rewrite)  │
                               │ batch processor CAS             │
                               │ ACCEPTED→RUNNING (TTL → 60m)    │
                               ▼                                 │
                         ┌────────────┐                          │
                         │  RUNNING   │                          │
                         └─────┬──────┘                          │
                  ┌────────────┴────────────┐                    │
                  │                         │                    │
                  ▼                         ▼                    ▼
           ┌────────────┐           ┌──────────────────────────────┐
           │ SUCCEEDED  │           │            FAILED            │
           │ + provider │           │ + detail (error text /       │
           │   IDs (add)│           │   "cancelled by client")     │
           │ TOMBSTONE  │           │ (TTL 60m — stays RETRYABLE:  │
           │ (TTL 24h,  │           │  a re-send rewrites it to    │
           │  refreshed │           │  ACCEPTED)                   │
           │  on re-send)│          │                              │
           └────────────┘           └──────────────────────────────┘
```

Enum values on the wire: `UNKNOWN=0, ACCEPTED=1, RUNNING=2, SUCCEEDED=3, FAILED=4`. State transitions are written to Redis by the batch processor; the racy ones (`cancel_ops` and queue-full rejection, both vs. ACCEPTED→RUNNING) use the `WATCH`-based compare-and-set so the loser of the race is skipped rather than clobbered. The broker's status handler reads Redis and returns the current state to the polling client — only for records belonging to the calling edge.

**SUCCEEDED is a tombstone, and that is a correctness requirement.** The terminal SUCCEEDED record must outlive any plausible client retry horizon, so it carries a **24-hour TTL** that is **refreshed on every duplicate send**. Clients re-send a deterministic operationID (`<uid>-remove`) for as long as they keep retrying an operation; when the tombstone expired with the 60-minute RUNNING TTL, the next re-send looked like a **brand-new** operationID, was ACCEPTED again, and the processor applied the catalog delta a **second** time — for a removal, retiring an extra healthy machine, once per hour per terminating NodeClaim. FAILED records deliberately keep the shorter TTL: a FAILED operation **must** stay retryable, and expiring is equivalent to re-accepting.

**Batch sequence (client side):**

```
t=0s       SendBatch{nodes[]}                → Recv KarpenterBatchAccepted{batchId}
           → resultCh resolved: Create()/Delete() return immediately
t=0–120s   Wait (no polls — per-batch initial delay)
t=120s     PollBatchStatus{batchId}          → Recv ... nodeResults: RUNNING
t=150s     PollBatchStatus{batchId}          → Recv ... RUNNING
...
t=Xs       PollBatchStatus{batchId}          → Recv terminal states:
             ├─ SUCCEEDED → recorded in `succeeded` + cleared from `inFlight`;
             │              batch removed from inProgress when all items terminal
             │              (removes: the NEXT Delete() call converges on this)
             └─ FAILED    → FailureHandler invoked (add: pending NodeClaim deleted);
                            cleared from `inFlight` so a retry can re-send
```

For **adds**, the `NodeProviderIDController` independently resolves the real ProviderID from the joined node (see [§6.1](#61-scale-out-node-provisioning)) — broker SUCCEEDED is not on the critical path for registration. For **removes**, SUCCEEDED *is* the critical path: it is what lets `Delete()` return `NodeClaimNotFoundError` and release the node's termination finalizer (see [§6.2](#62-scale-in-node-deprovisioning)).

---

## 9. Identity and Security

### mTLS Client Certificate

Edge-broker requires mutual TLS. The client certificate carries two identity fields used at runtime:

| Cert Field | Used For |
|-----------|---------|
| **Subject Organization (O)** | Edge identity — the first DNS label (e.g. `7dkgjkx` from `7dkgjkx.cluster.example.com`) is the edge hash ID. Broker reads this via `common.GetEdgeClientInfo(ctx)` to identify which cluster is calling. |
| **Subject OrganizationalUnit (OU)** | Broker hostname — used by the client to determine which edge-broker host to dial. |

Both the karpenter provider and `edge-client` use the **same certificate material** (`client.crt`, `client.key`, `ca.crt`) mounted from the `edge-client-creds` Secret.

**The client certificate is the only source of edge identity**, which has two consequences worth stating explicitly:

1. **Batch operations require mTLS.** `GetEdgeClientInfo` reads the peer's `credentials.TLSInfo`; on a plaintext connection there is no peer certificate and the batch-stream handler rejects the stream with `ErrorNoClientID` (the internal peer error `ErrorInvalidPeer` is not surfaced). `KarpenterBatchService` therefore cannot be served over the broker's plaintext internal listener (see [§4.2](#42-edge-broker-control-plane)). (It previously *panicked* there on an unchecked type assertion, crashing the entire broker process.)
2. **Every operation record is scoped to its edge.** The authenticated edge id is stamped on each `nodeop` / `batchop` record, and status polls and cancels refuse records owned by a different edge (see [§4.3](#43-redis--operation-state-store)).

### Session Routing

The `sessionid` gRPC metadata header (value = `STREAM_ID`) routes streams within the broker. Set once by `BrokerClient.streamContext` before the stream opens.

### Concurrency Control (broker side)

Edge-broker processes batches **strictly sequentially**: a single processor goroutine drains the per-process batch queue (buffer 64) one batch at a time, so two catalog mutations never race. If the queue is full, new batches are rejected in-band (`batch_rejected`) instead of queued. Within a batch, the ACCEPTED→RUNNING compare-and-set ensures each operation is claimed by exactly one processor run even if duplicate queue items exist.

### Concurrency Control (provider side)

`NodeProviderIDController` uses `MaxConcurrentReconciles: 1` with `apiReader` (direct API server reads) to ensure no two reconciles can assign the same node to different NodeClaims, even with a warm informer cache. `CloudProvider.findNodeProviderID` applies the same uncached-read discipline on the delete path.

---

## 10. Provider ID Format

### Synthetic pending (pre-join)

```
rafay://pending/<nodeclaim-uid>
```

Set by `Create()` immediately after broker ACK. Signals to `NodeProviderIDController` that this NodeClaim is awaiting real ProviderID assignment. The `<nodeclaim-uid>` is the Kubernetes UID of the NodeClaim.

### Real format (post-join, set by Rafay platform)

```
rafay://<nodepoolname>/<sku_name>/<hostname>
```

Example: `rafay://worker-pool-amd/oci-inst/payes-test-10-k97psrhs-w1-e6a5c`

| Segment | Source | Meaning |
|---------|--------|---------|
| `nodepoolname` | Rafay platform | Matches the `nodepoolname` node label and `karpenter.sh/nodepool` NodeClaim label |
| `sku_name` | Rafay platform | The **platform's SKU / instance-type name**. Matches the `sku_name` node label, and on the NodeClaim it matches the `node.kubernetes.io/instance-type` label (the instance type `Create()` selected) — or `spec.nodeClassRef.name` under the legacy single-SKU convention. See `NodeClaimSKUs`. |
| `hostname` | Rafay platform | Kubernetes node name |

The Rafay platform sets `spec.providerID` on the node when it joins. The `NodeProviderIDController` reads this value directly from `node.Spec.ProviderID` and patches it onto the NodeClaim — the provider never constructs this format.

> ⚠️ **The first segment is the node pool, NOT a cluster ID.** `ParseProviderID` in `pkg/rafay/providerid.go` splits a Rafay provider ID into its first path segment (`nodePoolName`) and the remainder (`"<sku_name>/<hostname>"`). It is **not** used to filter nodes by cluster: an earlier version of this provider had `listNodesFromKube` compare that first segment against `RAFAY_CLUSTER_ID`, which can never match, so `List()` always returned empty — and the core garbage-collection controller then deleted any `Registered` NodeClaim whose node briefly went `NotReady`. The cluster-ID filter is gone; `List()` now returns every Node carrying a `rafay://` ProviderID (see [`pkg/cloudprovider`](#pkgcloudprovider--karpenter-interface-implementation)).
>
> A synthetic pending ID (`rafay://pending/<uid>`) parses structurally with `nodePoolName == "pending"`; callers that must distinguish pending IDs check `PendingProviderIDPrefix` rather than the parse result.

---

## 11. Idempotency and Resilience

### Operation ID

Every operation is keyed deterministically off the Kubernetes `NodeClaim.UID`:
- **Adds**: `operationID = NodeClaim.UID`
- **Removes**: `operationID = NodeClaim.UID + "-remove"`

The UID is **stable across provider restarts** (Karpenter reuses the same NodeClaim object when retrying after a crash) and **unique per desired node**, so retried `Create()`/`Delete()` calls dedup both in the batcher and at the broker.

When the broker receives a batch item with an `operation_id` that already exists in Redis:
- `RUNNING`: skipped — a live processor owns it.
- `SUCCEEDED`: skipped (already done) **and the tombstone's 24h TTL is refreshed**, so a persistently retrying client keeps its own tombstone alive and can never be double-applied.
- `ACCEPTED`: re-enqueued **without rewriting Redis** (restart recovery; a duplicate queue item is harmless because the processor only claims still-ACCEPTED ops via CAS).
- `FAILED`: treated as a client retry — record rewritten to ACCEPTED and re-enqueued.

An operation belonging to a **different edge** is never touched (see the edge-scoping note in [§4.3](#43-redis--operation-state-store)).

### NodeBatcher Deduplication, In-Flight Suppression, and Multi-Waiter Fan-Out

Three mechanisms keep repeated `Create()`/`Delete()` calls from turning into repeated broker work:

- **Pending dedup**: `Enqueue` / `EnqueueRemove` check the `pending` map before adding to the queue. If `Create()`/`Delete()` is cancelled and retried with the same operationID, no duplicate `batchItem` enters the queue.
- **Multi-waiter fan-out**: `pending` maps an operationID to a **slice** of result channels, so *every* caller waiting on the operation is resolved when the result arrives — not just whichever one happened to drain the single buffered channel first. The entry is removed when the result (ACK or send error) is delivered.
- **In-flight suppression**: once the broker has ACKed an operation and it has not gone terminal, the operationID sits in the `inFlight` set and re-enqueueing it resolves immediately without sending anything. This is what makes Karpenter's **5-second** `Delete()` retry loop free: without it, every reconcile for the whole life of a removal would push another remove batch onto the broker's global 64-slot queue.

### Delete() Convergence

`Delete()` is not fire-and-forget. Karpenter's termination controller calls it on every reconcile and releases the Node's finalizer **only** on a `NodeClaimNotFoundError`. The batcher records every operationID the broker reports SUCCEEDED (`NodeBatcher.Succeeded`, retained 2h), and `Delete()` returns `NodeClaimNotFoundError` once `<uid>-remove` has SUCCEEDED. Node existence cannot serve as this signal — Karpenter's own finalizer keeps the Node alive until `Delete()` says the instance is gone, so it is circular (see [§6.2](#62-scale-in-node-deprovisioning)).

### No Retry on Batch Sends; One Retry on Polls/Cancels

`SendBatch` and `SendBatchRemove` use `callBrokerOnce`. On `codes.Unavailable`:
- The stale connection is closed so the next call re-dials clean.
- The failed send is **not re-issued** from the broker wrapper — a retry would risk duplicating the catalog mutation.
- Application-level retry: `Create()`/`Delete()` return the error, Karpenter retries, and the same operationID dedups at the broker if the first send actually landed.

`PollBatchStatus` and `CancelOperations` use `callBroker`, which retries **once** on `codes.Unavailable` — both are safe to re-issue. A failed poll is additionally retried on the next 30s tick.

### Batch Expiry Backstops

The status poller stops tracking a batch when:
- the broker answers an unknown batchID with an empty status **3 consecutive times**, or
- the batch is older than **2 hours** (`maxBatchAge`).

In both cases every unresolved item is treated as FAILED (`"batch expired at broker"`) and handed to the failure handler — so a lost broker-side record converts into a fast NodeClaim replacement rather than a silent hang.

### Crash Safety Summary

| State at pod restart | `Launched` in etcd | Broker knows? | Recovery |
|---|---|---|---|
| Item in queue, not sent | Unknown | No | Karpenter re-calls `Create()` → fresh first request |
| Batch sent, awaiting ACK | Unknown | Maybe | Karpenter re-calls `Create()` → broker deduplicates |
| Broker ACK'd, `Launched=True` set | True | Yes | `NodeProviderIDController` resumes from etcd; FAILED feedback for the in-flight batch is lost with the in-memory poll state → 60-min registration timeout backstop |
| Real ProviderID patched | True | Yes | Fully stable |

See [docs/crash_safety.md](crash_safety.md) for per-step failure analysis.

---

## 12. Timeouts and Polling Model

### Registration Timeout Window

Karpenter's `registrationTimeout` is **60 minutes** (set in the Rafay fork of `sigs.k8s.io/karpenter`, `github.com/RafaySystems/karpenter-rafay` branch `rafay-release-v1.14.x`, `pkg/controllers/nodeclaim/lifecycle/liveness.go`) from when `Registered=Unknown` is first set (typically ~1s after `Create()` returns). Nodes join at **~12 minutes**, leaving a wide margin. The 60-minute timer is a **backstop**: broker-reported failures do not wait for it — the status poller's failure handler deletes the pending NodeClaim within roughly one poll interval of the broker writing FAILED.

```
t=0       Create() returns at broker ACK → Launched=True, synthetic ProviderID in etcd
t=~1s     Registration reconciler → Registered=Unknown (60-min timer starts)
t=12m     Node joins → NodeProviderIDController patches real ProviderID
t=12m+    Registration reconciler finds node → Registered=True  ✓
t=60m     registrationTimeout backstop — only reached if the broker reported nothing
          (e.g. SUCCEEDED but the node never joined)
```

### Timeout Reference

| Layer | Constant | Value |
|-------|----------|-------|
| Batch: per-RPC deadline (send/poll/cancel) | `brokerCallTimeout` | 30s |
| Batch: initial wait before first poll (per batch) | `defaultInitialDelay` | 120s |
| Batch: interval between status polls | `defaultPollInterval` | 30s |
| Batch: collection window (starts at first item) | `defaultBatchWindow` | 10s |
| Batch: max items per batch | `defaultMaxBatchSize` | 10 |
| Batch: client queue buffer | `batchItemQueueBuffer` | 256 |
| Batch: max tracking age before expiry | `maxBatchAge` | 2h |
| Batch: empty polls before expiry | `maxConsecutiveEmptyPolls` | 3 |
| Batch: how long a SUCCEEDED operationID is remembered (drives `Delete()` convergence) | `succeededRetention` | 2h |
| Cancel: fire-and-forget call timeout | `cancelTimeout` | 10s |
| Karpenter (core): `Delete()` retry interval while terminating | `awaitInstanceTermination` requeue | 5s |
| NodeProviderIDController: requeue when no node | `nodeWaitRequeueTime` | 30s |
| Headroom: safety resync | `resyncInterval` | 5m |
| gRPC keepalive ping | `keepalive.ClientParameters.Time` | 5 min |
| gRPC keepalive timeout | `keepalive.ClientParameters.Timeout` | 30s |
| Karpenter (fork): launch timeout | `launchTimeout` | 5 min |
| Karpenter (fork): registration timeout | `registrationTimeout` | 60 min |
| Broker: batch queue buffer | `karpenterBatchQueueSize` | 64 |
| Broker: batch processor deadline | (server-side) | 50 min |
| Broker: compute-instance poll | (server-side) | every 30s, 60-min cap |
| Broker: ACCEPTED record TTL | `karpenterBatchAcceptedTTL` | 15 min |
| Broker: RUNNING / FAILED record TTL | `karpenterBatchRunningTTL` | 60 min |
| Broker: SUCCEEDED tombstone TTL (refreshed on re-send) | `karpenterBatchSucceededTTL` | 24 h |
| Broker: batchID→opIDs index TTL | `karpenterBatchOpTTL` | 90 min |

---

## 13. Configuration Reference

### TLS / Connectivity

| Variable | Default | Description |
|----------|---------|-------------|
| `CERT_FOLDER` / `EDGE_CLIENT_CERT_FOLDER` | — | Directory with `client.crt`, `client.key`, `ca.crt` for mTLS |
| `SERVER_PORT` / `EDGE_CLIENT_SERVER_PORT` | `5448` | TLS gRPC port to edge-broker (invalid values warn and keep the default) |
| `EDGE_BROKER_GRPC_INSECURE` | `false` | `true` = plaintext gRPC. ⚠️ **Not usable for batch operations** — the broker derives the edge id from the mTLS client certificate, so `BatchStreamOperations` on the plaintext listener is rejected with `ErrorNoClientID`. Keep `false`. |
| `EDGE_BROKER_GRPC_PORT` | `0` (must be set explicitly) | Port when `EDGE_BROKER_GRPC_INSECURE=true` (see the caveat above). The provider defaults this to `0`, **not** `5449`; `5449` is only the broker's hardcoded plaintext internal listener port, not the provider env-var default |
| `EDGE_BROKER_GRPC_HOST` | (from cert OU) | Override broker dial host (required when insecure + no cert) |

### Identity / Routing

| Variable | Default | Description |
|----------|---------|-------------|
| `EDGE_ID` | (from cert Subject O) | Logging label for edge identity; warns if it disagrees with the cert |
| `STREAM_ID` | (auto UUID v4) | gRPC `sessionid` metadata header — auto-generated if unset |
| `RAFAY_CLUSTER_ID` | — | Rafay cluster ID stamped on every broker add/remove payload. Sole source — no per-NodeClass override exists. |
| `RAFAY_PROJECT_ID` | — | Rafay project ID stamped on every broker add/remove payload, when the platform requires it. Sole source. |

### Controller Behavior

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ELECTION_NAMESPACE` | `karpenter` | Namespace for leader-election Lease objects |
| `KARPENTER_DISABLE_LEADER_ELECTION` | `false` | Disable leader election (local dev) |
| `HEADROOM_NAMESPACE` | `karpenter` | Namespace where headroom Deployments (pause pods) are created |
| `HEADROOM_CONFIG_NAMESPACE` | (= `HEADROOM_NAMESPACE`) | Namespace of the `headroom-policy` ConfigMap |
| `KARPENTER_CONFIG_BOOTSTRAP` | `true` | Fetch `RafayNodeClass` / `NodePool` from edge-broker and apply them (see [§7.1](#71-catalog--rafaynodeclass-bootstrap)). Set `false` when those objects are owned by hand or by GitOps — the resync applies with `force: true` and would overwrite the real owner |
| `KARPENTER_CONFIG_SYNC_INTERVAL` | `10m` | How often to re-fetch that config after the first success (Go duration) |
| `KARPENTER_ADOPT_EXISTING_NODES` | `true` | Create a NodeClaim for each worker node the platform provisioned before Karpenter ran, so the pool's existing nodes count towards its limits and can be consolidated (see [§5](#pkgcontrollersnodeadoption--existing-node-adoption-controller)). Set `false` to leave them outside Karpenter's control |

> **Why `karpenter` namespace for leader election?** Rafay's platform webhook blocks Lease writes in `rafay-system`. The controller uses `karpenter` namespace to avoid this restriction.

### Kubernetes Secrets

| Secret | Namespace | Contents |
|--------|-----------|---------|
| `edge-client-creds` | `rafay-system` | `client.crt`, `client.key`, `ca.crt` — mounted at `/opt/rcloud/certs/` |
| `karpenter-provider-rafay` | `rafay-system` | Optional `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` via `envFrom` |

### edge-broker (control plane)

| Variable | Default | Description |
|----------|---------|-------------|
| `EDGE_BROKER_SERVER_PORT` | `5448` | mTLS listener port |
| `EDGE_BROKER_CERT_FOLDER` | `/etc/rcloud/certs` | TLS certificate directory |
| `EDGE_BROKER_REDIS_ADDR` | `admin-redis:6379` | Redis address |
| `EDGE_BROKER_REDIS_DB` | `3` | Redis database index |
| `EDGE_HOST` | `edgesrv.rcloud-admin.svc.cluster.local` | Edge service address |
| `EDGE_PORT` | `50701` | Edge service port |
| `INFRA_API_SERVER_ADDR` | `infra-apiserver:7000` | Infra API server address (v2 cluster resolution) |
| `WORKSPACE_SERVICE_ADDR` | `paas-api:6000` | PaaS workspace RPC service address |
| `COMPUTE_PROFILE_SERVICE_ADDR` | `paas-api:6001` | PaaS compute RPC service address (`PaasProfileService`), used by `GetKarpenterConfig` to read node SKUs. A separate listener from `WORKSPACE_SERVICE_ADDR`; unset ⇒ that one RPC returns `FailedPrecondition` |
| `KARPENTER_NODEPOOL_CPU_LIMIT` | `1000` | `spec.limits.cpu` on generated NodePools (blast-radius guard; the catalog has no per-pool maximum). Empty ⇒ omit |
| `KARPENTER_NODEPOOL_MEMORY_LIMIT` | `1000Gi` | `spec.limits.memory` on generated NodePools |
| `KARPENTER_NODEPOOL_GPU_LIMIT` | `16` | GPU limit on generated NodePools, applied only to GPU SKUs |
| `PAAS_SERVICE_USERMETA_ID` | `edge-broker` | Service identity for PaaS calls |
| `PAAS_SERVICE_USERNAME` | `edge-broker` | Username for PaaS calls |

### RafayNodeClass CRD (portable — same manifest on every cluster)

> Normally generated by the broker — see [§7.1](#71-catalog--rafaynodeclass-bootstrap). Write it by hand only with `KARPENTER_CONFIG_BOOTSTRAP=false`.

```yaml
apiVersion: karpenter.rafay.io/v1alpha1
kind: RafayNodeClass
metadata:
  name: oci-inst
spec:
  instanceTypes:
    - name: oci-inst          # must match "skuname" in Worker Node SKU catalog
      cpu: "8"
      memory: "16Gi"
      nvidia.com/gpu: "8"     # TEMPORARY — see below. Omit for SKUs with no accelerator
      architectures: ["amd64"]
      operatingSystems: ["linux"]
```

> **`nvidia.com/gpu` is TEMPORARY and gets removed.** It exists so that a pod requesting a GPU can
> be provisioned for at all: an instance type whose `Capacity` omits the extended resource is
> filtered out by the scheduler's *fits* check, so a `nvidia.com/gpu` pod never yields a NodeClaim
> and stays `Pending` with `Failed to schedule pod, no instance type has enough resources`. The
> broker fills the field for every SKU — its own `gpu_count` where the ComputeProfile declares one,
> otherwise a fixed `"8"` (`temporaryGPUCapacity`).
>
> Two reasons it is a stopgap rather than GPU support:
>
> - **The resource name is hardcoded**, so an `amd.com/gpu` SKU is advertised under the wrong key.
>   The broker already resolves the real name per SKU (`KarpenterNodeSku.GPUResourceName`); the
>   replacement should carry that through `instanceTypes` instead.
> - **The fallback covers SKUs with no accelerator**, because a ComputeProfile that declares no
>   `gpu_count` is indistinguishable from one that has no GPU. Such a node registers and then never
>   reports `nvidia.com/gpu` in its allocatable, so `RequestedResourcesRegistered` leaves its
>   NodeClaim at `Initialized=Unknown` **forever** — there is no Initialized timeout, and
>   `liveness.go` only reaps NodeClaims that fail to become *Registered*. The node is then never
>   disruptable and the pod never runs. A zero or absent count is dropped rather than advertised
>   ([`rafayInstanceTypesToKarpenter`](../pkg/cloudprovider/cloudprovider.go)) so a hand-written
>   NodeClass cannot walk into this, but the broker's blanket fallback can.
>
> The GPU device plugin must also tolerate whatever taints the catalog puts on the pool, or the same
> stuck-`Initialized` outcome follows on a node that really does have GPUs.
>
> Note the synthetic price still ignores accelerators, so a GPU
> SKU and a same-shape non-GPU SKU price identically. Consolidation only replaces a node with a
> strictly cheaper one, so a GPU node left holding cpu-only pods is never consolidated away.

To remove the whole mechanism, delete together: `InstanceTypeSpec.GPU`, the CRD property, the
`Capacity` entry and `temporaryGPUResourceName` in `rafayInstanceTypesToKarpenter`, and edge-broker's
`temporaryGPUCapacity` / `instanceTypeGPUCapacity` / `yamlInstanceType.GPU`.

### Headroom policy ConfigMap (per-cluster, optional)

See `examples/policy_configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: headroom-policy
  namespace: karpenter        # HEADROOM_CONFIG_NAMESPACE
data:
  policy: |
    pools:
      - name: worker-pool-amd # must match a Karpenter NodePool name
        cpu: 30%
        memory: 20%
        podCPU: 500m
        podMemory: 512Mi
        minPods: 2
```

---

## 14. Key Dependencies

| Module | Version | Role |
|--------|---------|------|
| `sigs.k8s.io/karpenter` | v1.14.1, **replace → fork `github.com/RafaySystems/karpenter-rafay`** (branch `rafay-release-v1.14.x`, pinned by commit pseudo-version) | Core framework: operator, controllers, `CloudProvider` interface. The fork raises `registrationTimeout` to 60 min (`liveness.go`). |
| `sigs.k8s.io/controller-runtime` | v0.23.3 | Reconciler infrastructure, Manager, typed client |
| `github.com/awslabs/operatorpkg` | (see go.mod) | Operator lifecycle, `status.Condition`, `controller.Controller` |
| `github.com/RafaySystems/edge-common` | pinned commit pseudo-version in `go.mod` (same commit as `edge-broker/go.mod`; fetched from GitHub at build time) | `rep.edge.v1` Karpenter batch protos (add/remove/poll/cancel) and generated Go |
| `google.golang.org/grpc` | v1.72.2 | gRPC client (`grpc.NewClient`, lazy connect) |
| `google.golang.org/protobuf` | v1.36.11 | Protobuf serialization |
| `k8s.io/client-go` | v0.35.1 | Kubernetes client |
| `k8s.io/api`, `k8s.io/apimachinery` | v0.35.1 | Aligned with client-go / Karpenter |
| `github.com/samber/lo` | v1.52.0 | Functional helpers (`Filter`, `Map`, `Assign`) |
| `github.com/google/uuid` | v1.6.0 | UUID v4 for auto-generated `STREAM_ID` and batch IDs |

---

## 15. Design Decisions

### One batch protocol for add, remove, and cancel

Both node-add and node-remove requests are batched via `KarpenterBatchService.BatchStreamOperations` (adds and removes as separate batches with the same windowing). When multiple pods become unschedulable — or the disruption controller retires several nodes — Karpenter issues concurrent `Create()`/`Delete()` calls; the `NodeBatcher` collects each kind into a single broker request (up to 10 nodes / 10s window), and the broker applies one bulk catalog mutation per batch. The old per-operation `KarpenterNodeService.StreamOperations` delete path was removed: it held a stream open for the full removal (up to an hour) and its pending-claim cancellation path was broken.

### ACK-based unblocking for both Create() and Delete()

`Create()` and `Delete()` block only until the broker acknowledges the batch (typically seconds), well within Karpenter's `launchTimeout` (5 minutes) — no stream is held open for the duration of the work. The two calls then converge by different routes:

- **Adds**: the real ProviderID is resolved later from the joined node by the `NodeProviderIDController`. Broker SUCCEEDED is not on the critical path.
- **Removes**: `Delete()` returns `nil` at ACK, Karpenter requeues every 5s and calls it again, and the batcher suppresses the duplicate sends. The call converges when the status poller records the remove operation SUCCEEDED, at which point `Delete()` returns `NodeClaimNotFoundError` — the only signal that releases the Node's termination finalizer. Returning `nil` forever (the original behaviour) left NodeClaims and Nodes `Terminating` indefinitely.

Node existence is deliberately **not** the completion signal for removes: Karpenter's own finalizer keeps the Node object alive until `Delete()` reports the instance gone, so polling for the Node's disappearance is circular and never converges.

### Failure feedback via status poller + failure handler

Because callers unblock at ACK, broker-side failures need an asynchronous path back into the cluster: the status poller maps terminal FAILED results to the `FailureHandler`, which deletes the still-pending NodeClaim (UID == operationID). Karpenter reprovisions within seconds instead of waiting out the 60-minute registration-timeout backstop.

SUCCEEDED results are **recorded**, not merely logged. For adds the record is incidental — registration is driven by the node actually joining. For **removes** it is the convergence signal that lets `Delete()` return `NodeClaimNotFoundError` and release the Node's termination finalizer.

### Best-effort cancellation for pending deletes

Deleting a NodeClaim whose node has not yet joined sends `cancel_ops` for the queued add. Cancellation is deliberately narrow: only ops still ACCEPTED at the broker are transitioned (CAS) to FAILED `"cancelled by client"` — an op that is already RUNNING has potentially mutated the catalog and is left to finish.

### Removals are catalog decrements (PaaS limitation)

The PaaS worker-node catalog is declarative per pool+SKU counts, so the broker applies removals as negative deltas (clamped at 0) and **PaaS chooses which physical machine is retired**. The node's ProviderID is carried on `KarpenterBatchNodeRemoveItem` so a targeted-removal API can be adopted later without a protocol change.

### Synthetic pricing for instance-type selection

The private cloud has no price list, so each instance type gets a synthetic relative cost (1.0 per vCPU + 0.125 per GiB of memory). `Create()` picks the cheapest compatible type (stable sort, NodeClass spec order on ties), and consolidation gets a gradient for replacing underutilized nodes with smaller ones.

### ProviderID resolved from Kubernetes Node, not broker response

Rather than relying on the broker to return a provider ID in the batch response, the provider reads `node.Spec.ProviderID` directly from the Kubernetes node once it joins. This decouples the provider from the broker's response format and uses the node's own identity as the canonical identifier.

### NodeProviderIDController: single-threaded with uncached reads

`MaxConcurrentReconciles: 1` serializes the read-decide-patch cycle for node assignment. `apiReader` (direct API server reads) ensures that a just-patched ProviderID is visible to the next reconcile, preventing two pending NodeClaims from claiming the same node even if the informer cache is stale. `CloudProvider.findNodeProviderID` applies the same discipline on the delete path.

### NodeClaims as sole source of truth

No in-memory state is load-bearing across a pod restart. NodeClaims in etcd are the sole source of truth. `NodeProviderIDController` recovers on startup by re-processing all existing NodeClaims with `rafay://pending/` ProviderID. Broker idempotency (deterministic operation IDs) ensures no duplicate provisioning.

### No retry on batch sends

`SendBatch` and `SendBatchRemove` use `callBrokerOnce`. Retrying a send that partially executed (e.g. broker accepted but the response was lost) would cause double-provisioning. On transport failure, recovery is via application-level retry paths (Karpenter re-calling `Create()`/`Delete()` with the same operation ID → broker deduplication). Read-only polls and idempotent cancels retry once on `Unavailable`.

### In-band rejection and expiry instead of stream errors

Queue-full at the broker answers `batch_rejected` and unknown batchIDs answer an empty `batch_status` — both keep the stream and connection healthy instead of failing them. The client converts repeated empty polls (or 2h of tracking) into per-item failures, so nothing hangs forever on a lost broker record.

### Headroom via per-pool Deployments

Proactive capacity is maintained by the headroom controller as one Deployment of negative-priority pause pods per pool — the standard cluster-overprovisioner pattern — instead of managing bare pods. The ReplicaSet replaces preempted placeholders automatically; the controller only computes replica counts (fraction of pool allocatable / per-pod slice) and reacts to config/node/deployment events. This requires `consolidationPolicy: WhenEmpty` on participating NodePools, since pause pods intentionally keep nodes non-empty.

### No broker query API

`GetNode` and `ListNodes` return sentinel errors; the provider falls back to listing Kubernetes `Node` objects filtered by `spec.providerID` prefix `rafay://`. The Kubernetes API is the ground truth for what is currently running.

### No drift detection

`IsDrifted` always returns `("", nil)`. Node lifecycle is managed entirely by Rafay's control plane.

### Parity with `edge-client`

The TLS certificate path, broker host extraction from cert Subject OU, port environment variable names, keepalive parameters (5-min ping / 30s timeout), max message size (20 MB), and `sessionid` UUID generation all mirror `edge-client` so the same certificate material and deployment patterns apply without additional configuration.

### Instance types validated at admission and at call time

The readiness controller marks a NodeClass `Ready=False` (reason `ValidationFailed`) when `spec.instanceTypes` is empty or malformed, blocking provisioning at the NodePool gate. `rafayInstanceTypesToKarpenter` additionally uses `resource.ParseQuantity` (not `MustParse`) and returns an error for malformed `cpu`, `memory` or `nvidia.com/gpu` values, surfacing misconfiguration cleanly rather than panicking the controller. Both checks cover the same fields on purpose: a quantity that readiness accepts but `GetInstanceTypes` rejects would leave the NodeClass `Ready=True` while every provisioning attempt failed.
