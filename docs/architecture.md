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

- **karpenter-provider-rafay** (this repo) — the Karpenter `CloudProvider` implementation plus six helper controllers (`nodeproviderid`, `rafaynodeclass`, `headroom`, `nodeconfig`, `nodeadoption`, `batchresume`), wired in `cmd/controller/main.go`. It bootstraps the cluster's `RafayNodeClass` / `NodePool` objects from edge-broker, adopts the worker nodes the platform built before Karpenter ran, then translates Karpenter's per-`NodeClaim` `Create`/`Delete` calls into node add/remove requests, batches them, and drives them to completion.
- **edge-broker** — a broker process reachable over mTLS gRPC that owns one in-memory batch queue and processor **per edge** (batches of one cluster run one at a time, different clusters in parallel; a per-cluster Redis lock serializes replicas). It turns batch add/remove requests into edits of the edge's **workspace compute-instance worker-node catalog** (oneclick clusters: a declarative PaaS object holding per-pool+SKU counts) or of the MKS `Cluster`'s per-pool `scaling.desired` (first-class clusters), then polls the platform until it converges.
- **edge-common** — shared gRPC plumbing and the hand-written `KarpenterBatchService` / `KarpenterConfigService` message/stream types used on the wire between the provider and the broker.

The model is **batch-over-broker with a declarative catalog**: the provider never calls a cloud "create instance" API directly. It edits counts in the PaaS catalog (increment to add, decrement to remove) via the broker; the PaaS materializes or retires physical machines to match. Because the catalog is count-based, the platform — **not** Karpenter — chooses which machine a decrement retires (see the targeted-removal limitation in [§4.2](#42-edge-broker-control-plane)).

All node lifecycle operations are routed through `rep.edge.v1.KarpenterBatchService.BatchStreamOperations`, using the same mTLS certificate material and dial conventions as the `edge-client` agent deployed inside every Rafay-managed cluster. The same stream also carries best-effort cancellation of queued operations. **NodeClaims in etcd are the sole source of truth**; no in-memory state is load-bearing across a pod restart, and idempotency is enforced at three layers (deterministic operationIDs, the batcher's in-flight set, and the broker's 24 h SUCCEEDED tombstone).

### Key Properties

| Property | Detail |
|----------|--------|
| **Add protocol** | `BatchStreamOperations` `batch_add` — up to 10 nodes per batch, 10s collection window |
| **Delete protocol** | `BatchStreamOperations` `batch_remove` — same batching, batched separately from adds |
| **Cancel protocol** | `BatchStreamOperations` `cancel_ops` — only still-queued (ACCEPTED) operations; `Delete()` calls it synchronously for a pending NodeClaim and acts on the answer |
| **Transport security** | mTLS — client certificate required; edge identity derived from cert Subject O |
| **State store (broker)** | Redis (per-op records: 15-min TTL while ACCEPTED, **2 h** once RUNNING and on a terminal FAILED, **24-h tombstone once SUCCEEDED**; batch index: 3 h; per-cluster processing lock: 2 h). Exception: the `batch queue full; try again` / `broker shutting down; retry` / `cluster locked by another broker; retry` FAILED writes keep the 15-min TTL |
| **Deadlines** | Adding a node can take up to **60 min** (the whole infrastructure is created). Broker add processing: **90 min**; remove: **60 min**; provider batch tracking: **3 h**; Karpenter registration timeout (fork): **60 min** |
| **Provisioning backend** | PaaS WorkspaceComputeInstance via `WorkspaceRPCService` (paas-api) for oneclick clusters; MKS `Cluster` `scaling.desired` via the MKS ClusterService for first-class clusters |
| **Idempotency** | Operation ID — NodeClaim UID for adds, NodeClaim UID + `"-remove"` for deletes — prevents duplicate catalog mutations |
| **ProviderID resolution** | `NodeProviderIDController` patches real `spec.providerID` from the joined node |
| **Targeted removal** | Not supported by PaaS today — removes are catalog count decrements; the platform chooses which physical machine is retired (the node's ProviderID is carried in the protocol for future targeted removal). `Delete()` therefore waits for the NodeClaim's own machine to stop before finalizing (see [§6.2](#62-scale-in-node-deprovisioning)) |
| **Expiry / drift** | Every broker-rendered NodePool sets `expireAfter: Never` and a Drifted-scoped `nodes: "0"` budget: nodes are never force-rotated, and drift is marked but never rolled (see [§7.1](#71-catalog--rafaynodeclass-bootstrap)) |

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
│   ├── confluence-architecture.md # Mermaid rendering of this document for Confluence
│   ├── crash_safety.md          # Crash-proof and idempotent design analysis
│   ├── headroom.md              # Headroom (overprovisioning) controller reference
│   ├── karpenter-rafay-internals.md  # Karpenter internals + bug-fix history
│   └── implementation/          # Batch add / batch remove design docs
│
├── examples/                    # Sample RafayNodeClass, NodePool, headroom policy, test pods
│
└── pkg/
    ├── apis/v1alpha1/           # RafayNodeClass type, spec/status, DeepCopy (hand-maintained with config/crd)
    ├── broker/                  # gRPC dial helpers, TLS credential loading, session ID
    ├── cloudprovider/           # Karpenter CloudProvider implementation, batch failure handler, PoolBackoff
    ├── controllers/
    │   ├── batchresume/         # Re-registers pending NodeClaims' add batches with the batcher after a restart
    │   ├── headroom/            # Per-pool overprovisioning Deployments of pause pods
    │   ├── nodeadoption/        # Creates a NodeClaim per pre-existing worker node in a pool
    │   ├── nodeconfig/          # Applies RafayNodeClass/NodePool fetched from edge-broker at startup and every sync interval
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
                       │  KarpenterConfigService.GetKarpenterConfig      │
                       │  Per-edge batch queue (one batch at a time per  │
                       │  edge, edges in parallel; per-cluster lock)     │
                       │  Redis: operation state (15 min/2 h/24 h TTLs)  │
                       └────────────────────────┬──────────────────────┘
                                                │
                       ┌────────────────────────▼──────────────────────┐
                       │  KarpenterBackend (oneclick | first-class)      │
                       │  ├── edgesrv (edge registry)                   │
                       │  ├── paas-api (WorkspaceRPCService /            │
                       │  │             MKS ClusterService)              │
                       │  └── IaaS / Cloud (VM/node pool)                │
                       └───────────────────────────────────────────────┘
```

---

## 4. Component Descriptions

### 4.1 karpenter-provider-rafay (in-cluster)

Runs as a Kubernetes Deployment inside the managed cluster. Implements the Karpenter `CloudProvider` interface.

| Sub-component | Package | Responsibility |
|---------------|---------|----------------|
| **CloudProvider** | `pkg/cloudprovider` | `Create`, `Delete`, `Get`, `List`, `GetInstanceTypes`. `Create` and `Delete` both return immediately after broker ACK. `Create` selects the **cheapest compatible instance type** (synthetic price) and returns a synthetic pending ProviderID; `Delete` converges across repeated calls on the broker's SUCCEEDED plus the machine actually stopping, and records Warning events on the NodePool for the odd outcomes (`NodeNotRetired`, `NodeRetiredExternally`, `RemoveRetiredOtherMachine`). |
| **Batch failure handler** | `pkg/cloudprovider` | `NewBatchFailureHandler` — invoked by the batcher on a terminal FAILED result. Every FAILED add is first recorded per operation in `PoolBackoff` (`MarkFailedOp`, kept 2 h) so a later `Delete()` of the pending NodeClaim knows no machine is coming. On a FAILED **add** it deletes the matching pending NodeClaim so Karpenter reprovisions immediately instead of waiting out the 60-min registration timeout. Some add failures are permanent — a detail starting `pool at maximum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling` or `pool precondition` (`IsPermanentRefusalDetail`): the handler then first marks the NodePool in the shared `PoolBackoff` (default 5 min, `RAFAY_POOL_AT_MAX_COOLDOWN`), records a Warning event on it (`PoolAtPlatformMaximum` for the maximum, `PoolRefusedByPlatform` otherwise; message = the broker's detail), and `GetInstanceTypes` reports that pool's offerings unavailable for the cooldown, so Karpenter leaves the pods pending instead of re-asking every few minutes. FAILED **remove** operations are log-only unless permanently refused (then recorded so `Delete()` converges instead of re-sending every 5 s). |
| **Pool backoff** | `pkg/cloudprovider` | `PoolBackoff` — per-NodePool "held back until" store shared by the failure handler (marks) and `GetInstanceTypes` (withholds offerings), plus the per-operation FAILED records `Delete()` reads. `IsPoolAtMaxDetail`, `IsPermanentRefusalDetail` and `IsNotRetiredDetail` (`node not retired` prefix) are the contract with the broker's detail strings. See [Pool maximum: three layers](#pool-maximum-three-layers). |
| **NodeProviderIDController** | `pkg/controllers/nodeproviderid` | Watches NodeClaims with `rafay://pending/` ProviderID + real Rafay nodes. Labels the node `karpenter.sh/registered=true`, then patches the real ProviderID once the node joins. Single-threaded (`MaxConcurrentReconciles: 1`) and serialized with adoption and `Delete()` by `cloudprovider.NodeOwnershipMu`. |
| **NodeAdoptionController** | `pkg/controllers/nodeadoption` | Creates a NodeClaim (annotated `karpenter.rafay.io/adopted-provider-id`, no broker call) for every Ready worker node the platform built before Karpenter ran, so the pool's existing nodes count towards its limits and can be consolidated. Skips pools annotated `karpenter.rafay.io/auto-scaling=false`. Disabled by `KARPENTER_ADOPT_EXISTING_NODES=false`. See [§5](#pkgcontrollersnodeadoption--existing-node-adoption-controller). |
| **BrokerClient** | `pkg/rafay` | One shared `*grpc.ClientConn`, never closed on an RPC error. `SendBatch`, `SendBatchRemove`, `PollBatchStatus`, `CancelOperations` — each a short-lived stream on `BatchStreamOperations` with a 30s deadline — plus `GetKarpenterConfig`, a unary call on `KarpenterConfigService` (90s deadline: the broker fans out to PaaS per node SKU). |
| **Node config controller** | `pkg/controllers/nodeconfig` | Leader-elected runnable. Fetches this cluster's `RafayNodeClass` / `NodePool` manifests from edge-broker and server-side-applies them (classes first) at startup, then every `KARPENTER_CONFIG_SYNC_INTERVAL`. **Re-applies every interval** — an unchanged revision only lowers the log line to `V(2)` — so a deleted or hand-edited managed object is restored; applies the **inert** shape when the compute instance has autoscaling off (the broker still ships every pool); **never prunes**. A `NotFound` whose message carries `karpenter config unavailable for this edge` ("this cluster has no Karpenter config" — no workspace compute instance, or no worker-pool catalog) is a quiet no-op sync re-checked at the normal interval; every other error is retried on a 5 s → 2 min backoff. See [§7.1](#71-catalog--rafaynodeclass-bootstrap). |
| **NodeBatcher** | `pkg/rafay` | Collects `Create()`/`Delete()` requests into batches (up to 10 / 10s, adds and removes partitioned into separate batches), unblocks callers at broker ACK, polls status every 30 s (no initial delay) for SUCCEEDED/FAILED feedback, tracks a batch for up to 3 h. Deduplicates retries by `operationID`. |
| **BatchResume controller** | `pkg/controllers/batchresume` | One leader-only pass at startup that hands the batcher back the add batches a previous process sent (from the `karpenter.rafay.io/batch-id` annotation `Create()` stamps), so a FAILED add is still reaped in minutes after a restart. See [§5](#pkgcontrollersbatchresume--add-batch-resume-on-restart). |
| **RafayNodeClass controller** | `pkg/controllers/rafaynodeclass` | Validates `spec.instanceTypes` and sets `status.conditions[Ready]` (True, or False with reason `ValidationFailed`) so Karpenter's NodePool controller can schedule. |
| **Headroom controller** | `pkg/controllers/headroom` | Maintains one Deployment of low-priority pause pods per configured NodePool (`headroom-<pool>`) for proactive scale-out; holds it at 0 replicas while the pool is inert. See [§5](#pkgcontrollersheadroom--overprovisioning-buffer-controller). |
| **RafayNodeClass CRD** | `pkg/apis/v1alpha1` | Defines the node shapes available for provisioning (`spec.instanceTypes`; each entry requires `name`, `cpu`, `memory`; `kubectl get karpenter` lists the class). It carries **no** Rafay cluster/project identity — that comes only from the controller's `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` env. |

### 4.2 edge-broker (control plane)

Central gRPC relay in the Rafay control plane.

**Two listeners:**

| Port | TLS | Karpenter service |
|------|-----|-------------------|
| `5448` (`EDGE_BROKER_SERVER_PORT`) | mTLS (client cert required) | `KarpenterBatchService` + `KarpenterConfigService` — **the only usable port for node operations** |
| `5449` (`EDGE_BROKER_INTERNAL_PORT`) | Plaintext (internal only) | `EdgeBrokerService` (platform-internal callers only; no longer on the mTLS listener); `KarpenterBatchService` / `KarpenterConfigService` are registered but **cannot serve node operations** — see below |

> ⚠️ **`KarpenterBatchService` fundamentally cannot be served over the plaintext listener.** Every `BatchStreamOperations` handler needs the caller's **edge id**, and that id is derived from the **mTLS client certificate** (Subject Organization — `common.GetEdgeClientInfo`). On a plaintext connection there is no peer certificate, so the edge id cannot be established and the handler rejects the stream with `ErrorNoClientID` (`"NO CLIENT ID"`) — it maps *any* `GetEdgeClientInfo` failure to `ErrorNoClientID` and does not surface the underlying peer error (`ErrorInvalidPeer`) to the client. `GetKarpenterConfig` answers the same situation with gRPC `Unauthenticated` and the same text. Node operations must use the mTLS listener (`5448`); `EDGE_BROKER_GRPC_INSECURE=true` pointed at `:5449` will not work for them.
>
> This used to be worse than a clean failure: `GetEdgeClientInfo` performed an unchecked `p.AuthInfo.(credentials.TLSInfo)` type assertion, and on a plaintext connection `AuthInfo` is `nil` — so opening `BatchStreamOperations` on `:5449` **panicked and crashed the whole broker process** (gRPC does not recover handler panics), an unauthenticated DoS. It now returns `ErrorNoClientID` cleanly, and both gRPC servers install panic-recovery interceptors.

The old per-operation `KarpenterNodeService.StreamOperations` protocol has been **removed** — adds, removes, status polls, and cancels all flow through `BatchStreamOperations`.

**`BatchStreamOperations` request dispatch (client→broker union, field tags 1–4):**

| Frame | Tag | Handler |
|-------|-----|---------|
| `batch_add` (`KarpenterBatchNodeAddRequest`) | 1 | Classifies the whole request before writing anything: a batch of more than **64** nodes → `batch_rejected` (`"batch too large: <n> nodes, max 64"`); a `batch_id` owned by a **different edge** → `batch_rejected` (`"batch id belongs to another edge"`); this edge's queue already holding **16** batches → `batch_rejected` (`"batch queue full"`) — all **in-band, keeping the stream alive, with nothing written**. Otherwise: per-op idempotency check against Redis, writes ACCEPTED records (15-min TTL, stamped with `edge_id`, `project_id`, `cluster_id`), writes the batchID→opIDs index, enqueues the new/retry nodes on **this edge's** queue, replies `batch_accepted`. In the residual race where the queue fills between the check and the enqueue, only the ops this call itself wrote are CAS'd to FAILED (`"batch queue full; try again"`, 15-min TTL). |
| `status_poll` (`KarpenterBatchStatusPoll`) | 2 | Reads the batch index + per-op records, replies `batch_status` with `nodeResults[]`. Unknown/expired batchID → **empty `batch_status`** (stream stays alive); an expired op record inside a known batch → FAILED `"operation record expired"`. |
| `batch_remove` (`KarpenterBatchNodeRemoveRequest`) | 3 | Mirrors `batch_add` exactly, with op records written as kind `"delete"`. Same idempotency rules, same per-edge queue, same `batch_accepted`/`batch_rejected` replies. `provider_id` may be empty (untargeted) for a NodeClaim whose machine never registered. |
| `cancel_ops` (`KarpenterBatchOperationCancel`) | 4 | A Redis compare-and-set transitions each op (first 64 ids) from ACCEPTED → FAILED (`"cancelled by client"`). RUNNING and terminal ops are untouched. Replies `cancel_ack` listing the ops actually cancelled. |

**Broker→client union (field tags 1–4):** `batch_accepted`, `batch_status`, `batch_rejected`, `cancel_ack`.

**Batch processor:** one in-memory FIFO and one processor goroutine **per edge** (`KarpenterBatchQueue`; capacity 16 waiting batches per edge, the batch in flight not counted; processors start on the edge's first batch and are reaped after 1 h idle). Batches of one edge run **one at a time, FIFO**; different edges run **in parallel** — head-of-line blocking exists only within an edge. Across broker replicas the processor first takes a per-cluster Redis lock (`/edge/karpenter/lock/<edgeID>`, 2 h TTL, retried every 15 s on its own 90-min budget; a lock whose holder's liveness beacon is gone is taken over). For each batch it CASes every still-ACCEPTED op to RUNNING (claiming it and extending the Redis TTL to 2 h), groups ops by `{pool, sku}`, and hands the deltas to the edge's `KarpenterBackend` — **oneclick**: one bulk catalog mutation (positive deltas for adds, negative for removes; rows never created, an add past `maxNodeCount` trimmed, a decrement below `minNodeCount`/0 refused), `Apply` + `Publish`, then a 30 s poll of the compute instance; **first-class**: `scaling.desired = live count ± delta` on the MKS `Cluster`, `ApplyCluster`, then a 30 s poll of `GetClusterStatus`. The whole run is bounded by **one** deadline: `karpenterBatchAddTimeout` (**90 min**) for adds, `karpenterBatchRemoveTimeout` (**60 min**) for removes — the settle poll has no separate deadline. A busy platform ("operation already in progress") is waited out, not failed. Ops are then written SUCCEEDED (adds carry provider IDs; removes carry none) or FAILED with the error detail — with three refinements: a **remove is tombstoned SUCCEEDED the moment the decrement is published**, before the settle poll (a later settle failure keeps SUCCEEDED with detail `catalog decremented; compute instance did not settle`); a failure **after the platform commit** never yields a retryable FAILED (ops become SUCCEEDED, detail `committed; …`); and a **permanent refusal** is reported as FAILED `pool at maximum` / `pool not found` / `pool sku mismatch` / `pool not auto-scaling` / `pool precondition` for adds (the part of the batch that fits is still applied) and as SUCCEEDED `node not retired: <reason>` for removes (nothing written). On restart, `sweepOrphanedKarpenterOps` re-queues every still-ACCEPTED op onto its edge's queue within seconds; RUNNING ops are left to their TTL because the catalog write may already have landed.

> **Platform limitation — no targeted removal.** The PaaS worker-node catalog is declarative (per pool+SKU counts), so a remove is a count decrement and **PaaS chooses which physical machine is retired** — not necessarily the node Karpenter selected. The `provider_id` field on `KarpenterBatchNodeRemoveItem` is carried so a future targeted-removal API can be adopted without a protocol change. Until then `Delete()` compensates on the provider side (see [§6.2](#62-scale-in-node-deprovisioning)).

### 4.3 Redis — Operation State Store

Three key families:

**Per-operation records** under `/edge/karpenter/nodeop/<operationID>`:

```json
{
  "kind":         "add | delete",
  "state":        1,
  "detail":       "human-readable status message",
  "provider_ids": ["rafay://nodepoolname/sku_name/hostname"],
  "edge_id":      "7dkgjkx",
  "project_id":   "…",
  "cluster_id":   "…"
}
```

| Value | Constant | Meaning |
|-------|----------|---------|
| 0 | `UNKNOWN` | Never written by the broker; a record in this state is corrupt or resurrected — never re-accepted as a retry, never re-created (`patchNodeOp` returns `redis.Nil` for a missing record) |
| 1 | `ACCEPTED` | Request received, queued for processing |
| 2 | `RUNNING` | Batch processor active, catalog mutation in progress |
| 3 | `SUCCEEDED` | Operation complete (adds: provider IDs available; removes: written at publish; also `committed; …` and `node not retired: …` outcomes) |
| 4 | `FAILED` | Operation failed, detail contains error (also used for client cancellations, permanent add refusals, `broker shutting down; retry`, `cluster locked by another broker; retry`, `batch processor panicked: …; retry`) |

TTL strategy:

- **ACCEPTED** records use a **short 15-minute TTL** (`karpenterBatchAcceptedTTL`). A broker restart no longer strands them (the startup sweep re-queues them within seconds), and while a batch waits in its edge's queue the broker's per-queue keepalive re-arms the TTL every 5 minutes, so an op only expires (`"operation record expired"` on the next poll) when the broker holding it died without recovery. The processor extends the TTL to **2 hours** (`karpenterBatchRunningTTL` = add timeout + 30 min) when it CASes ACCEPTED→RUNNING.
- **SUCCEEDED** records are long-lived **tombstones** with a **24-hour TTL** (`karpenterBatchSucceededTTL`), and every duplicate send of an already-SUCCEEDED operationID **refreshes that TTL**. This is a correctness requirement, not a cache: clients re-send a deterministic operationID (`<uid>-remove`) for the whole life of their retry loop, so a tombstone that expired would make the next retry look like a brand-new operation — it would be ACCEPTED again and the processor would apply the catalog delta a **second** time. For a removal that retires an extra, healthy machine. A tombstone that vanished before the processor could write it is rewritten in full from the queue item.
- **FAILED** records are **not** tombstoned: a FAILED operationID must stay retryable (a re-send rewrites it to ACCEPTED), and letting it expire is equivalent to re-accepting it. Terminal FAILED writes by the processor and `cancel_ops` carry the 2-hour RUNNING TTL; only the `batch queue full; try again`, `broker shutting down; retry` and `cluster locked by another broker; retry` rejections keep the 15-minute TTL.

**Batch index records** under `/edge/karpenter/batchop/<batchID>` (`{"operation_ids": [...], "edge_id": "…"}`, **3-hour TTL**) map a batch ID to its operation IDs so status polls can report every node in the batch while any op of it could still be alive.

**Cluster lock** under `/edge/karpenter/lock/<edgeID>` (value = owner token `brokerID/batchID/nanos`, **2-hour TTL**) serializes the catalog read-modify-write per cluster across broker replicas.

**Edge scoping**: every `nodeop` and `batchop` record carries the `edge_id` of the authenticated stream that created it. Status polls and cancels only act on records belonging to the calling edge — another edge's batch ID is answered exactly like an unknown one (empty result list), and its operations are never cancelled. Records with an **empty** `edge_id` were written by an older broker and stay visible to everyone, so operations in flight across a broker upgrade are unaffected (the Redis key schema is unchanged).

State transitions that race other writers go through a **compare-and-set helper** built on a go-redis `WATCH/MULTI/EXEC` optimistic transaction, so a concurrent writer invalidates the transaction instead of being clobbered. Both racy paths use it: `cancel_ops` and the **queue-full rejection** (which marks ops FAILED only if they are still ACCEPTED, so it cannot clobber an op a processor concurrently claimed as RUNNING).

### 4.4 KarpenterBackend — Provisioning Backend

`karpenter_backend.go` defines the one seam to the platform (`LoadPools` / `ResolveSkus` for the config RPC, `AddNodes` / `RemoveNodes` for the batch processor), selected per edge by `karpenterBackendFor`:

- **oneclick** (`karpenter_backend_oneclick.go`, `karpenter_workspace.go`) translates a batch into **one** **WorkspaceComputeInstance** catalog mutation via `WorkspaceRPCService` (paas-api): `Get → bulk increment/decrement → Apply → Publish`, then a 30 s poll loop until the compute instance reaches a terminal state (every terminal state ends the poll with the platform's reason). The commit point is `Apply`+`Publish`; an add whose run ends FAILED has its increment restored so the reprovision is a single increment. See [§7 Worker Node SKU Catalog](#7-worker-node-sku-catalog) for the catalog data model.
- **first-class** (`karpenter_backend_firstclass.go`) edits `scaling.desired` of the MKS `Cluster`'s automatic pools (`desired = live count ± delta`, from a `GetClusterStatus` snapshot), `ApplyCluster`, then polls `GetClusterStatus` until the hostnames appear or vanish. Chosen when the MKS ClusterService reports a `Cluster` object for the edge (the edge record's `FirstClass` marker is only an operator fallback).

Both apply the [cross-repo contract](#42-edge-broker-control-plane) on permanent refusals and commit points.

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
| 2 | Reads all config from env vars; logs an Info line if `RAFAY_CLUSTER_ID` or `RAFAY_PROJECT_ID` are unset (provisioning is unaffected — the broker identifies the cluster from the mTLS certificate); invalid port/duration values log a warning and fall back to the default instead of being silently ignored |
| 3 | Resolves **edge identity** from TLS client cert Subject Organization (O) |
| 4 | Constructs `rafay.BrokerClient` with resolved connection params |
| 5 | Registers the **batch failure handler** (`cloudprovider.NewBatchFailureHandler`) on the batcher — must happen **before** the batcher starts polling |
| 6 | Calls `rafayClient.StartBatcher(ctx)` — launches `NodeBatcher` sender + poller goroutines |
| 7 | Builds `CloudProvider` (with both the cached client and the uncached `apiReader`, the shared `PoolBackoff`, `WithEventRecorder(op.EventRecorder)` so `Delete()`'s Warning events reach the NodePool, and `WithRemoveSettleWindow(RAFAY_REMOVE_SETTLE_WINDOW)`), wraps with `metrics.Decorate`, builds `state.NewCluster` |
| 8 | Resolves headroom namespaces from `HEADROOM_NAMESPACE` / `HEADROOM_CONFIG_NAMESPACE` |
| 9 | Registers all controllers: `rafaynodeclass`, `nodeproviderid`, `headroom`, `nodeadoption` (unless `KARPENTER_ADOPT_EXISTING_NODES=false`), `nodeconfig` (unless `KARPENTER_CONFIG_BOOTSTRAP=false`), `batchresume`, plus upstream Karpenter controllers |
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
│       ├── nvidia.com/gpu   string             # TEMPORARY accelerator capacity, e.g. "8"; only for SKUs that really have GPUs
│       ├── zone             string             # topology.kubernetes.io/zone (default: "default")
│       ├── architectures    []string           # kubernetes.io/arch values (default ["amd64"] when omitted)
│       └── operatingSystems []string           # kubernetes.io/os values (default ["linux"] when omitted)
└── status
    └── conditions           []Condition        # Ready=True once validated; Ready=False/ValidationFailed otherwise
```

`spec.instanceTypes` is **required**, and the CRD schema requires `name`, `cpu` and `memory` on every entry (an entry missing one is rejected at apply time). The readiness controller marks the NodeClass `Ready=False` (reason `ValidationFailed`) when the list is empty or any entry has an empty name or unparseable cpu/memory/`nvidia.com/gpu`, and `GetInstanceTypes` returns an error for an empty list. The CRD (`config/crd/karpenter.rafay.io_rafaynodeclasses.yaml`, category `karpenter`) and `zz_generated.deepcopy.go` are hand-maintained; `TestMsCRDSchemaMatchesGoTypes` pins them to the Go types.

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
└── statusPoller goroutine (ticker: 30s; every tracked batch is polled on the next tick after
     │                       its ACK — there is deliberately no initial delay)
     └── pollAll → BrokerClient.PollBatchStatus per in-progress batch
          ├── SUCCEEDED       → record in `succeeded` (with detail + provider IDs), clear in-flight
          │                       (this is how Delete() learns a removal actually completed)
          ├── FAILED          → invoke FailureHandler(operationID, kind, detail), clear in-flight
          │                       (so a retry can re-send the operation)
          ├── ACCEPTED/RUNNING → keep in inProgress, poll next tick
          ├── unrecognised state → logged once, treated as still in progress
          ├── 3 consecutive empty responses (unknown batchID at broker)
          │     → drop batch, every remaining item FAILED "batch expired at broker"
          └── batch older than 3h (maxBatchAge) → same expiry treatment
```

**ACK is the unblocking event**: `Create()`/`Delete()` return as soon as the broker acknowledges the batch — they do **not** block until SUCCEEDED. But for removes the batcher does not forget the operation at ACK: `Succeeded(operationID)` / `SucceededResult` report whether the broker has confirmed the operation reached SUCCEEDED (and with which detail and provider IDs), and `CloudProvider.Delete` polls it on each reconcile to decide when the removal is complete (see [§6.2](#62-scale-in-node-deprovisioning)). Entries are pruned after `succeededRetention` (2h); the prune runs on every poller tick, so the retention holds on a quiet cluster too.

**No initial poll delay**: a remove's SUCCEEDED tombstone is written the moment the decrement is published (seconds after ACK), and an add's permanent refusal is decided when the broker claims the batch — seconds after ACK on an idle edge. Both must reach `Delete()` / the failure handler within one poll interval, so every batch is polled on the next 30 s tick. A node that takes up to 60 minutes to land costs a few read-only polls that report nothing terminal.

**Batch tracking horizon**: `maxBatchAge` is **3 h**. Adding a node can take up to 60 minutes and the broker's own add deadline is 90 minutes, so the client-side horizon must stay comfortably above both — an add that is merely slow must never be reported as FAILED (which deletes the pending NodeClaim). Never shorten it.

**In-flight suppression**: Karpenter's termination controller re-invokes `Delete()` **every 5 seconds** for the entire life of a node removal. Every operationID the broker has ACKed and not yet driven to a terminal state is held in the `inFlight` set; enqueueing one again resolves the caller **immediately** instead of sending a duplicate batch. Without this, each 5s reconcile would push another remove batch at the broker, flooding the edge's 16-slot queue with hundreds of copies of the same removal. The caller's contract — "queued at the broker" — is already satisfied, so resolving immediately is correct. An operation is dropped from the set as soon as it goes terminal (SUCCEEDED or FAILED) or its batch expires, so a genuine retry after a failure is never suppressed; `opBatch` is kept in step with the set on every path.

**Deduplication and multi-waiter fan-out**: `Enqueue(operationID, req)` / `EnqueueRemove(...)` check the `pending` map first. `pending` maps an operationID to a **slice** of result channels: the first caller queues the `batchItem`, and every concurrent or retrying caller for the same operationID **appends its own channel** and is resolved together with the others when the result arrives. (With a single shared channel only one waiter could consume the buffered result; the rest would block until their context was cancelled.) The pending entry is removed *before* the results are delivered, so a fast retry starts fresh rather than joining a list that is already being drained. Enqueue and broker-ACK are atomic per operationID — both hold `mu` across their `pendingMu` section (lock order `mu → pendingMu`) — so a second concurrent `Create()`/`Delete()` for the same NodeClaim during the ACK either joins the ACKed waiters or is short-circuited as in flight, never queued as a duplicate batch.

**Cancellation**: `Cancel(ctx, operationID) (applied bool, err error)` sends `CancelOperations` (10s timeout) and reports whether the broker actually cancelled the op. Only ops still ACCEPTED at the broker are cancelled; `Delete()` decides what to do with a pending NodeClaim from the answer (see [`pkg/cloudprovider`](#pkgcloudprovider--karpenter-interface-implementation)).

**Testability**: the batcher holds its broker as the small unexported `batchBroker` interface (`SendBatch`, `SendBatchRemove`, `PollBatchStatus`, `CancelOperations`) — a subset of `*BrokerClient`. Production code still passes the concrete `*BrokerClient` via `NewNodeBatcher`; unit tests (`batcher_test.go`) substitute a scripted mock so no real gRPC is needed.

#### `BrokerClient` (`brokerclient.go`)

One shared `*grpc.ClientConn` (lazy-initialized, mutex-protected). **Every broker RPC carries a 30-second deadline** (`brokerCallTimeout`) — these are short request/response exchanges; long-running work is tracked via status polling.

- **`SendBatch` / `SendBatchRemove`** — open a `BatchStreamOperations` stream, send the add/remove request, receive `KarpenterBatchAccepted{batchId}` (or `KarpenterBatchRejected` → `ErrBatchRejected`), return batchID. **No retry** (`callBrokerOnce`) — re-issuing could duplicate catalog mutations.
- **`PollBatchStatus`** — sends `KarpenterBatchStatusPoll{batchId}`, receives `KarpenterBatchStatusResponse{nodeResults[]}`. Retries **once** on `codes.Unavailable` (`callBroker`) — polls are read-only and safe to retry.
- **`CancelOperations`** — sends `KarpenterBatchOperationCancel{operationIds[]}`, receives `KarpenterBatchCancelAck{cancelledOperationIds[]}`. Retries once on `codes.Unavailable` — cancels are idempotent at the broker.
- **`GetKarpenterConfig`** — unary call on `KarpenterConfigService` (90 s deadline); retries once on `codes.Unavailable`.
- **Session routing** via gRPC metadata key `sessionid` = `STREAM_ID`.
- **The connection is never closed on an RPC failure.** One `*grpc.ClientConn` is shared by the batch sender, the status poller, cancels and the config sync; closing it would abort every other goroutine's in-flight stream with `codes.Canceled` (a send whose batch the broker had already ACCEPTED was reported as failed). On `codes.Unavailable` `callBroker` calls `ResetConnectBackoff()` and retries once **on the same connection** (log: `edge-broker: RPC failed (...), retrying once`); gRPC reconnects by itself after a transport failure. `closeConn` is only used by the shutdown/test hook.

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
  ├─ SUCCEEDED  →  providerIds[] available for adds (removes carry none today);
  │                detail may say "committed; …" (write landed, settle unknown) or
  │                "node not retired: …" (remove refused for good, nothing written)
  ├─ FAILED     →  detail string contains error (incl. "cancelled by client",
  │                "operation record expired", "batch queue full; try again",
  │                "broker shutting down; retry", "cluster locked by another broker; retry",
  │                and the permanent add refusals "pool at maximum", "pool not found",
  │                "pool sku mismatch", "pool not auto-scaling", "pool precondition")
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
| `Create(NodeClaim)` | Resolves `RafayNodeClass` → filters compatible instance types → **picks the cheapest** (synthetic price; stable sort keeps NodeClass spec order on ties) → enqueues to `NodeBatcher` (operationID = NodeClaim UID) → **blocks until broker ACK** → returns `NodeClaim` with `status.providerID = "rafay://pending/<uid>"`, the `karpenter.rafay.io/batch-id` annotation, capacity, allocatable and labels populated. The `NodeProviderIDController` later patches the real ProviderID. **Adopted NodeClaims short-circuit**: a NodeClaim annotated `karpenter.rafay.io/adopted-provider-id` describes a machine that is already running, so no broker call is made and that ProviderID is returned directly (see [§5](#pkgcontrollersnodeadoption--existing-node-adoption-controller)). A missing, not-Ready or empty/unparseable `RafayNodeClass` is reported as `NodeClassNotReadyError`, so Karpenter drops the NodeClaim at once instead of retrying for the 5-minute launch timeout; other errors stay plain so the launch is retried. |
| `Delete(NodeClaim)` | **Drives the removal to completion across repeated calls.** For a real ProviderID: (1) if the batcher reports `<uid>-remove` SUCCEEDED, converge (see below); (2) before the **first** remove is sent, look up the Node carrying the ProviderID (cached `spec.providerID` index) — no Node at all, or a Terminating Node whose kubelet has stopped that was deleted no later than the NodeClaim and not by Karpenter's disruption queue (`retiredExternally`; event `NodeRetiredExternally`) means the machine is already gone and `NodeClaimNotFoundError` is returned **without** a remove (a remove would retire one more healthy machine); (3) otherwise enqueue a batch remove (operationID = NodeClaim UID + `"-remove"`, carrying the ProviderID), **return `nil` at broker ACK**, and Karpenter requeues and calls `Delete` again in 5s — in-flight suppression makes those repeat calls cheap. **Convergence on SUCCEEDED**: a detail starting `node not retired` (the platform refused the count change) converges at once with a `NodeNotRetired` Warning event (the machine keeps running without a Node object; restart its kubelet or re-register it and adoption takes it back); a SUCCEEDED that names provider IDs not including this NodeClaim's, or an **untargeted** SUCCEEDED whose Node is still `Ready` after the **remove settle window** (`RAFAY_REMOVE_SETTLE_WINDOW`, default and floor 60 min, measured from the first time this process saw the SUCCEEDED), means the platform retired a different machine: event `RemoveRetiredOtherMachine`, annotation `karpenter.rafay.io/remove-retired-other` (persisted, so the verdict survives restarts and the 24 h tombstone), and the NodeClaim is held Terminating until no Node carries its ID — the remove is never re-sent (operator runbook: remove the Node's `karpenter.sh/termination` finalizer once the machine is retired or re-registered); otherwise `NodeClaimNotFoundError` follows as soon as the Node is gone or not Ready. A **pending** ProviderID is resolved by `resolvePendingRemoval` from the add's own state (see below). |
| `Get(providerID)` | For `rafay://pending/` prefix: returns minimal NodeClaim (provisioning in progress). Otherwise: the k8s Node carrying the ID, through the cache's `spec.providerID` index. **Not** the termination completion signal — see [§6.2](#62-scale-in-node-deprovisioning); Karpenter's dead-instance short-circuit in the termination controller cannot fire in broker mode because the Node it asks about is pinned by the termination finalizer, and `Get` must not guess from readiness (a transiently NotReady machine is alive). |
| `List()` | `ListNodes` unsupported → falls back to listing **every** k8s Node whose `spec.providerID` carries the `rafay://` prefix. There is deliberately **no cluster-ID filter** — see the note below. |
| `GetInstanceTypes(NodePool)` | Returns `RafayNodeClass.spec.instanceTypes` parsed and validated. Each type declares every well-known label (arch defaults to `amd64`, os to `linux`, zone to `default`, region as `Exists`), `pods: 110`, `nodes: 1` (so `limits.nodes` holds within a scheduling round), the temporary `nvidia.com/gpu` capacity only when the spec's count is positive, and an **overhead** (`RAFAY_VM_MEMORY_OVERHEAD_PERCENT` 7.5 % of memory as system-reserved, `RAFAY_KUBE_RESERVED_CPU` 80m / `RAFAY_KUBE_RESERVED_MEMORY` 255Mi as kube-reserved, 100Mi eviction threshold): capacity stays nominal, allocatable drops, so a pod requesting exactly the SKU's nominal size selects the next size up. Each offering carries a **synthetic price = 1.0 per vCPU + 0.125 per GiB of memory** so smallest-fit selection and consolidation have a gradient. Pools held in `PoolBackoff` report their offerings unavailable. |
| `IsDrifted(NodeClaim)` | Always `("", nil)` — no **cloud-provider** drift. Karpenter core's own drift checks still run, and run *before* this hook is consulted: static drift (`karpenter.sh/nodepool-hash` mismatch — any NodePool template edit), requirements drift and `instanceTypeNotFound`. A drifted NodeClaim is marked but never rolled on broker-rendered pools (Drifted-scoped `nodes: "0"` budget, [§7.1](#71-catalog--rafaynodeclass-bootstrap)). |
| `Name()` | `"rafay"` |

Cluster/project IDs for broker requests have a single source: `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID`, read at startup and captured into `CloudProvider.clusterID` / `.projectID` by `NewCloudProvider`. Both `Create` and `Delete` stamp those values on every `AddNodesRequest` / `RemoveNodesRequest` verbatim. There is no per-NodeClass override and no precedence rule to reason about. Both are optional: the broker resolves the cluster from the mTLS certificate and never widens its lookup by the `project_id` hint.

**`resolvePendingRemoval`** — what `Delete` does for a NodeClaim whose ProviderID is still `rafay://pending/<uid>`. Only the add's own state decides, never a Node that merely matches the pool/SKU (such a node may belong to another pending NodeClaim or to an operator):

1. A remove already ACKed for this NodeClaim in this process is re-sent as is (no LISTs).
2. The add **SUCCEEDED** at the broker: a machine exists or is about to register. If a Node can be resolved (`findNodeProviderID`), its ID is patched onto the NodeClaim first — a reservation so the `NodeProviderIDController` cannot bind the same node elsewhere — and the remove is sent with it; otherwise the remove is sent **untargeted** (empty `provider_id`) so the catalog count is still corrected.
3. The add **FAILED** (recorded by the failure handler in `PoolBackoff`): no machine is coming → `NodeClaimNotFoundError`, no RPC.
4. Otherwise a **synchronous** cancel: cancelled → `NodeClaimNotFoundError`; not cancellable (RUNNING or already finished) → `nil`, the NodeClaim stays Terminating until the poller reports the add's outcome (the answer is memoised, one RPC per NodeClaim); cancel RPC error → returned, Karpenter retries in 5s.

A transient LIST error is returned, never turned into NotFound. The per-UID lookups are memoised for the life of the process, so the 5-second retries issue no uncached LISTs.

> **Why `List()` must not filter by cluster ID.** A real ProviderID is stamped by the Rafay platform as `rafay://<nodepoolname>/<sku_name>/<hostname>` — its first segment is the **node pool**, not a cluster ID (see [§10](#10-provider-id-format)). Comparing that segment against `RAFAY_CLUSTER_ID` therefore never matches, and `List()` would return an empty slice. That is dangerous, not merely useless: Karpenter's core garbage-collection controller **deletes any `Registered` NodeClaim whose ProviderID is absent from `List()`**, so an empty list tears down every managed node the moment it briefly goes `NotReady`.

**`findNodeProviderID`** (used by `Delete` for pending NodeClaims whose add SUCCEEDED) reads through the **uncached `apiReader`** — a stale informer cache here could match the wrong node — and runs under `cloudprovider.NodeOwnershipMu`, the process-wide lock that serializes the three read-decide-write cycles that bind a Node to a NodeClaim (this lookup + reservation, the `NodeProviderIDController`'s LIST-to-patch, and the `NodeAdoptionController`'s LIST-to-last-Create), closing the cross-controller TOCTOU where two of them resolved one operator-added node to different NodeClaims:
```
findNodeProviderID(ctx, nodeClaim)
  ├─ Build usedIDs: status.providerID of all non-self non-pending NodeClaims
  │                 ∪ karpenter.rafay.io/adopted-provider-id of every NodeClaim   (apiReader)
  ├─ List Nodes with label:  nodepoolname=<pool>        (apiReader; sku filtered in code)
  └─ Return first node where:
       ├─ node.sku_name ∈ NodeClaimSKUs(nodeClaim)   (see below)
       ├─ spec.providerID != "" and not in usedIDs
       └─ creationTimestamp > nodeClaim.creationTimestamp
```

**`NodeClaimSKUs` — how a node's `sku_name` is matched to a NodeClaim.** `sku_name` on a node is the **platform's SKU / instance-type name**, not the RafayNodeClass name. A node matches if its `sku_name` equals:

1. the NodeClaim's `node.kubernetes.io/instance-type` label — the instance type `Create()` actually selected, stamped onto the NodeClaim via `requirementsToLabels`. This is the authoritative match, and when the label is present it is the **only** one; **or**
2. only when the NodeClaim carries **no** instance-type label: the NodeClaim's `spec.nodeClassRef.name` — back-compat with the legacy single-SKU convention where a `RafayNodeClass` is named after its one SKU.

Because a NodeClaim can accept more than one `sku_name` value, `sku_name` **cannot** be a server-side label selector; the Node list is selected by `nodepoolname` only and `sku_name` is filtered in code. Matching on the NodeClass name *alone* would never resolve a node for any `RafayNodeClass` that lists **several** `instanceTypes` — exactly the configuration cheapest-fit selection exists to serve — leaving such NodeClaims unmatched until the 60-minute registration timeout deleted them, orphaning the machine each cycle.

`NodeClaimSKUs` is exported because the same matching rule has to hold in three places that must not disagree: `Delete`'s `findNodeProviderID`, the [`NodeProviderIDController`](#pkgcontrollersnodeproviderid--providerid-resolution-controller), and the [`NodeAdoptionController`](#pkgcontrollersnodeadoption--existing-node-adoption-controller) — the last two being complementary halves of one decision about which NodeClaim owns a node.

**`NewBatchFailureHandler(kubeClient, apiReader, poolBackoff, recorder)`** returns the `rafay.FailureHandler` wired in `main.go`:
- every terminal FAILED is first **recorded per operation** in `poolBackoff` (`MarkFailedOp`, kept 2 h) — for adds unconditionally, for removes only when permanently refused. `Delete()` reads those records: a pending NodeClaim whose add FAILED has no machine to remove, and a remove the broker will refuse again must not be re-sent every 5s.
- kind `"add"`: finds the NodeClaim whose UID equals the operationID and **deletes it** — but only if it still carries a pending ProviderID and has no deletion timestamp. Karpenter then reprovisions immediately (seconds) instead of waiting out the 60-minute registration timeout.
- kind `"add"` with a **permanent refusal** detail (`IsPermanentRefusalDetail`: `pool at maximum`, `pool at minimum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling`, `pool precondition`, plus the wordings older brokers used — `is at its maximum`, `is at its minimum`, `is not on cluster`, `on the cluster but`, `has no scaling block`, `service pool not configured`, `not found on compute instance` — matched case-insensitively anywhere in the detail): **first marks the NodeClaim's NodePool in `poolBackoff`**, then deletes the NodeClaim as above. Reprovisioning at once would be refused again, so `GetInstanceTypes` withholds the pool's offerings for the cooldown (`DefaultPoolAtMaxCooldown` = 5 min, `RAFAY_POOL_AT_MAX_COOLDOWN`) and the scheduler leaves the pods pending with `no instance type has the required offering`. The NodeClaim is still deleted: kept, it would be an in-flight node the scheduler expects the pods to land on, for the whole 60-minute registration timeout. A nil `poolBackoff` (or a `0` cooldown) restores plain delete-and-reprovision. The hold is also recorded as a **Warning event on the NodePool** — reason `PoolAtPlatformMaximum` for the maximum (`IsPoolAtMaxDetail`), `PoolRefusedByPlatform` for the others, message = the broker's detail plus the hold end (deduped per pool for 2 minutes by Karpenter's recorder) — because the pods' own scheduling message during the hold — `nodepool requirements filtered out all available instance types` — does not say why.
- kind `"remove"`: log-only for transient failures — nothing to clean up client-side. The operationID is cleared from the in-flight set, and Karpenter keeps calling `Delete` (every 5s, while the NodeClaim is terminating), so the next call re-sends the removal and the broker rewrites the FAILED record to ACCEPTED. A permanently refused remove from an old broker (`is at its minimum`, `pool at minimum`, …) is recorded instead, and the next `Delete` converges with the `NodeNotRetired` event rather than re-sending.

**Testability**: `CloudProvider` holds its batcher as the small unexported `nodeBatcher` interface (`Succeeded`, `SucceededResult`, `Enqueue`, `EnqueueRemove`, `Cancel`) — the subset of `*rafay.NodeBatcher` that `Create`/`Delete` call. `NewCloudProvider` still takes the concrete `*rafay.NodeBatcher`; the unit tests (`cp_*_test.go`) substitute a scripted fake so every `Delete` branch can be driven without a broker, a 10-second batch window or a 30-second poll tick.

---

### `pkg/controllers/nodeproviderid` — ProviderID Resolution Controller

**Purpose**: Resolves the real Kubernetes `spec.providerID` for NodeClaims that have a synthetic pending ProviderID, and patches `NodeClaim.Status.ProviderID` with the real value.

**Why it exists**: `Create()` returns immediately after broker ACK to avoid blocking past Karpenter's 5-minute `launchTimeout`. Nodes typically take ~12 minutes to join, and up to 60 (the platform builds the whole machine). The `NodeProviderIDController` bridges this gap inside Karpenter's 60-minute `registrationTimeout` window.

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

Lock cloudprovider.NodeOwnershipMu (held from the NodeClaim LIST to the status patch)

Read NodeClaimList directly from API server (apiReader, not cache) → build usedIDs
  = { status.providerID of non-pending NodeClaims }
  ∪ { karpenter.rafay.io/adopted-provider-id of every NodeClaim }   # an adopted, not yet
  (bypasses informer cache to prevent stale-cache double-assignment race)   # Launched claim reserves its node too

List Nodes with label: nodepoolname=<nodePoolName>
  (sku_name is NOT a server-side selector — a NodeClaim may accept several values)

Find first unclaimed node where:
  ├─ node.Labels[sku_name] ∈ skus
  ├─ spec.providerID != "" and not in usedIDs
  └─ creationTimestamp > nodeClaim.creationTimestamp

If not found → requeue after 30s

Patch Node: label karpenter.sh/registered=true (merge patch, skipped when already set)
  └─ failure → error returned, NodeClaim stays pending and is retried (never half-bound)

Patch NodeClaim.Status.ProviderID = node.Spec.ProviderID (optimistic locking)
  ├─ Conflict → Requeue: true (retry with fresh resource version)
  └─ NotFound → ignore (NodeClaim deleted during disruption)
```

**Key properties:**
- **`sku_name` matches the selected instance type, not just the NodeClass name.** `node.sku_name` is the platform's SKU name; a NodeClass listing several `instanceTypes` provisions a node whose `sku_name` is the *selected* one, which does not equal the NodeClass name. Matching on `NodeClassRef.Name` alone therefore never resolved such NodeClaims — they churned on the hourly registration timeout, orphaning a machine each cycle. See `NodeClaimSKUs` under [`pkg/cloudprovider`](#pkgcloudprovider--karpenter-interface-implementation).
- `MaxConcurrentReconciles: 1` — read-decide-patch is never concurrent, preventing two NodeClaims from claiming the same node; `cloudprovider.NodeOwnershipMu` extends the same guarantee across the adoption controller and `Delete()` (a provider-ID reconcile waits for an in-progress adoption pass — seconds on a large first pass).
- **`karpenter.sh/registered=true` is set on the node before the binding patch**, so Karpenter's Registration reconciler no longer logs `missing taint prevents registration-related race conditions` / emits `UnregisteredTaintMissing` for every provisioned node, and a failed node patch leaves the claim pending (retried) rather than half-bound.
- **Secondary watch on Nodes**: when a real Rafay node appears (filtered by `rafayNodePredicate`: non-empty `spec.providerID` + Rafay labels), matching pending NodeClaims are immediately enqueued without waiting for the 30s requeue timer. `rafayNodePredicate` is a package-level variable (not a closure inside `Register`) only so its event filtering can be unit-tested; the same holds for the adoption controller's predicate.
- **Stateless across restarts**: controller-runtime re-enqueues all existing NodeClaims with `rafay://pending/` ProviderID on pod startup. No explicit resume logic needed for the binding; the add batches themselves are resumed by [`batchresume`](#pkgcontrollersbatchresume--add-batch-resume-on-restart).
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

lock cloudprovider.NodeOwnershipMu (held from the NodeClaim LIST to the last Create of the pass)

poolNodes  ← Nodes with label nodepoolname=<nodePool.Name>            (informer cache)
allNodes   ← every Node, for the worker-node count in the log line    (informer cache)
claimList  ← every NodeClaim, from the API server (apiReader, not the cache)

claimedIDs ← { status.providerID } ∪ { karpenter.rafay.io/adopted-provider-id }
pending    ← NodeClaims for this pool still carrying rafay://pending/
for each adopted NodeClaim of the pool: strip karpenter.sh/nodepool-hash if Karpenter
  back-filled it, re-pin karpenter.sh/nodepool-hash-version         # durable drift exemption

log: pool <name> has N node(s) and M nodeclaim(s) (K worker node(s) across all pools)

for each node in poolNodes:
  skip if deleting, control-plane, no sku_name label, or a non-rafay:// providerID
  skip if the NodeReady condition is not True                  # wait for it; see below
  candidateID ← node.spec.providerID, or rafay://<pool>/<sku>/<name> when empty
  if claimedIDs[candidateID]:                                  # already has a NodeClaim
    if node.spec.providerID == "": stamp candidateID + karpenter.sh/registered=true back
      onto the node (a re-registered kubelet); create NO second NodeClaim
    skip
  skip if claimedByPendingClaim(pending, node, sku)           # a scale-out is waiting for it
  skip if sku_name ∉ the pool's RafayNodeClass instanceTypes  # would be marked drifted
  skip if validateAdoptable fails                             # labels vs pool reqs + SKU offerings

  patch Node: spec.providerID (if empty) = rafay://<pool>/<sku>/<name>, karpenter.sh/registered=true
  create NodeClaim annotated karpenter.rafay.io/adopted-provider-id=<providerID>
                   annotated karpenter.sh/nodepool-hash-version=<current>, no nodepool-hash

requeue after 5m
```

The Node watch (`rafayNodePredicate`) fires on Create/Delete for nodes carrying both pool labels and on Update only when the labels, `spec.providerID` or the `NodeReady` condition changed — a kubelet heartbeat or image-list churn does not trigger a pool reconcile (and its uncached NodeClaim LIST); the 5-minute resync is the fallback.

**How adoption avoids provisioning a machine.** Karpenter's `Launch` reconciler calls `CloudProvider.Create()` for every NodeClaim whose `Launched` condition is not yet `True` — so a hand-created NodeClaim would order a **second** machine for a node the cluster already has. The `karpenter.rafay.io/adopted-provider-id` annotation is what prevents that: `Create()` sees it and returns that ProviderID (plus the SKU's capacity) without touching the broker. Karpenter then registers and initializes the NodeClaim against the existing node through its normal path.

The marker is an **annotation on the object at creation time**, not a status pre-patch by this controller, because status is a separate subresource: a create-then-patch sequence leaves a window in which `Launch` sees a NodeClaim with no ProviderID and calls the broker. The annotation is visible on the very first `Launch` reconcile and survives controller restarts.

**Key properties:**

- **Only Ready nodes are adopted** — `nodeutils.GetCondition(node, NodeReady).Status == True`, the same test Karpenter's own `Initialization` reconciler applies. A NodeClaim for a NotReady node is worse than none at all: it is **counted** (while uninitialized, `StateNode.Capacity()` fills any resource the node does not report from the NodeClaim's — so for a node whose kubelet never reported, or whose Node object is later deleted while the claim lives, the SKU's *declared* figures become the whole capacity and the pool's limits are charged for a machine that is not there), and it is **stuck** — `Initialization` requires `NodeReady`, so the claim sits at `Registered=True` / `Initialized=Unknown("NodeNotReady")` while `Liveness` only reaps claims that failed to *register*, and garbage collection only considers claims absent from `List()` — which this one is not, since adoption just stamped a `rafay://` providerID on its node. Nothing reaps it — not even expiration, since every broker-rendered pool sets `spec.expireAfter: Never`. A machine mid-teardown presents identically, and adopting it writes an **immutable** `spec.providerID` onto an object already on its way out. Waiting costs nothing: the Node watch fires on the status update that flips the condition, so adoption follows within seconds rather than at the next resync. A node that is Ready and *later* goes NotReady keeps its NodeClaim — this gate governs only when to take a node on; from then on Karpenter's **disruption** path owns it (not node repair: that controller is only registered when `RepairPolicies()` is non-empty, and this provider returns `nil`).
- **The readiness gate is evaluated before the ownership checks**, so that a node still coming up — kubelet registered, but `sku_name` or `spec.providerID` not yet stamped by the platform — gets a quiet `V(2)` deferral rather than recurring warnings, and so the lazy `GetInstanceTypes` resolution (which hard-errors the reconcile on an unusable `RafayNodeClass`) is not hoisted above it. The consequence is that an **already-adopted** node that goes NotReady reaches the gate too, which is why the `notReady` counter excludes nodes whose providerID is already claimed — otherwise the log would report a node as "waiting to become Ready" while the same line said nodes == nodeclaims.
- **Idempotent through the status gap.** An adopted NodeClaim has an empty `status` until `Launch` runs, so `claimedIDs` is built from the adoption annotation *as well as* `status.providerID`. Reading only the status would let a second pass adopt the same node again — two NodeClaims for one machine, the second of which Karpenter eventually terminates, taking the node with it. NodeClaims are read through `apiReader` for the same reason: a just-created NodeClaim is not in the informer cache yet.
- **Never steals a pending NodeClaim's node.** `claimedByPendingClaim` is the mirror of the [`NodeProviderIDController`](#pkgcontrollersnodeproviderid--providerid-resolution-controller) rule: that controller binds a node to a pending NodeClaim only when the node is *newer* than the claim, so a node newer than any pending claim for the same pool+SKU is that claim's to take. Adopting it would leave the claim unresolved until its 60-minute registration timeout.
- **Refuses to adopt what Karpenter would immediately replace.** `validateAdoptable` mirrors the drift sub-controller exactly — `areRequirementsDrifted` (pool requirements vs NodeClaim labels) and `instanceTypeNotFound` (a SKU offering compatible with those labels). An arm64 node labelled into an amd64 pool fails both; adopting it would mark the NodeClaim drifted and drain a running node. Such nodes are named in a warning and left unmanaged.
- **Well-known labels come from the Node, not the SKU.** `Registration` copies a NodeClaim's labels *onto* its node, so a value inferred from the SKU that disagrees with the node would overwrite what the kubelet reported. Topology labels are omitted entirely when the node has none — a `RafayNodeClass` with no `zone` yields the synthetic zone `default`, and stamping that onto a running node would replace real topology information. Omitting the key is safe only because `validateAdoptable` applies the same **strict** check as the drift sub-controller: `Requirements.Compatible` *without* `AllowUndefinedWellKnownLabels` (what `areRequirementsDrifted` calls) treats an absent key as a mismatch, so a node that would fail it is never adopted in the first place; the offerings half (`Offerings.HasCompatible`) is the only lenient part. Keep `validateAdoptable` strict.
- **Pool taints are not copied.** `Registration` merges a NodeClaim's taints onto its node; a `NoExecute` taint the node does not already have would evict the pods running on it. The platform has already applied the pool's catalog taints to the nodes it built.
- **No `karpenter.sh/nodepool-hash` annotation — durably.** Static drift is only evaluated when *both* NodePool and NodeClaim carry the hash, so leaving it off means a later edit to the pool template does not mark every adopted node drifted. The NodeClaim *is* stamped with the current `karpenter.sh/nodepool-hash-version`, because Karpenter's nodepool-hash controller only back-fills claims whose version annotation is stale; and every adoption pass strips the hash from any adopted claim that acquired one anyway (a hash-version bump on upgrade, an operator `kubectl replace` of the NodePool) and re-pins the version (`stripNodePoolHash`, log `nodeadoption: stripped karpenter.sh/nodepool-hash from adopted nodeclaim …`). In the moments between a back-fill and the strip the claim may show `Drifted`; the broker's Drifted-scoped `nodes: "0"` budget keeps that from rolling anything.
- **Control-plane nodes are refused** even if they carry a `nodepoolname` label: a NodeClaim would let Karpenter drain and remove the cluster's own control plane.
- **`spec.providerID` is filled in only when empty.** Kubernetes makes the field immutable once set, which is also why a wrong value would be unrecoverable — `rafay.BuildProviderID` renders exactly the format the platform itself writes (`rafay://<nodepoolname>/<sku_name>/<hostname>`, see [§10](#10-provider-id-format)).
- **`karpenter.sh/registered=true` is pre-set on the node.** The `Registration` reconciler logs a registration error and emits an event for any node carrying neither that label nor the `karpenter.sh/unregistered` startup taint. An already-joined node genuinely is registered, and the taint is not an option — it is `NoExecute` and would evict the pods already there.
- **`spec.expireAfter` comes from the pool template**, exactly as for a NodeClaim Karpenter provisions — so an adopted node follows the pool's own expiry policy. Every broker-rendered pool sets `Never` — an explicit owner decision, expiry is not managed by this integration (see [§7.1](#71-catalog--rafaynodeclass-bootstrap)) — so in practice neither adopted nor provisioned nodes are ever force-expired; a hand-written pool that sets a duration rotates both alike. Diverging here would make adopted and provisioned nodes in the same pool age differently.
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

- **Config**: `headroom-policy` ConfigMap, per pool: buffer fractions `cpu`/`memory`/`gpu` (percentages, **0–50**: the buffer is a fraction of *total* pool allocatable, so a larger fraction cannot converge once DaemonSet overhead is counted — the effective buffer is `f/(1−f−d)`), per-pod slice `podCPU`/`podMemory`/`podGPU` (defaults 500m / 512Mi; GPU has no default and must be a whole number), `minPods` cold-start floor, optional `maxPods` replica ceiling (when > 0 it must be ≥ `minPods`), and optional `tolerations`. Data keys are scanned in a **deterministic order** — the well-known keys `policy` then `config`, then every other key sorted by name — because Go map iteration is randomized and an unsorted scan would let the applied policy flap between reconciles when two keys both declare pools. Parse-time validation refuses the whole policy on: duplicate pool names, a pool `name` that is not a DNS-1123 label, a toleration with `operator: Exists` and a value, an Equal toleration without a key, a toleration key that is not a qualified name, per-pod sizes below `1m` / `1Mi`, and a fraction outside `[0, 50]`.
- **A ConfigMap parse error keeps the buffer up.** A YAML typo returns an **error** from `Reconcile`, which short-circuits *before* any GC runs. It must never read as "no pools configured": falling through with an empty pool list would make `cleanupStaleDeployments` tear down **every** headroom Deployment in the cluster over a single bad character. A ConfigMap that parses cleanly and declares *no* pools is a legitimate "headroom off" policy and does tear the Deployments down. Keys that are not `policy`/`config` and hold a YAML scalar or list (free text, `"false"`, a list) are skipped, and a non-well-known key that is not valid YAML is ignored with a warning when a well-known key parsed cleanly — so `policy: "pools: []"` next to a `notes:` key still reads as headroom off. Malformed YAML under a well-known key, or a malformed fallback key with no clean well-known key, still fails closed.
- **Replica math**: `replicas = max over configured resources of ceil(fraction × Σ ready-node allocatable / per-pod size)`, floored at `minPods` (with zero ready nodes the result is exactly `minPods`), capped at `maxPods` when set, then **clamped to `[0, maxHeadroomReplicas]` (5000)**. The per-pod size is the divisor, so a typo there (or a huge `minPods`) can ask for an absurd number of pause pods; the clamp turns that into a loud warning and a bounded pod count instead of an `int32` overflow or a pod flood. Per-pod sizes below `1m` CPU / `1Mi` memory are rejected at parse time for the same reason. Because every pool consolidates `WhenEmpty`, a pool with headroom never shrinks below `ceil((workload + buffer) / node size)` and a node hosting a pause pod is reclaimed only once it leaves.
- **Inert pools get no headroom.** A pool whose `NodePool` does not exist, is annotated `karpenter.rafay.io/auto-scaling="false"`, or has `spec.limits.nodes: 0` is inert: no Deployment is created and an existing one is scaled to 0 replicas (kept, shape still owned, not GC'd), with one Warning log per transition (`headroom: pool "x" is inert (<reason>); holding its headroom at 0 replicas — …`) and one Info when it goes live again. The controller watches NodePools (generation/annotation changes) so a flip is picked up before the resync; a non-NotFound NodePool read error fails that pool's reconcile (Deployment untouched, retried).
- **Watch-based reconciler** (not a ticker): the `headroom-policy` ConfigMap (through its own cache scoped to `HEADROOM_CONFIG_NAMESPACE` with a `metadata.name=headroom-policy` field selector — not the manager's cluster-wide ConfigMap informer; the policy itself is read with the manager's uncached `APIReader`, one GET per sync), pool-Node, headroom-Deployment and NodePool events all funnel into one synthetic full-sync request, plus a 5-minute safety resync. `MaxConcurrentReconciles: 1`.
- **Pod spec**: pause image, PriorityClass `rafay-headroom` (value −1000, `PreemptionPolicy: Never`), Guaranteed QoS (requests == limits), a **preferred `podAffinity`** (weight 100) on `kubernetes.io/hostname` selecting `rafay.io/headroom-pool=<pool>` so pause pods **pack** rather than spread (a hostname topology spread kept one pause pod per node forever and blocked `WhenEmpty` scale-in; existing Deployments have the spread stripped on the next sync), **only the tolerations from config** — no blanket `Exists` toleration, so headroom pods respect taints that real workloads cannot cross — and a restricted-PSS-compliant security context (`automountServiceAccountToken: false`, `runAsNonRoot`, `runAsUser: 65535`, `seccompProfile: RuntimeDefault`, `allowPrivilegeEscalation: false`, capabilities `drop: [ALL]`).
- **Steady-state reconciles issue no Updates.** The reconciler mutates only the fields it owns, **in place on the fetched Deployment template**. Assigning a whole `PodTemplateSpec` would drop everything the API server defaults (`restartPolicy`, `dnsPolicy`, `schedulerName`, `securityContext`, …), so the object would differ from the stored one on *every* reconcile and `CreateOrUpdate` would issue a pointless Update — forever, every 5 minutes.
- **PriorityClass ensured on every Reconcile**, not just at startup: the startup runnable gets exactly one attempt, and without the PriorityClass the pause pods are rejected at admission, silently disabling headroom until the next restart. It is a cached Get first and a Create only on NotFound (an `AlreadyExists` race falls back to the drift check; another Get error is returned wrapped and tolerated as before). A PriorityClass's `Value` is immutable and the controller holds no update verb, so a **pre-existing `rafay-headroom` class with a different value or `preemptionPolicy` cannot be repaired** — it is reported with a **loud warning on every reconcile** instead of passing silently (a non-negative value makes headroom pods un-preemptable, so the buffer stops handing capacity back; `preemptionPolicy != Never` lets them evict real workloads). Fixing it means deleting the class so it is recreated.
- **GC**: legacy bare headroom pods and Deployments for pools no longer in the config are deleted on every sync — but only after the config parsed successfully (see above).
- **Namespaces** from `HEADROOM_NAMESPACE` (Deployments) and `HEADROOM_CONFIG_NAMESPACE` (ConfigMap); see [§13](#13-configuration-reference).
- **RBAC**: needs write on `apps/deployments`, get/list/watch/create on `scheduling.k8s.io/priorityclasses`, get/list/watch on `configmaps`, `nodes` and `karpenter.sh/nodepools` (granted in `config/deploy/rbac.yaml`).

> **GPU headroom reserves capacity on existing GPU nodes; it cannot cold-start a pool — by design.** The GPU resource name is **derived from the GPU nodes the pool already has**; on a pool with no GPU nodes the `podGPU` request is **dropped with a warning** (`… GPU headroom is reserve-only by design: it sizes from existing GPU nodes and does not cold-start them`) rather than guessed (a guessed request would only park a pause pod in `Pending` forever, and on an `amd.com/gpu` pool the guess would be wrong outright). Use `minPods` with a cpu/memory-sized pod to cold-start a GPU pool; the GPU buffer applies once nodes exist.
>
> This is stricter than the cloudprovider needs. Instance types of SKUs whose ComputeProfile declares a positive `gpu_count` advertise a **temporary** `nvidia.com/gpu` capacity (see [the RafayNodeClass reference](#rafaynodeclass-crd-portable--same-manifest-on-every-cluster)), so Karpenter *can* provision for a pending GPU pod — but the headroom controller deliberately keys off existing nodes. Revisit it when that temporary field is replaced by real per-SKU accelerator capacity; sourcing `gpuKey` from the instance type would then let a GPU buffer drive scale-out too.

> **Requires `consolidationPolicy: WhenEmpty`** on participating NodePools. Pause pods keep nodes non-empty by design; with `WhenEmptyOrUnderutilized` Karpenter would repeatedly consolidate nodes that hold only headroom pods, causing churn. The examples in `examples/nodepool.yaml` are set accordingly.

---

### `pkg/controllers/batchresume` — Add-Batch Resume on Restart

Hands the status poller back the add batches a previous incarnation of the provider sent. The `NodeBatcher` tracks sent batches only in memory, and for adds nothing ever re-sends: once the broker ACKs, the NodeClaim is `Launched` and Karpenter's launch reconciler never calls `Create()` again. A node that does arrive is still bound by the [`NodeProviderIDController`](#pkgcontrollersnodeproviderid--providerid-resolution-controller), so success needs no help — but after a restart a **FAILED** add would go unnoticed, and its pending NodeClaim would sit until the 60-minute registration timeout instead of being reaped within a couple of minutes by the batcher's failure handler.

- **`Create()` stamps the batch on the NodeClaim** it returns (`karpenter.rafay.io/batch-id`). `BatchResult.BatchID` is set on every ACK, including the in-flight short-circuit, and Karpenter's `PopulateNodeClaimDetails` merges returned annotations onto the stored object, so the batch ID outlives the process.
- **One pass at startup**, as a leader-only manager `Runnable` rather than a reconciler: it lists NodeClaims through the uncached `apiReader`, keeps those that are not deleting and still carry a `rafay://pending/` providerID, groups them by the annotation, and calls `NodeBatcher.ResumeAddBatch` per batch. A batch already tracked is left alone, so the pass is idempotent. NodeClaims without the annotation (created by a provider from before it existed) fall back to the registration-timeout path as they always did.
- **Resumed batches are polled on the next tick** (like every batch) and their max-age clock (3 h) restarts from now; the broker's batch index TTL (3 h) bounds it independently. A batch the broker has forgotten yields empty polls, which the batcher already treats as failure — the right outcome, since that add really is lost.
- **Removes are not resumed on purpose:** Karpenter re-issues `Delete()` every few seconds with the same deterministic operation ID, so they recover through the normal path.
- **Errors are logged, never returned:** a failed list must not stop the manager, and the cost of a missed resume is only the slower registration-timeout recovery that existed before.

### Unit test conventions

All tests use the standard `testing` package (no assertion libraries) and run with `go test -race ./...`.

- **Test-only seams are unexported and behavior-preserving**: `batchBroker` and `nodeBatcher` interfaces, the two `rafayNodePredicate` package variables, `NodeBatcher.newTimer` (so `collectBatch` windows are driven manually), and the headroom controller's `configReader`. Production wiring in `cmd/controller/main.go` is unchanged by them.
- **No live infrastructure**: `pkg/rafay` tests start an in-process gRPC server on a loopback listener that implements the hand-written `KarpenterBatchService` / `KarpenterConfigService`; `pkg/broker` tests generate a throwaway CA and client certificate with `crypto/x509`; controller tests use controller-runtime's fake client (headroom tests seed a live `NodePool` for every pool that expects a buffer, since inert pools get none).
- **File naming**: the September 2026 review added tests as new files with a per-area prefix — `cp_` (cloudprovider), `rf_` (rafay), `ad_` (nodeadoption / nodeproviderid), `hr_` (headroom), `ms_` (nodeconfig, batchresume, rafaynodeclass, broker, apis, cmd), plus `fup_` for the review's follow-up round and `p2_` for the phase-2 residuals. New helpers in those files carry the same prefix.
- **Known-bug regression tests**: during the review, a test whose first statement was `t.Skip("known bug <finding-id>: …")` asserted the *correct* behavior for a confirmed but not yet fixed defect. Every such defect has since been fixed and its `Skip` removed — **there are no `t.Skip` known-bug tests left in this repository** (`grep -rn 't.Skip(' --include='*_test.go'` is empty); each former skip is now a live regression guard. If the convention is needed again, add the `Skip` with the finding id and verify the test fails without it; never "fix" such a test by changing its assertions.
- **Fixture conventions**: provider IDs in tests are pool-first (`rafay://pool-1/sku-1/<host>`); no cluster-first IDs remain. The CRD schema test (`TestMsCRDSchemaMatchesGoTypes`) reads `config/crd`, which is why `.dockerignore` keeps that directory in the image build context.

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
            status.capacity   = selected instance type capacity (nominal)
            status.allocatable = capacity − overhead
            annotations[karpenter.rafay.io/batch-id] = <broker batch>
        }
  └─▶ Karpenter lifecycle controller patches NodeClaim.status (Launched=True, synthetic ProviderID written to etcd)
  └─▶ Karpenter registration reconciler sets Registered=Unknown (60-min timer starts)

  [in the background]
  └─▶ [statusPoller] polls every 30s from the next tick after ACK (no initial delay)
        ├─ SUCCEEDED → recorded in the batcher's `succeeded` set (adds: not on the
        │               registration critical path — the joined node is)
        └─ FAILED    → FailureHandler records the op, deletes the pending NodeClaim
                        ├─▶ Karpenter reprovisions immediately (fresh NodeClaim, fresh UID)
                        └─▶ unless the detail is a permanent refusal ("pool at maximum",
                            "pool not found", "pool sku mismatch", "pool not auto-scaling",
                            "pool precondition"): the NodePool is held back first (offerings
                            unavailable for 5 min, Warning event PoolAtPlatformMaximum /
                            PoolRefusedByPlatform), so the pods stay pending instead of a
                            new NodeClaim being refused again

  [typically ~12 minutes later — up to 60 minutes: the platform builds the whole machine]

  └─▶ Node joins cluster with labels nodepoolname + sku_name, spec.providerID set by Rafay platform
        └─▶ NodeProviderIDController reconciles (triggered by node watch or 30s requeue)
              ├─ Lists NodeClaims from API server (no cache)  →  build usedIDs
              ├─ Lists Nodes with matching labels
              ├─ Patch Node label karpenter.sh/registered=true
              └─ Patch NodeClaim.Status.ProviderID = node.Spec.ProviderID
  └─▶ Karpenter registration reconciler finds node matching real ProviderID → Registered=True
```

### 6.2 Scale-In (Node Deprovisioning)

Karpenter's node termination controller finalizes a Node in order — `awaitDrain` → `awaitVolumeDetachment` → `awaitInstanceTermination` — and only the last step calls `CloudProvider.Delete()`; it does so on **every** reconcile from then on and releases the Node's termination finalizer **only** when `Delete()` returns a `NodeClaimNotFoundError`; anything else requeues in **5 seconds**. So no remove op exists at the broker while a drain or a stuck PDB is holding the node, and `Delete()` must eventually converge on that error, or the NodeClaim and Node stay `Terminating` forever.

```
Disruption controller: consolidate NodeClaim          (expiration is off on every broker-rendered
                                                        pool and drift is never rolled — §7.1; only a
                                                        hand-written pool still force-expires / rolls)
  └─▶ Node drained, volumes detached (termination controller; Delete() not called yet)
  └─▶ CloudProvider.Delete(nodeClaim)                       [called every ~5s until it converges]
        ├─ If batcher.SucceededResult("<uid>-remove") is set:
        │    ├─ detail "node not retired: …"  → NodeClaimNotFoundError, Warning NodeNotRetired ✓
        │    │     (platform refused; machine keeps running without a Node object until its
        │    │      kubelet is restarted / it is re-registered, then adoption takes it back)
        │    ├─ provider IDs named, ours not among them → hold: Warning RemoveRetiredOtherMachine,
        │    │     annotation karpenter.rafay.io/remove-retired-other, nil until no Node carries
        │    │     the ID (never re-sent)
        │    ├─ untargeted (no IDs): Node gone or NotReady → NodeClaimNotFoundError ✓
        │    │     Node still Ready → nil, for up to RAFAY_REMOVE_SETTLE_WINDOW (60m) from the
        │    │     first sighting; still Ready after that → retired-other hold as above
        │    └─ pending ProviderID (machine never registered) → NodeClaimNotFoundError ✓
        ├─ If ProviderID is "rafay://pending/<uid>": resolvePendingRemoval (§5)
        │    ├─ add SUCCEEDED → reserve the joined node's ID (if any) on the NodeClaim, send
        │    │                  the remove (untargeted when nothing registered yet)
        │    ├─ add FAILED    → NodeClaimNotFoundError (no machine is coming)
        │    └─ otherwise     → synchronous batcher.Cancel(uid): applied → NodeClaimNotFoundError;
        │                       not applied (RUNNING/finished) → nil until the poller reports it
        ├─ First remove for this NodeClaim (nothing ACKed yet in this process):
        │    ├─ no Node carries the ProviderID           → NodeClaimNotFoundError, send NOTHING
        │    └─ Node Terminating, kubelet stopped, deleted no later than the NodeClaim, not
        │       chosen by the disruption queue           → NodeClaimNotFoundError, send NOTHING,
        │                                                  Warning NodeRetiredExternally
        │       (a remove here would retire one more healthy machine — the GC cascade)
        └─▶ NodeBatcher.EnqueueRemove(operationID=<uid>+"-remove", req{
                  clusterID, projectID,                  # from RAFAY_CLUSTER_ID / RAFAY_PROJECT_ID env
                  instanceType, nodePoolName, providerID })
              ├─ already in flight at the broker?  →  resolve immediately, send nothing
              │     (suppresses the duplicate batch each 5s reconcile would otherwise push)
              ├─ [batchSender] batches removes (separately from adds)
              ├─ BrokerClient.SendBatchRemove → KarpenterBatchAccepted{batchId}
              └─ broker ACK resolves resultCh → Delete() returns nil
                    → Karpenter requeues in 5s and calls Delete() again
  └─▶ Broker: catalog count for {pool, sku} decremented by 1 (refused below the pool minimum),
        Apply + Publish → op SUCCEEDED at publish; settle poll up to 60 min
        ⚠ PaaS chooses which physical machine is retired (no targeted removal today);
          the providerID is carried in the protocol for a future targeted-removal API
  └─▶ [statusPoller] observes the op SUCCEEDED → records it in `succeeded`
        └─▶ the NEXT Delete() call (≤5s later) takes the branch at the top; it converges once
            the NodeClaim's own machine has stopped (kubelet NotReady ~40 s after it halts) ✓
```

> **Why node existence cannot be the completion signal — and readiness can.** It is tempting to have `Delete()` report "gone" once no Kubernetes Node carries the ProviderID. That never converges: during termination **Karpenter itself holds the Node object alive with its own finalizer**, and it only drops that finalizer once `Delete()` reports the instance gone. "Is there still a Node with this providerID?" is therefore circular. The broker's SUCCEEDED result is the *external* signal that says the platform accepted the removal. But because the removal is untargeted, SUCCEEDED alone does not say *this* machine is gone — the finalizer keeps the Node **object**, not the kubelet's heartbeat, so `Ready` is a valid signal for the opposite question. `Delete()` therefore converges on SUCCEEDED **plus** the Node being absent or NotReady, which makes a normal termination take as long as the platform's real retirement rather than the broker's publish; the alternative deleted a Node object whose kubelet was alive, and a v1.31 kubelet never re-registers on its own.

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
  ├─ batchresume runnable (leader only, one pass)
  │    └─ lists pending, non-deleting NodeClaims, groups them by karpenter.rafay.io/batch-id,
  │       ResumeAddBatch per batch → the status poller follows the previous process's add
  │       batches again (FAILED feedback within a poll interval, not the 60-min timeout)
  │
  └─ Removes need no resume: Karpenter re-issues Delete() every 5s with the same
     deterministic <uid>-remove; the broker's tombstone / idempotency dedups the re-send.
     The retired-other verdict survives in the karpenter.rafay.io/remove-retired-other annotation.
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
  │    └─ on any error: return it; the connection is KEPT (gRPC reconnects by itself)
  │         (no re-issue — would duplicate the catalog mutation)
  │
  └─ PollBatchStatus / CancelOperations / GetKarpenterConfig  →  callBroker
       └─ on codes.Unavailable: ResetConnectBackoff() and retry ONCE on the SAME connection
            (safe to retry: polls/config are read-only, cancels are idempotent at the broker;
             closing the shared conn would abort every other goroutine's in-flight stream)
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
                                    karpenter.sh/nodepool-hash-version=<current>
                                    (NO karpenter.sh/nodepool-hash — stripped again if back-filled)
                       labels:      karpenter.sh/nodepool=pool1, instance-type=oci-inst,
                                    arch/os copied from the node, NO zone unless the node has one
                       ownerRef:    NodePool pool1
                       spec:        pool template requirements, instance-type pinned to oci-inst,
                                    NO taints
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

The broker applies one **bulk** mutation per batch (`applyWorkerNodeCatalogDeltas`): all per-`{poolname, skuname}` deltas from the batch in a single pass, then Apply + Publish. It only ever moves `noOfSku` of rows the operator opted in (`autoScaling: true`); it never creates, renames or re-types a row. On a single-row catalog the live worker count from `status.output` is the base of every change when it differs from `noOfSku`.

**Scale-out:** `noOfSku += N` for the matching row, trimmed to the row's `maxNodeCount` (the part that fits is applied; the rest is refused `pool at maximum`). No row for `{poolname, skuname}` → refused `pool not found`; another SKU on the row → `pool sku mismatch`; row not opted in → `pool not auto-scaling`. If the run settles with fewer worker hostnames than nodes applied, the missing ones are refused `pool precondition` and their increment is taken back.
**Scale-in:** `noOfSku -= N` for the matching row; a decrement the row cannot absorb (at `minNodeCount`, at 0, no row, other SKU, not opted in) is **not applied and not retried** — the op converges SUCCEEDED with detail `node not retired: <reason>` and the machine keeps running.

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
hand would send a `poolname` the catalog does not have, and every scale-out would be refused
`pool not found` (the broker never appends a row).

`noOfSku` is deliberately not consumed — it is the pool's size *now*, and from bootstrap onward
Karpenter owns that number.

**Autoscaling is strictly opt-in per pool — but every pool is visible.** A catalog row carries
`autoScaling` (with `minNodeCount`/`maxNodeCount` bounds), and the broker derives the response's
`auto_scaling` flag from the rows. The broker renders a NodePool for **every** valid row whether
or not any row opts in, so `kubectl get nodepools` shows the whole catalog — but only the
opted-in rows render as live pools. A row without the field — or with it null/false — renders
**inert**: identical labels, taints, requirements, nodeClassRef and `expireAfter`, but
`spec.limits.nodes: "0"` (the scheduler excludes a pool with no remaining node budget, so it can
never scale out) and a single all-reasons disruption budget of `nodes: "0"` (consolidation and
drift can never pick its nodes). When **no** row opts in the response carries
`auto_scaling: false` and the inert shape of every pool; this controller applies it all the
same, so a previously live NodePool is neutralised rather than left scaling. Every rendered
pool is stamped `karpenter.rafay.io/auto-scaling: "true"|"false"`; the node-adoption controller
skips pools marked `"false"` (a disabled pool's nodes never get NodeClaims, so Karpenter cannot
count, drain, or terminate them) and the headroom controller holds their buffer at 0. There is
no cluster-level toggle in this path — the old "Auto Scaling" compute-instance variable is not
consulted. For opted-in pools the max bound lands as `spec.limits.nodes` plus a
`karpenter.rafay.io/max-nodes` annotation — and a pool **with** a catalog maximum renders
**only** `limits.nodes`; the broker-wide `KARPENTER_NODEPOOL_{CPU,MEMORY,GPU}_LIMIT` capacity
limits reach only live pools **without** a maximum, and a limit below one node of the pool's
SKU is raised to one node with a warning. The min is annotation-only
(`karpenter.rafay.io/min-nodes`) — a NodePool has no floor; the broker refuses a remove below it
(`node not retired: pool at minimum …`) and headroom is the mechanism that holds warm capacity.
A disabled row's bounds are not rendered at all (the driver never validated them). Pool and SKU
names must also be valid label values (≤ 63 characters) or the pool is dropped with a warning;
a catalog label on one of the four requirement keys that contradicts the rendered requirement
is dropped; template labels are capped at 90 and annotations at 128 KiB.

#### Pool maximum: three layers

`spec.limits.nodes` alone did not hold the ceiling. On 2026-09-23 a pool with 3 of max 4 nodes
and two pending pods looped for half an hour: Karpenter created **two** NodeClaims per round,
the broker refused the whole batch (`3 + 2 would exceed max 4`), the failure handler deleted
both, Karpenter recreated two five seconds later — every 2½ minutes (10 s batch window + the
then 120 s initial poll delay + 30 s poll tick), never placing the one pod that would have fit.
The cause is in Karpenter core, upstream and fork alike: the scheduler charges an *existing*
node one `nodes` unit against the limit (`StateNode.Capacity()` adds it), but charges a
NodeClaim it has *just planned in the same round* only what the instance type's capacity
declares (`subtractMax`), and the in-round guard fires only when zero nodes remain. Upstream's
own tests provision one pod per round. Three layers now hold the ceiling, each covering the one
above:

| Layer | Where | What it does |
|---|---|---|
| **Scheduler** | `rafayInstanceTypesToKarpenter` sets `nodes: 1` on every instance type's `Capacity` | Each planned NodeClaim now consumes one unit of the pool's remaining `limits.nodes` within the round, so with one slot left only one NodeClaim is created and the other pod is reported `node limits have been exhausted for nodepool`. The entry is stripped again (`nodeClaimResources`) before the capacity is copied onto a NodeClaim's status, so `kubectl get nodeclaim` still reads like the node. |
| **Broker** | edge-broker `AddNodes` on both backends (first-class `applyDeltasToCluster`, oneclick `applyWorkerNodeCatalogDeltas`) | A batch past `scaling.max` / `maxNodeCount` is **clamped**, not refused: the nodes that fit are applied and the rest come back `FAILED` with a detail starting `pool at maximum` (first-class reports the live count: `has N of max M`). Catches the cases the scheduler cannot see — the platform count moving under Karpenter (a node added from the console, a maximum lowered in the catalog before the next config sync). The same per-op refusal covers `pool not found`, `pool sku mismatch`, `pool not auto-scaling` and `pool precondition`. |
| **Provider backoff** | `PoolBackoff` + `NewBatchFailureHandler` + `GetInstanceTypes` | Any permanent refusal (`IsPermanentRefusalDetail`) holds the NodePool back for `RAFAY_POOL_AT_MAX_COOLDOWN` (default 5 min): its offerings are reported unavailable, so no new NodeClaim is created for it until the platform view and Karpenter's have had time to converge (node adoption of the console-added node, the 10-minute config sync lowering `limits.nodes`). The refused NodeClaim is still deleted — kept, it would be a phantom in-flight node for 60 minutes. The hold is recorded as a `PoolAtPlatformMaximum` (maximum) or `PoolRefusedByPlatform` (other refusals) Warning event on the NodePool. With no initial poll delay the whole refusal round trip now completes within one 30 s poll interval. |

Turning a pool's `autoScaling` **off** takes effect on the next resync: the server-side
apply rewrites its NodePool into the inert shape, so scale-out and consolidation stop without
anyone deleting the object. Because `spec.template` (including `expireAfter`) is identical in
both shapes, the toggle is hash-neutral — no NodeClaim is marked drifted by it.

**Expiration is off for every pool, enabled or inert — an explicit owner decision
(2026-09-27).** The broker stamps `template.spec.expireAfter: Never` on every NodePool it
renders (`defaultExpireAfter`). Expiry is not managed by this integration: the platform owns
machine lifecycle and a working node must never be force-expired by anything the autoscaler
adds. Karpenter's CRD default (`720h`) would force-rotate each node 30 days after creation, and
on this platform a rotation is a catalog decrement followed by an increment with PaaS — not
Karpenter — choosing which physical machine the decrement retires (§4.2), so age-based expiry
would only churn arbitrary healthy machines through a long reprovision. The catalog has no
per-pool expiry knob. Do not report `Never` on live pools as a defect. One caveat: a NodeClaim
stamped before the broker rendered `Never` keeps its original `expireAfter` (NodeClaim spec is
immutable) and is force-expired once at that horizon — and if its pool has since gone inert, it
is not replaced.

**Drift is observed, never acted on.** Every live NodePool renders
`spec.disruption.budgets: [{nodes: "10%"}, {nodes: "0", reasons: [Drifted]}]` (the CRD default
spelled out, plus a Drifted-scoped zero); inert pools keep their single all-reasons
`{nodes: "0"}`. `expireAfter`, labels, annotations, taints, the SKU, `nodeClassRef` — anything in
`spec.template` — is a `karpenter.sh/nodepool-hash` input (`NodeClaimTemplateSpec` carries no
`hash:"ignore"` tag and `NodeClaim.spec.expireAfter` is immutable), so a catalog edit still
re-hashes the pool and marks every Karpenter-provisioned NodeClaim in it `Drifted` — visible in
`kubectl` and metrics — but the disruption controller never replaces them: an untargeted remove
would make a roll non-convergent (PaaS could retire the replacement just provisioned). Such
edits reach existing machines only through the platform or an explicit operator delete of the
NodeClaim. Budgets sit outside `spec.template`, so adding them changed no hash. **Rollout
effect** of the `Never` change itself: one nodepool-hash move per live pool, its provisioned
NodeClaims marked `Drifted` once and never rolled. Adopted NodeClaims carry no nodepool-hash
annotation (§6.6) and are not drifted; the exemption is durable because adoption stamps the
current `karpenter.sh/nodepool-hash-version` and strips any back-filled hash on its next pass.

Operational notes: objects carry `karpenter.rafay.io/managed-by=edge-broker` and
`karpenter.rafay.io/config-revision`; every resync **re-applies** (an unchanged revision only
lowers the log line to `V(2)`), so a managed object deleted or hand-edited in the cluster is
restored within one interval; nothing is ever pruned (the broker also omits a pool when its
`ComputeProfile` read fails, and deleting a `NodePool` drains its nodes — a pool **removed from
the catalog** keeps its stale NodePool in-cluster until someone removes it);
`KARPENTER_CONFIG_BOOTSTRAP=false` turns the whole thing off. See the broker-side rendering rules
in `edge-broker/docs/karpenter-node-lifecycle.md`.

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
                               │ batch processor CAS (under the  │
                               │ per-cluster lock)               │
                               │ ACCEPTED→RUNNING (TTL → 2h)     │
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
           │   IDs (add)│           │   "cancelled by client" /    │
           │ removes:   │           │   permanent refusal /        │
           │ at publish │           │   "…; retry")                │
           │ TOMBSTONE  │           │ (TTL 2h; 15m for queue-full, │
           │ (TTL 24h,  │           │  shutdown, lock-timeout —    │
           │  refreshed │           │  stays RETRYABLE: a re-send  │
           │  on re-send)│          │  rewrites it to ACCEPTED)    │
           └────────────┘           └──────────────────────────────┘
```

Enum values on the wire: `UNKNOWN=0, ACCEPTED=1, RUNNING=2, SUCCEEDED=3, FAILED=4`. State transitions are written to Redis by the batch processor; the racy ones (`cancel_ops` and queue-full rejection, both vs. ACCEPTED→RUNNING) use the `WATCH`-based compare-and-set so the loser of the race is skipped rather than clobbered. The broker's status handler reads Redis and returns the current state to the polling client — only for records belonging to the calling edge. A record in `UNKNOWN` is never re-accepted and never re-created. Three SUCCEEDED outcomes carry a distinguishing detail: a **remove** is tombstoned at publish (a later settle failure keeps SUCCEEDED, detail `catalog decremented; compute instance did not settle`); a write that failed **after** the platform commit is `committed; <reason>`; a remove the platform refused for good is `node not retired: <reason>` (nothing written).

**SUCCEEDED is a tombstone, and that is a correctness requirement.** The terminal SUCCEEDED record must outlive any plausible client retry horizon, so it carries a **24-hour TTL** that is **refreshed on every duplicate send**. Clients re-send a deterministic operationID (`<uid>-remove`) for as long as they keep retrying an operation; when the tombstone expired with the then 60-minute RUNNING TTL, the next re-send looked like a **brand-new** operationID, was ACCEPTED again, and the processor applied the catalog delta a **second** time — for a removal, retiring an extra healthy machine, once per hour per terminating NodeClaim. FAILED records are not tombstoned: a FAILED operation **must** stay retryable, and expiring is equivalent to re-accepting.

**Batch sequence (client side):**

```
t=0s       SendBatch{nodes[]}                → Recv KarpenterBatchAccepted{batchId}
           → resultCh resolved: Create()/Delete() return immediately
t≤30s      PollBatchStatus{batchId}          → Recv ... nodeResults: RUNNING   (next ticker tick)
t=+30s     PollBatchStatus{batchId}          → Recv ... RUNNING
...
t=Xs       PollBatchStatus{batchId}          → Recv terminal states:
             ├─ SUCCEEDED → recorded in `succeeded` + cleared from `inFlight`;
             │              batch removed from inProgress when all items terminal
             │              (removes: the NEXT Delete() call acts on this)
             └─ FAILED    → FailureHandler invoked (add: pending NodeClaim deleted);
                            cleared from `inFlight` so a retry can re-send
t=3h       batch still unresolved → dropped, items FAILED "batch expired at broker"
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

Edge-broker processes each edge's batches **strictly sequentially**: one processor goroutine per edge drains that edge's queue (16 waiting batches) one batch at a time, so two catalog mutations on the same cluster never race within a process; different edges proceed in parallel. Across broker replicas the per-cluster Redis lock (`/edge/karpenter/lock/<edgeID>`) gives the same guarantee — a replica that finds the cluster locked waits (retrying every 15 s, for up to 90 min) and takes over only a lock whose holder's liveness beacon is gone. If an edge's queue is full, new batches are rejected in-band (`batch_rejected`) with nothing written instead of queued. Within a batch, the ACCEPTED→RUNNING compare-and-set ensures each operation is claimed by exactly one processor run even if duplicate queue items exist.

### Concurrency Control (provider side)

`NodeProviderIDController` uses `MaxConcurrentReconciles: 1` with `apiReader` (direct API server reads) to ensure no two reconciles can assign the same node to different NodeClaims, even with a warm informer cache. `CloudProvider.findNodeProviderID` applies the same uncached-read discipline on the delete path, and the process-wide `cloudprovider.NodeOwnershipMu` serializes it with the `NodeProviderIDController` and the `NodeAdoptionController`, so the three read-decide-write cycles that bind a Node to a NodeClaim never interleave.

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
- **In-flight suppression**: once the broker has ACKed an operation and it has not gone terminal, the operationID sits in the `inFlight` set and re-enqueueing it resolves immediately without sending anything. This is what makes Karpenter's **5-second** `Delete()` retry loop free: without it, every reconcile for the whole life of a removal would push another remove batch onto the edge's 16-slot broker queue.

### Delete() Convergence

`Delete()` is not fire-and-forget. Karpenter's termination controller calls it on every reconcile and releases the Node's finalizer **only** on a `NodeClaimNotFoundError`. The batcher records every operationID the broker reports SUCCEEDED (`NodeBatcher.Succeeded` / `SucceededResult`, retained 2h), and `Delete()` returns `NodeClaimNotFoundError` once `<uid>-remove` has SUCCEEDED **and** the NodeClaim's machine has stopped (Node absent or NotReady; at once for a `node not retired` detail or a pending ProviderID). Node existence cannot serve as the "is it done" signal — Karpenter's own finalizer keeps the Node alive until `Delete()` says the instance is gone, so it is circular — but readiness can serve the opposite question, because the finalizer keeps the object, not the heartbeat (see [§6.2](#62-scale-in-node-deprovisioning)). A remove is never sent for a machine that is already gone, and a remove the platform retired another machine for is never re-sent (`RemoveRetiredOtherMachine`, annotation `karpenter.rafay.io/remove-retired-other`).

### No Retry on Batch Sends; One Retry on Polls/Cancels/Config

`SendBatch` and `SendBatchRemove` use `callBrokerOnce`. On any failure:
- The shared connection is **kept** — closing it would abort every other goroutine's in-flight stream; gRPC reconnects by itself.
- The failed send is **not re-issued** from the broker wrapper — a retry would risk duplicating the catalog mutation.
- Application-level retry: `Create()`/`Delete()` return the error, Karpenter retries, and the same operationID dedups at the broker if the first send actually landed.

`PollBatchStatus`, `CancelOperations` and `GetKarpenterConfig` use `callBroker`, which on `codes.Unavailable` resets the connection's backoff and retries **once on the same connection** — all three are safe to re-issue. A failed poll is additionally retried on the next 30s tick.

### Batch Expiry Backstops

The status poller stops tracking a batch when:
- the broker answers an unknown batchID with an empty status **3 consecutive times**, or
- the batch is older than **3 hours** (`maxBatchAge`; a node add can take up to 60 min and the broker's add deadline is 90 min, so this must stay above both — never shorten it).

In both cases every unresolved item is treated as FAILED (`"batch expired at broker"`) and handed to the failure handler — so a lost broker-side record converts into a fast NodeClaim replacement rather than a silent hang.

### Crash Safety Summary

| State at pod restart | `Launched` in etcd | Broker knows? | Recovery |
|---|---|---|---|
| Item in queue, not sent | Unknown | No | Karpenter re-calls `Create()` → fresh first request |
| Batch sent, awaiting ACK | Unknown | Maybe | Karpenter re-calls `Create()` → broker deduplicates |
| Broker ACK'd, `Launched=True` set | True | Yes | `NodeProviderIDController` resumes from etcd; `batchresume` re-registers the batch (from `karpenter.rafay.io/batch-id`) so FAILED feedback still arrives within a poll interval; a NodeClaim without the annotation falls back to the 60-min registration timeout |
| Real ProviderID patched | True | Yes | Fully stable |
| Remove ACK'd, node draining/terminating | (deleting) | Yes | Karpenter re-issues `Delete()`; `<uid>-remove` dedups at the broker (24 h tombstone); the settle window restarts; a retired-other verdict is read back from the NodeClaim annotation |

See [docs/crash_safety.md](crash_safety.md) for per-step failure analysis.

---

## 12. Timeouts and Polling Model

### Registration Timeout Window

Karpenter's `registrationTimeout` is **60 minutes** (set in the Rafay fork of `sigs.k8s.io/karpenter`, `github.com/RafaySystems/karpenter-rafay` branch `rafay-release-v1.14.x`, `pkg/controllers/nodeclaim/lifecycle/liveness.go`; it lives outside both repos) from when `Registered=Unknown` is first set (typically ~1s after `Create()` returns). Nodes typically join at **~12 minutes**, but adding a node creates the whole infrastructure behind it and can take **up to 60 minutes** — the registration timeout equals that bound with no margin, which is why every other deadline on the add path (broker 90 min, RUNNING record 2 h, batch index and client tracking 3 h) sits well above it. The 60-minute timer is a **backstop**: broker-reported failures do not wait for it — the status poller's failure handler deletes the pending NodeClaim within roughly one poll interval of the broker writing FAILED.

```
t=0       Create() returns at broker ACK → Launched=True, synthetic ProviderID in etcd
t=~1s     Registration reconciler → Registered=Unknown (60-min timer starts)
t≤30s     first status poll (next ticker tick; a permanent refusal is handled here)
t=12m     Node joins (typical) → NodeProviderIDController patches real ProviderID
t=12m+    Registration reconciler finds node → Registered=True  ✓
t=60m     registrationTimeout backstop — only reached if the broker reported nothing
          (e.g. SUCCEEDED but the node never joined); Delete() then sends an untargeted
          remove so the catalog count is corrected
```

### Timeout Reference

| Layer | Constant | Value |
|-------|----------|-------|
| Batch: per-RPC deadline (send/poll/cancel) | `brokerCallTimeout` | 30s |
| Config: `GetKarpenterConfig` deadline | `karpenterConfigCallTimeout` | 90s |
| Batch: initial wait before first poll | — | none (polled on the next 30 s tick after ACK) |
| Pool hold per NodePool (after a permanent add refusal) | `DefaultPoolAtMaxCooldown` | 5m (`RAFAY_POOL_AT_MAX_COOLDOWN`) |
| Failure handler: per-operation FAILED record retention | `failedOpRetention` | 2h |
| `Delete()`: settle window for an untargeted SUCCEEDED remove | `DefaultRemoveSettleWindow` | 60m (`RAFAY_REMOVE_SETTLE_WINDOW`, never lower) |
| Batch: interval between status polls | `defaultPollInterval` | 30s |
| Batch: collection window (starts at first item) | `defaultBatchWindow` | 10s |
| Batch: max items per batch | `defaultMaxBatchSize` | 10 |
| Batch: client queue buffer | `batchItemQueueBuffer` | 256 |
| Batch: max tracking age before expiry | `maxBatchAge` | 3h |
| Batch: empty polls before expiry | `maxConsecutiveEmptyPolls` | 3 |
| Batch: how long a SUCCEEDED operationID is remembered (drives `Delete()` convergence) | `succeededRetention` | 2h |
| Cancel: broker cancel call timeout | `cancelTimeout` | 10s |
| Config: initial-fetch / error retry backoff | `initialRetryDelay` → `maxRetryDelay` | 5s → 2m (doubling) |
| Config: resync | `defaultSyncInterval` | 10m (`KARPENTER_CONFIG_SYNC_INTERVAL`) |
| Karpenter (core): `Delete()` retry interval while terminating | `awaitInstanceTermination` requeue | 5s |
| NodeProviderIDController: requeue when no node | `nodeWaitRequeueTime` | 30s |
| NodeAdoptionController: resync | — | 5m |
| Headroom: safety resync | `resyncInterval` | 5m |
| gRPC keepalive ping | `keepalive.ClientParameters.Time` | 5 min |
| gRPC keepalive timeout | `keepalive.ClientParameters.Timeout` | 30s |
| Karpenter (fork): launch timeout | `launchTimeout` | 5 min |
| Karpenter (fork): registration timeout | `registrationTimeout` | 60 min |
| Broker: waiting batches per edge | `karpenterBatchQueuePerEdgeMax` | 16 |
| Broker: nodes per batch / ids per cancel | `karpenterBatchMaxNodes` | 64 |
| Broker: idle per-edge processor reap | `karpenterBatchEdgeIdleReap` | 1 h |
| Broker: add processing deadline (incl. settle poll) | `karpenterBatchAddTimeout` | 90 min |
| Broker: remove processing deadline (incl. settle poll) | `karpenterBatchRemoveTimeout` | 60 min |
| Broker: platform settle poll interval | `oneclickPollInterval` / `firstClassPollInterval` | 30s |
| Broker: cluster lock TTL / wait / retry | `karpenterClusterLockTTL` / `karpenterClusterLockWait` / `karpenterClusterLockRetryInterval` | 2 h / 90 min / 15 s |
| Broker: ACCEPTED record TTL | `karpenterBatchAcceptedTTL` | 15 min |
| Broker: RUNNING / terminal FAILED record TTL | `karpenterBatchRunningTTL` | 2 h (add timeout + 30 min); 15 min for the queue-full / shutdown / lock-timeout rejections |
| Broker: SUCCEEDED tombstone TTL (refreshed on re-send) | `karpenterBatchSucceededTTL` | 24 h |
| Broker: batchID→opIDs index TTL | `karpenterBatchOpTTL` | 3 h |
| Broker: SIGTERM graceful stop / processor stop bound | `gracefulStopTimeout` / `Stop` | 20 s / 30 s |

---

## 13. Configuration Reference

### TLS / Connectivity

| Variable | Default | Description |
|----------|---------|-------------|
| `CERT_FOLDER` / `EDGE_CLIENT_CERT_FOLDER` | — | Directory with `client.crt`, `client.key`, `ca.crt` for mTLS |
| `SERVER_PORT` / `EDGE_CLIENT_SERVER_PORT` | `5448` | TLS gRPC port to edge-broker (invalid values warn and keep the default) |
| `EDGE_BROKER_GRPC_INSECURE` | `false` | `true` = plaintext gRPC. ⚠️ **Not usable for node operations** — the broker derives the edge id from the mTLS client certificate, so `BatchStreamOperations` on the plaintext listener is rejected with `ErrorNoClientID` (`GetKarpenterConfig` with `Unauthenticated`). Keep `false`. |
| `EDGE_BROKER_GRPC_PORT` | `5449` | Port when `EDGE_BROKER_GRPC_INSECURE=true` (see the caveat above). `main.go` parses an unset value as `0`, and `BrokerClient.dialBroker` turns `port <= 0` into `defaultEdgeBrokerRPCPort` (5449) on the insecure path — the broker's `EDGE_BROKER_INTERNAL_PORT` default |
| `EDGE_BROKER_GRPC_HOST` | (from cert OU) | Override broker dial host (required when insecure + no cert) |

### Identity / Routing

| Variable | Default | Description |
|----------|---------|-------------|
| `EDGE_ID` | (from cert Subject O) | Logging label for edge identity; warns if it disagrees with the cert |
| `STREAM_ID` | (auto UUID v4) | gRPC `sessionid` metadata header — auto-generated if unset |
| `RAFAY_CLUSTER_ID` | — | Rafay cluster ID stamped on every broker add/remove payload and the config request, for diagnostics. Optional: the broker identifies the cluster from the mTLS certificate (startup logs an Info line when unset). Sole source — no per-NodeClass override exists. |
| `RAFAY_PROJECT_ID` | — | Rafay project ID stamped on the same payloads. Optional: the broker derives the project itself and ignores a hint naming a project the edge record does not (`client project hint is not a project the edge record names; ignoring it`). Sole source. |
| `RAFAY_POOL_AT_MAX_COOLDOWN` | `5m` | How long a NodePool is held back from provisioning after the broker refused an add for a reason no retry can fix — pool at its platform maximum, not on the cluster, SKU mismatch, not auto-scaling, precondition (Go duration). `0` disables the hold and restores delete-and-reprovision-immediately. See [Pool maximum: three layers](#pool-maximum-three-layers). |
| `RAFAY_REMOVE_SETTLE_WINDOW` | `60m` | How long `Delete()` keeps a NodeClaim terminating after its untargeted remove SUCCEEDED while its Node is still `Ready`, before concluding the platform retired a different machine (Go duration; values below 60m are raised to it with a warning — the broker's own remove deadline; a provider restart restarts the window). See [§6.2](#62-scale-in-node-deprovisioning). |

### Instance type overhead

| Variable | Default | Description |
|----------|---------|-------------|
| `RAFAY_VM_MEMORY_OVERHEAD_PERCENT` | `7.5` | Percentage of a SKU's nominal memory the OS/kernel/firmware keep (system-reserved), in `[0, 100)` |
| `RAFAY_KUBE_RESERVED_CPU` | `80m` | kube-reserved cpu |
| `RAFAY_KUBE_RESERVED_MEMORY` | `255Mi` | kube-reserved memory |

Read once at startup; an invalid value keeps the default with a warning. The kubelet's default hard eviction threshold (`memory.available < 100Mi`) is added and is not configurable. `Capacity` stays nominal (what `limits` and the catalog count are written against); `Allocatable` drops by the total, so a pod requesting exactly a SKU's nominal size selects the next size up and `NodeClaim.status.allocatable` is below `status.capacity`.

### Controller Behavior

| Variable | Default | Description |
|----------|---------|-------------|
| `LEADER_ELECTION_NAMESPACE` | `""` in the fork; `config/deploy/deployment.yaml` sets `karpenter` | Namespace for the leader-election Lease |
| `LEADER_ELECTION_NAME` | `karpenter-leader-election` | Name of the Lease; the `karpenter-provider-rafay-leader-election` Role in `rbac.yaml` restricts update/patch to this name — widen `resourceNames` if you change it |
| `DISABLE_LEADER_ELECTION` | `false` | Disable leader election (local dev; `scripts/run-local.sh` sets it). The fork reads this name verbatim — there is no `KARPENTER_` prefix |
| `MEMORY_LIMIT` | (set by the Deployment from `limits.memory`) | Karpenter sets the Go GC soft limit to 90 % of it |
| `HEADROOM_NAMESPACE` | `karpenter` | Namespace where headroom Deployments (pause pods) are created (set explicitly in the Deployment) |
| `HEADROOM_CONFIG_NAMESPACE` | (= `HEADROOM_NAMESPACE`) | Namespace of the `headroom-policy` ConfigMap — defaults to the headroom namespace, not to the pod's |
| `KARPENTER_CONFIG_BOOTSTRAP` | `true` | Fetch `RafayNodeClass` / `NodePool` from edge-broker and apply them (see [§7.1](#71-catalog--rafaynodeclass-bootstrap)). Set `false` when those objects are owned by hand or by GitOps — the resync applies with `force: true` every interval and would overwrite the real owner |
| `KARPENTER_CONFIG_SYNC_INTERVAL` | `10m` | How often to re-fetch and re-apply that config after the first success (Go duration) |
| `KARPENTER_ADOPT_EXISTING_NODES` | `true` | Create a NodeClaim for each worker node the platform provisioned before Karpenter ran, so the pool's existing nodes count towards its limits and can be consolidated (see [§5](#pkgcontrollersnodeadoption--existing-node-adoption-controller)). Set `false` to leave them outside Karpenter's control |

> **Why `karpenter` namespace for leader election?** Rafay's platform webhook blocks Lease writes in `rafay-system`. The controller uses `karpenter` namespace to avoid this restriction. The same namespace hosts the headroom pause-pod Deployments and the `headroom-policy` ConfigMap, so do not quota its pods to zero or label it with a Pod Security profile the pause pods cannot satisfy.

### Kubernetes Secrets

| Secret | Namespace | Contents |
|--------|-----------|---------|
| `edge-client-creds` | `rafay-system` | `client.crt`, `client.key`, `ca.crt` — mounted at `/opt/rcloud/certs/` |
| `karpenter-provider-rafay` | `rafay-system` | Optional `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` via `envFrom` |

### edge-broker (control plane)

edge-broker reads its configuration through viper with `SetEnvPrefix("EDGE_BROKER")`, so every key below is an **`EDGE_BROKER_`-prefixed** variable; only the four `*_SERVICE_ADDR` keys are additionally read under their bare name first (`os.Getenv`).

| Variable | Default | Description |
|----------|---------|-------------|
| `EDGE_BROKER_SERVER_PORT` | `5448` | mTLS listener port (`KarpenterBatchService`, `KarpenterConfigService`, `EdgeCommandService`) |
| `EDGE_BROKER_INTERNAL_PORT` | `5449` | Plaintext internal listener (`EdgeBrokerService`; the Karpenter services are registered but unusable there) |
| `EDGE_BROKER_CERT_FOLDER` | `/etc/rcloud/certs` | TLS certificate directory |
| `EDGE_BROKER_REDIS_ADDR` | `admin-redis:6379` | Redis address |
| `EDGE_BROKER_REDIS_DB` | `3` | Redis database index |
| `EDGE_BROKER_EDGE_HOST` | `edgesrv.rcloud-admin.svc.cluster.local` | Edge service address |
| `EDGE_BROKER_EDGE_PORT` | `50701` | Edge service port |
| `EDGE_BROKER_INFRA_API_SERVER_ADDR` | `infra-apiserver:7000` | Infra API server address (v2 cluster resolution) |
| `WORKSPACE_SERVICE_ADDR` (or `EDGE_BROKER_WORKSPACE_SERVICE_ADDR`) | `paas-api:6000` | PaaS workspace RPC service address (compute instances) |
| `COMPUTE_PROFILE_SERVICE_ADDR` (or `EDGE_BROKER_…`) | `paas-api:6001` | PaaS compute RPC service address (`PaasProfileService`), used by `GetKarpenterConfig` to read node SKUs. A separate listener from `WORKSPACE_SERVICE_ADDR`; unset ⇒ that one RPC returns `FailedPrecondition` |
| `MKS_SERVICE_ADDR` (or `EDGE_BROKER_…`) | `paas-api:6011` | MKS ClusterService — first-class clusters; also the `GetCluster` probe that selects the backend |
| `DEV_STORAGE_SERVICE_ADDR` (or `EDGE_BROKER_…`) | `paas-api:6010` | Dev-layer `BmProfile` / `VmProfile` RPC — first-class SKUs |
| `KARPENTER_NODEPOOL_CPU_LIMIT` | `1000` | `spec.limits.cpu` on generated NodePools — a blast-radius guard rendered **only** on live pools **without** a catalog `maxNodeCount` (a pool with a maximum renders `limits.nodes` alone); a value below one node of the pool's SKU is raised to one node with a warning. Empty ⇒ omit; not a Kubernetes quantity ⇒ WARN and keep the default |
| `KARPENTER_NODEPOOL_MEMORY_LIMIT` | `1000Gi` | `spec.limits.memory` on generated NodePools (same rules) |
| `KARPENTER_NODEPOOL_GPU_LIMIT` | `16` | GPU limit on generated NodePools, applied only to GPU SKUs (same rules) |
| `EDGE_BROKER_PAAS_SERVICE_USERMETA_ID` | (empty) | Service identity for PaaS calls (no built-in default) |
| `EDGE_BROKER_PAAS_SERVICE_USERNAME` | (empty) | Username for PaaS calls (no built-in default) |

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
      nvidia.com/gpu: "8"     # TEMPORARY — see below. Only for SKUs that really have accelerators
      architectures: ["amd64"] # default when omitted
      operatingSystems: ["linux"] # default when omitted
```

> **`nvidia.com/gpu` is TEMPORARY and gets removed.** It exists so that a pod requesting a GPU can
> be provisioned for at all: an instance type whose `Capacity` omits the extended resource is
> filtered out by the scheduler's *fits* check, so a `nvidia.com/gpu` pod never yields a NodeClaim
> and stays `Pending` with `Failed to schedule pod, no instance type has enough resources`. The
> broker fills the field **only** for a SKU whose ComputeProfile declares a positive `gpu_count`
> (or an alias: `gpucount`, `gpus`, `gpu`, `num_gpus`, …); a SKU without one advertises no GPU at
> all — there is no fixed fallback any more, so a GPU pod is never scheduled onto a CPU-only SKU,
> and a SKU with real GPUs **must** declare its count on its profile to be GPU-schedulable.
>
> Why it is still a stopgap rather than GPU support:
>
> - **The resource name is hardcoded**, so an `amd.com/gpu` SKU is advertised under the wrong key.
>   The broker already resolves the real name per SKU (`KarpenterNodeSku.GPUResourceName`); the
>   replacement should carry that through `instanceTypes` instead.
> - **A wrong count is unrecoverable at runtime.** A node that registers but never reports
>   `nvidia.com/gpu` in its allocatable leaves its NodeClaim at `Initialized=Unknown` **forever**
>   (`RequestedResourcesRegistered`; there is no Initialized timeout, and `liveness.go` only reaps
>   NodeClaims that fail to become *Registered*). The node is then never disruptable and the pod
>   never runs. A zero or absent count is dropped rather than advertised
>   ([`rafayInstanceTypesToKarpenter`](../pkg/cloudprovider/cloudprovider.go)) so neither a
>   hand-written NodeClass nor the broker can walk into this by omission — only by a profile that
>   claims GPUs it does not have.
>
> The GPU device plugin must also tolerate whatever taints the catalog puts on the pool, or the same
> stuck-`Initialized` outcome follows on a node that really does have GPUs.
>
> Note the synthetic price still ignores accelerators, so a GPU
> SKU and a same-shape non-GPU SKU price identically. Consolidation only replaces a node with a
> strictly cheaper one, so a GPU node left holding cpu-only pods is never consolidated away.

To remove the whole mechanism, delete together: `InstanceTypeSpec.GPU`, the CRD property, the
`Capacity` entry and `temporaryGPUResourceName` in `rafayInstanceTypesToKarpenter`, and edge-broker's
`instanceTypeGPUCapacity` / `yamlInstanceType.GPU`.

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

Both node-add and node-remove requests are batched via `KarpenterBatchService.BatchStreamOperations` (adds and removes as separate batches with the same windowing). When multiple pods become unschedulable — or the disruption controller retires several nodes — Karpenter issues concurrent `Create()`/`Delete()` calls; the `NodeBatcher` collects each kind into a single broker request (up to 10 nodes / 10s window; the broker caps a batch at 64), and the broker applies one bulk catalog mutation per batch. The old per-operation `KarpenterNodeService.StreamOperations` delete path was removed: it held a stream open for the full removal (up to an hour) and its pending-claim cancellation path was broken.

### ACK-based unblocking for both Create() and Delete()

`Create()` and `Delete()` block only until the broker acknowledges the batch (typically seconds), well within Karpenter's `launchTimeout` (5 minutes) — no stream is held open for the duration of the work. The two calls then converge by different routes:

- **Adds**: the real ProviderID is resolved later from the joined node by the `NodeProviderIDController`. Broker SUCCEEDED is not on the critical path.
- **Removes**: `Delete()` returns `nil` at ACK, Karpenter requeues every 5s and calls it again, and the batcher suppresses the duplicate sends. Those duplicates come back as `BatchResult.Duplicate`, and `Delete()` logs them at V(2) — only the first ACK per removal is an Info line (a 15-minute retire would otherwise log ~180 of them). The call converges once the status poller records the remove operation SUCCEEDED **and** the NodeClaim's machine has stopped, at which point `Delete()` returns `NodeClaimNotFoundError` — the only signal that releases the Node's termination finalizer. Returning `nil` forever (the original behaviour) left NodeClaims and Nodes `Terminating` indefinitely.

Node existence is deliberately **not** the completion signal for removes: Karpenter's own finalizer keeps the Node object alive until `Delete()` reports the instance gone, so polling for the Node's disappearance is circular and never converges. Node *readiness* is a valid signal for the opposite question — the finalizer keeps the object, not the kubelet's heartbeat — which is how `Delete()` tells the platform retired this machine rather than another one.

### Failure feedback via status poller + failure handler

Because callers unblock at ACK, broker-side failures need an asynchronous path back into the cluster: the status poller maps terminal FAILED results to the `FailureHandler`, which records the operation in `PoolBackoff` and deletes the still-pending NodeClaim (UID == operationID). Karpenter reprovisions within seconds instead of waiting out the 60-minute registration-timeout backstop. The failures that must *not* be retried at once — the broker refusing the node for a reason no retry fixes (`pool at maximum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling`, `pool precondition`) — additionally hold the NodePool back for a cooldown through `PoolBackoff` and record a Warning event on it, so the scheduler withholds the pool rather than creating a NodeClaim the broker would refuse again (see [Pool maximum: three layers](#pool-maximum-three-layers)).

SUCCEEDED results are **recorded**, not merely logged. For adds the record is incidental — registration is driven by the node actually joining — though `Delete()` consults it for a pending NodeClaim (an add that SUCCEEDED needs an untargeted remove, a cancel cannot undo it). For **removes** it is the convergence signal that lets `Delete()` return `NodeClaimNotFoundError` and release the Node's termination finalizer.

### Synchronous cancellation for pending deletes

Deleting a NodeClaim whose add is still open at the broker sends `cancel_ops` for the queued add and waits for the answer. Cancellation is deliberately narrow: only ops still ACCEPTED at the broker are transitioned (CAS) to FAILED `"cancelled by client"` — an op that is already RUNNING has potentially mutated the catalog and is left to finish. That is why the answer matters: a cancelled add lets `Delete()` finalize the NodeClaim at once, while a non-cancellable one keeps the NodeClaim (`nil`) until the poller reports the add's outcome, so the machine that lands has an owner and, if it must go, gets a proper remove. A node that joined with no NodeClaim waiting for it (a lost cancel from an older provider) is reclaimed only by the node-adoption controller — once Ready, and then only when empty under the pool's `WhenEmpty` policy; with adoption disabled it is an unowned machine that must be removed from the catalog by hand.

### Removals are catalog decrements (PaaS limitation)

The PaaS worker-node catalog is declarative per pool+SKU counts, so the broker applies removals as negative deltas (refused below the pool's minimum) and **PaaS chooses which physical machine is retired**. The node's ProviderID is carried on `KarpenterBatchNodeRemoveItem` so a targeted-removal API can be adopted later without a protocol change. Until then the provider compensates: no remove is sent for a machine that is already gone (the GC-cascade guard and `retiredExternally`), a SUCCEEDED remove counts as done only once the NodeClaim's own machine has stopped, and a remove after which the platform demonstrably retired another machine is held with `RemoveRetiredOtherMachine` rather than re-sent.

### Synthetic pricing for instance-type selection

The private cloud has no price list, so each instance type gets a synthetic relative cost (1.0 per vCPU + 0.125 per GiB of memory). `Create()` picks the cheapest compatible type (stable sort, NodeClass spec order on ties), and consolidation gets a gradient for replacing underutilized nodes with smaller ones.

### ProviderID resolved from Kubernetes Node, not broker response

Rather than relying on the broker to return a provider ID in the batch response, the provider reads `node.Spec.ProviderID` directly from the Kubernetes node once it joins. This decouples the provider from the broker's response format and uses the node's own identity as the canonical identifier.

### NodeProviderIDController: single-threaded with uncached reads

`MaxConcurrentReconciles: 1` serializes the read-decide-patch cycle for node assignment. `apiReader` (direct API server reads) ensures that a just-patched ProviderID is visible to the next reconcile, preventing two pending NodeClaims from claiming the same node even if the informer cache is stale. `CloudProvider.findNodeProviderID` applies the same discipline on the delete path.

### NodeClaims as sole source of truth

No in-memory state is load-bearing across a pod restart. NodeClaims in etcd are the sole source of truth. `NodeProviderIDController` recovers on startup by re-processing all existing NodeClaims with `rafay://pending/` ProviderID. Broker idempotency (deterministic operation IDs) ensures no duplicate provisioning.

### No retry on batch sends

`SendBatch` and `SendBatchRemove` use `callBrokerOnce`. Retrying a send that partially executed (e.g. broker accepted but the response was lost) would cause double-provisioning. On transport failure, recovery is via application-level retry paths (Karpenter re-calling `Create()`/`Delete()` with the same operation ID → broker deduplication). Read-only polls, idempotent cancels and the config fetch retry once on `Unavailable`, on the same connection — the shared connection is never closed by an RPC failure.

### In-band rejection and expiry instead of stream errors

Queue-full at the broker answers `batch_rejected` and unknown batchIDs answer an empty `batch_status` — both keep the stream and connection healthy instead of failing them. The client converts repeated empty polls (or 3h of tracking) into per-item failures, so nothing hangs forever on a lost broker record.

### Headroom via per-pool Deployments

Proactive capacity is maintained by the headroom controller as one Deployment of negative-priority pause pods per pool — the standard cluster-overprovisioner pattern — instead of managing bare pods. The ReplicaSet replaces preempted placeholders automatically; the controller only computes replica counts (fraction of pool allocatable / per-pod slice, capped at 50 % and at `maxPods`) and reacts to config/node/deployment/NodePool events. Pause pods pack onto as few nodes as possible (preferred pod affinity) so `WhenEmpty` consolidation can still reclaim buffer nodes; this requires `consolidationPolicy: WhenEmpty` on participating NodePools, since pause pods intentionally keep nodes non-empty.

### No broker query API

`GetNode` and `ListNodes` return sentinel errors; the provider falls back to listing Kubernetes `Node` objects filtered by `spec.providerID` prefix `rafay://`. The Kubernetes API is the ground truth for what is currently running.

### No cloud-provider drift detection

`IsDrifted` always returns `("", nil)`: the provider never reports a machine as drifted on its own
(no image, firmware or instance-metadata comparison), because the physical machine's lifecycle
belongs to Rafay's control plane. Karpenter core's own drift checks still run — and are evaluated
*before* `IsDrifted` is consulted: static drift (a `karpenter.sh/nodepool-hash` mismatch, which any
NodePool template edit triggers), requirements drift (pool requirements vs NodeClaim labels) and
`instanceTypeNotFound` (a SKU dropped from the NodeClass). On broker-rendered pools a drifted
NodeClaim is **marked but never replaced**: every live pool carries a Drifted-scoped `nodes: "0"`
disruption budget (§7.1), because an untargeted remove makes a roll non-convergent. Only a
hand-written pool without that budget rolls drifted nodes. Adopted NodeClaims carry no
nodepool-hash annotation and are durably exempt from static drift (§6.6).

### Parity with `edge-client`

The TLS certificate path, broker host extraction from cert Subject OU, port environment variable names, keepalive parameters (5-min ping / 30s timeout), max message size (20 MB), and `sessionid` UUID generation all mirror `edge-client` so the same certificate material and deployment patterns apply without additional configuration.

### Instance types validated at admission and at call time

The readiness controller marks a NodeClass `Ready=False` (reason `ValidationFailed`) when `spec.instanceTypes` is empty or malformed, blocking provisioning at the NodePool gate. `rafayInstanceTypesToKarpenter` additionally uses `resource.ParseQuantity` (not `MustParse`) and returns an error for malformed `cpu`, `memory` or `nvidia.com/gpu` values, surfacing misconfiguration cleanly rather than panicking the controller. Both checks cover the same fields on purpose: a quantity that readiness accepts but `GetInstanceTypes` rejects would leave the NodeClass `Ready=True` while every provisioning attempt failed.
