# karpenter-provider-rafay — Architecture

> **Confluence note:** Diagrams in this document use Mermaid syntax. Render them with the **Mermaid Diagrams for Confluence** app, or paste each block into [mermaid.live](https://mermaid.live) to preview.

---

## Table of Contents

1. [Overview](#1-overview)
2. [System Architecture](#2-system-architecture)
3. [Component Descriptions](#3-component-descriptions)
4. [Data Flows](#4-data-flows)
5. [Core Packages](#5-core-packages)
6. [Worker Node SKU Catalog](#6-worker-node-sku-catalog)
7. [Operation State Machine](#7-operation-state-machine)
8. [NodeClaim Registration Timeline](#8-nodeclaim-registration-timeline)
9. [Identity and Security](#9-identity-and-security)
10. [Provider ID Format](#10-provider-id-format)
11. [Idempotency and Resilience](#11-idempotency-and-resilience)
12. [Timeouts Reference](#12-timeouts-reference)
13. [Configuration Reference](#13-configuration-reference)
14. [Key Dependencies](#14-key-dependencies)
15. [Design Decisions](#15-design-decisions)

---

## 1. Overview

`karpenter-provider-rafay` integrates Karpenter's scheduling engine with Rafay's private-cloud infrastructure layer. It is **not** a public cloud provider — there are no AWS/GCP/Azure API calls. Node-add **and** node-remove requests are batched and forwarded to the **edge-broker** over mTLS gRPC on a single protocol: `KarpenterBatchService.BatchStreamOperations`.

### Key Properties

| Property | Detail |
|---|---|
| **Add protocol** | `KarpenterBatchService.BatchStreamOperations` `batch_add` — up to 10 nodes per batch, 10 s collection window |
| **Remove protocol** | Same stream, `batch_remove` frame — removes share the batcher with adds, sent as separate batches |
| **Cancel protocol** | Same stream, `cancel_ops` frame — best-effort cancel of operations still queued (ACCEPTED) at the broker |
| **Transport security** | mTLS — client certificate required; edge identity derived from cert Subject O. Batch operations **cannot** be served over a plaintext listener (no client cert → no edge id) |
| **State store (broker)** | Redis — per-op records (15-min TTL while ACCEPTED, **2 h** once RUNNING and on a terminal FAILED, **24-h tombstone once SUCCEEDED**) + per-batch index (3-h TTL) + per-cluster processing lock (2 h). Every record is scoped to the authenticated `edge_id` |
| **Deadlines** | Adding a node can take up to **60 min**. Broker add processing 90 min, remove 60 min; provider batch tracking 3 h; Karpenter registration timeout (fork) 60 min |
| **Provisioning backend** | `WorkspaceComputeInstance` via `WorkspaceRPCService` (paas-api) — declarative pool+SKU counts — for oneclick clusters; MKS `Cluster` `scaling.desired` for first-class clusters |
| **Idempotency key** | Add: `NodeClaim.UID` · Remove: `NodeClaim.UID + "-remove"` — prevents duplicate catalog mutation on retry |
| **Removal granularity** | Catalog **count decrement** per pool+SKU — the PaaS platform chooses which physical machine is removed (`provider_id` is carried for a future targeted-removal API); `Delete()` waits for the NodeClaim's own machine to stop before finalizing |
| **ProviderID resolution** | `NodeProviderIDController` patches real `spec.providerID` once the node joins the cluster |
| **Pre-existing nodes** | `NodeAdoptionController` creates a NodeClaim per unclaimed worker node in a pool (no broker call), so the pool's original nodes count towards its limits and can be consolidated |
| **Failure recovery** | Status poller reports broker-side FAILED ops to a failure handler that deletes the still-pending NodeClaim, so Karpenter reprovisions in seconds instead of waiting out the 60-min registration timeout. A permanent refusal (`pool at maximum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling`, `pool precondition`) additionally holds the NodePool back for a cooldown (default 5 min) and records a `PoolAtPlatformMaximum` / `PoolRefusedByPlatform` event so Karpenter does not re-ask at once |
| **Removal completion** | Status poller records broker-side SUCCEEDED ops; `Delete()` returns `NodeClaimNotFoundError` once the remove op has SUCCEEDED and the NodeClaim's Node is gone or NotReady — the only signal that releases the Node's termination finalizer |
| **Expiry / drift** | Every broker-rendered NodePool sets `expireAfter: Never` and a Drifted-scoped `nodes: "0"` budget: no age-based rotation, drift is marked but never rolled |
| **Source of truth** | NodeClaims in etcd — no in-memory state is load-bearing across pod restarts |

### High-Level Architecture

```mermaid
graph TB
    KARP["Karpenter Autoscaler"]
    CP["karpenter-provider-rafay\nCloudProvider interface"]
    EB["Rafay edge-broker\nKarpenterBatchService.BatchStreamOperations\nRedis op records + per-edge batch queues"]
    CTRL["Rafay control plane\nKarpenterBackend (oneclick | first-class)"]
    NODE["Node joins cluster\nspec.providerID = rafay://pool/sku/hostname"]
    NPID["NodeProviderIDController\npatches NodeClaim.Status.ProviderID"]
    REG["Karpenter registration reconciler\nNodeClaim Registered=True"]

    KARP -->|"Create / Delete / Get"| CP
    CP -->|"batch_add · batch_remove · status_poll · cancel_ops\n(batch gRPC, mTLS)"| EB
    EB --> CTRL
    CTRL --> NODE
    NODE --> NPID
    NPID --> REG
```

---

## 2. System Architecture

```mermaid
graph TB
    subgraph K8S["Managed Kubernetes Cluster"]
        subgraph POD["karpenter-provider-rafay Pod  (rafay-system namespace)"]
            KC["Karpenter Core Controllers\n(upstream — scheduling, disruption, lifecycle)"]
            CP["CloudProvider\nCreate · Delete · Get · List · GetInstanceTypes"]
            NB["NodeBatcher\nbatchSender + statusPoller goroutines\nFailureHandler on broker-side FAILED"]
            BC["BrokerClient\nOne shared grpc.ClientConn  (lazy, mutex-protected)"]
            NPID["NodeProviderID Controller\nWatches NodeClaims + Nodes\nPatches real ProviderID"]
            RNC["RafayNodeClass Controller\nValidates spec.instanceTypes\nSets Ready=True / Ready=False"]
            HR["Headroom Controller\nPer-pool Deployments of pause pods\n(proactive scale-out buffer)"]
        end
        NC[("NodeClaims\netcd — sole source of truth")]
        KN["Kubernetes Nodes\nspec.providerID = rafay://..."]
    end

    EB["Rafay edge-broker\nKarpenterBatchService.BatchStreamOperations\nRedis op state  (15 min ACCEPTED / 2 h RUNNING / 24 h SUCCEEDED)\nPer-edge batch queues (one batch at a time per edge)"]

    subgraph PLANE["Rafay Control Plane"]
        LC["KarpenterBackend\n(oneclick | first-class)"]
        ES["edgesrv\nedge registry"]
        PA["paas-api\nWorkspaceRPCService"]
        IAAS["IaaS / Cloud\nVM / node pool"]
    end

    KC -->|CloudProvider interface| CP
    CP -->|"Enqueue (add)\nEnqueueRemove (delete)\nCancel (pending delete)"| NB
    NB -->|"SendBatch · SendBatchRemove\nPollBatchStatus · CancelOperations"| BC
    BC -->|"gRPC over mTLS  port 5448\nmetadata: sessionid=STREAM_ID"| EB
    NPID -.->|Watch| NC
    NPID -.->|Watch| KN
    NPID -->|Patch ProviderID| NC
    RNC -->|"status.conditions[Ready]"| NC

    EB --> LC
    LC --> ES
    LC --> PA
    PA --> IAAS
    IAAS -->|"Node joins\nRafay sets spec.providerID"| KN
```

---

## 3. Component Descriptions

### 3.1 In-Cluster Components

| Component | Package | Responsibility |
|---|---|---|
| **CloudProvider** | `pkg/cloudprovider` | Implements `Create`, `Delete`, `Get`, `List`, `GetInstanceTypes`. `Create` picks the **cheapest compatible** instance type (synthetic price), blocks until broker ACK, then returns a synthetic pending ProviderID (a missing / not-Ready NodeClass is `NodeClassNotReadyError`). `Delete` enqueues a batch remove, returns at broker ACK, and is re-invoked by Karpenter every 5 s until the broker reports the removal SUCCEEDED **and** the NodeClaim's own machine has stopped — at which point it returns `NodeClaimNotFoundError` and the Node's finalizer is released. It sends no remove for a machine that is already gone, and records `NodeNotRetired` / `NodeRetiredExternally` / `RemoveRetiredOtherMachine` events on the NodePool for the odd outcomes. |
| **NodeProviderIDController** | `pkg/controllers/nodeproviderid` | Watches NodeClaims with `rafay://pending/` ProviderID and real Rafay nodes. Labels the node `karpenter.sh/registered=true`, then patches the real ProviderID once the node joins. Single-threaded (`MaxConcurrentReconciles: 1`), serialized with adoption and `Delete()` by `cloudprovider.NodeOwnershipMu`. |
| **NodeAdoptionController** | `pkg/controllers/nodeadoption` | Creates a NodeClaim for each Ready worker node the platform built **before** Karpenter ran (labelled `nodepoolname`/`sku_name` but unclaimed), filling in `spec.providerID` when it is empty. Without it a pool's pre-existing nodes count for nothing — Karpenter provisions on top of its own `limits` and can never consolidate them. The NodeClaim carries `karpenter.rafay.io/adopted-provider-id`, which makes `Create()` return that ProviderID instead of asking the broker for a machine. Skips pools annotated `karpenter.rafay.io/auto-scaling=false`. Single-threaded (`MaxConcurrentReconciles: 1`); disabled by `KARPENTER_ADOPT_EXISTING_NODES=false`. |
| **BrokerClient** | `pkg/rafay/brokerclient.go` | One shared `*grpc.ClientConn`, never closed on an RPC error; every batch RPC carries a 30 s deadline (90 s for `GetKarpenterConfig`). `SendBatch`/`SendBatchRemove` are never retried (`callBrokerOnce`); `PollBatchStatus`/`CancelOperations`/`GetKarpenterConfig` retry once on `Unavailable`, on the same connection (`callBroker`). |
| **NodeBatcher** | `pkg/rafay/batcher.go` | Collects `Create()`/`Delete()` requests into batches (up to 10 / 10 s, adds and removes partitioned), sends via `SendBatch`/`SendBatchRemove`, unblocks callers at broker ACK, polls via `PollBatchStatus` every 30 s from the next tick (no initial delay), tracks a batch for up to 3 h. Deduplicates retries by `operationID`. FAILED results invoke the registered `FailureHandler`. |
| **Node config Controller** | `pkg/controllers/nodeconfig` | Fetches the broker-rendered `RafayNodeClass` / `NodePool` manifests (`GetKarpenterConfig`) at startup and every `KARPENTER_CONFIG_SYNC_INTERVAL` (10 min) and server-side-applies them **every** interval (an unchanged revision only lowers the log level); applies the inert shape when autoscaling is off; never prunes. A `NotFound` carrying `karpenter config unavailable for this edge` is a quiet no-op; other errors retry on a 5 s → 2 min backoff. |
| **RafayNodeClass Controller** | `pkg/controllers/rafaynodeclass` | Validates `spec.instanceTypes` (non-empty, names set, cpu/memory quantities parse). Valid → `Ready=True`; invalid → `Ready=False` with reason `ValidationFailed`, which blocks the NodePool readiness gate. |
| **Headroom Controller** | `pkg/controllers/headroom` | Maintains one Deployment (`headroom-<pool>`) of low-priority pause pods per pool configured in the `headroom-policy` ConfigMap, keeping a preemptible capacity buffer that triggers proactive scale-out; holds it at 0 replicas while the pool is inert. Watch-driven (ConfigMap/Node/Deployment/NodePool events) with a 5-min resync. |
| **BatchResume Controller** | `pkg/controllers/batchresume` | One leader-only pass at startup: lists pending (`rafay://pending/`) NodeClaims, groups them by the `karpenter.rafay.io/batch-id` annotation that `Create()` stamps at ACK, and re-registers each batch with the NodeBatcher so the status poller keeps following adds sent by the previous process. Without it a restart trades the batcher's poll-interval FAILED signal for the 60-minute registration timeout. Removes are not resumed (Karpenter re-issues `Delete()`). |
| **RafayNodeClass CRD** | `pkg/apis/v1alpha1` | Cluster-scoped CRD defining available instance types (`spec.instanceTypes`) — and nothing else. It carries no Rafay cluster/project identity; that comes only from the controller's `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` env. |

### 3.2 edge-broker (Control Plane)

| Port | TLS | Karpenter service |
|---|---|---|
| `5448` (`EDGE_BROKER_SERVER_PORT`) | mTLS (client cert required) | `KarpenterBatchService` + `KarpenterConfigService` — **the only usable port for node operations** |
| `5449` (`EDGE_BROKER_INTERNAL_PORT`) | Plaintext (internal only) | `EdgeBrokerService` (internal callers only); the Karpenter services are registered but **cannot serve node operations** |

> ⚠️ **`KarpenterBatchService` cannot be served over the plaintext listener.** Every `BatchStreamOperations` handler needs the caller's edge id, and that id is derived from the **mTLS client certificate** (Subject Organization, via `common.GetEdgeClientInfo`). A plaintext connection has no peer certificate, so the stream is rejected with `ErrorNoClientID` (`"NO CLIENT ID"` — the broker maps every `GetEdgeClientInfo` failure to it and never surfaces edge-common's internal `ErrorInvalidPeer`). Batch operations must use `5448`; `EDGE_BROKER_GRPC_INSECURE=true` pointed at `:5449` cannot work for them. (Before the fix, `GetEdgeClientInfo` did an unchecked `p.AuthInfo.(credentials.TLSInfo)` assertion — `AuthInfo` is `nil` on a plaintext connection, so opening the stream on `:5449` **panicked and crashed the whole broker process**, since gRPC did not recover handler panics. It now fails cleanly, and both servers carry panic-recovery interceptors.)

**`BatchStreamOperations` request dispatch** (client→broker union, field tags in parentheses):

| Frame | Handler |
|---|---|
| `batch_add` (1) `KarpenterBatchNodeAddRequest` | `handleBatchAdd` — classifies the batch first (≤ 64 nodes, this edge's queue below 16 waiting batches, batch id not owned by another edge; otherwise `batch_rejected` with nothing written), then per-op idempotency check in Redis, writes ACCEPTED records (`kind:add`, with `edge_id`/`project_id`/`cluster_id`), writes batchID→opIDs index, enqueues onto **this edge's** queue, replies `batch_accepted` |
| `status_poll` (2) `KarpenterBatchStatusPoll` | `handleBatchStatusPoll` — reads the batch index + per-op records, replies `batch_status`; **unknown/expired batchID replies an empty `batch_status` and keeps the stream alive**. A batch owned by a **different edge** is answered exactly like an unknown one, so no cross-edge state leaks |
| `batch_remove` (3) `KarpenterBatchNodeRemoveRequest` | `handleBatchRemove` — mirrors `handleBatchAdd` with `kind:delete` records and the same per-edge queue; `provider_id` may be empty (untargeted) |
| `cancel_ops` (4) `KarpenterBatchOperationCancel` | `handleCancelOps` — CAS-transitions ops **still ACCEPTED** (first 64 ids) to FAILED `"cancelled by client"`; RUNNING/terminal ops untouched, as are ops owned by **another edge**; replies `cancel_ack` with the IDs actually cancelled |

**Broker→client union:** `batch_accepted` (1), `batch_status` (2), `batch_rejected` (3), `cancel_ack` (4).

If this edge's queue already holds 16 waiting batches, the broker replies **`batch_rejected` (`"batch queue full"`) in-band — the stream stays alive, nothing is written** — and the client surfaces `ErrBatchRejected`. In the residual race where the queue fills between the check and the enqueue, only the ops this call itself wrote are moved ACCEPTED→FAILED (`"batch queue full; try again"`, 15-min TTL) with a **compare-and-set**, not a blind patch, so it cannot clobber an op that a processor concurrently claimed as RUNNING (which is already doing catalog work and must not be reported FAILED).

**Edge scoping:** every `nodeop` and `batchop` record carries the `edge_id` of the authenticated stream that created it, and a batchop index owned by another edge is never overwritten. Records with an **empty** `edge_id` were written by an older broker and stay visible to everyone, so operations in flight across a broker upgrade are unaffected.

One processor goroutine **per edge** drains that edge's FIFO **one batch at a time** (different edges in parallel; processors start on the edge's first batch and are reaped after 1 h idle; `RunProcessor` is the supervisor); each batch carries exactly one kind (add or remove). Across broker replicas a per-cluster Redis lock (`/edge/karpenter/lock/<edgeID>`, 2 h) serializes the catalog read-modify-write. On restart `sweepOrphanedKarpenterOps` re-queues every still-ACCEPTED op within seconds.

### 3.3 Redis — Operation State Store

Three key families:

- `/edge/karpenter/nodeop/<operationID>` — per-operation record. TTL **15 min while ACCEPTED**, re-armed every 5 minutes by the broker's per-queue keepalive for as long as the batch waits behind the same edge's earlier batches (so a queued op only expires if the broker that holds it dies without recovery; a broker restart is covered by the startup sweep), extended to **2 h** (`karpenterBatchAddTimeout` + 30 min) when the processor transitions it to RUNNING and on a terminal FAILED write (15 min for the `batch queue full; try again` / `broker shutting down; retry` / `cluster locked by another broker; retry` rejections). A terminal **SUCCEEDED** write instead gets a **24-hour tombstone TTL** (see below).
- `/edge/karpenter/batchop/<batchID>` — batchID → operation IDs index used by status polls. TTL **3 h**.
- `/edge/karpenter/lock/<edgeID>` — per-cluster processing lock (owner token `brokerID/batchID/nanos`). TTL **2 h**.

Both record types carry the authenticated **`edge_id`** of the stream that created them (plus `project_id` / `cluster_id`, so a swept batch resolves the same backend).

> **SUCCEEDED records are tombstones — a correctness requirement, not a cache.** Clients re-send a deterministic operationID (`<uid>-remove`) for the entire life of their retry loop, so a terminal record must outlive any plausible retry horizon. When SUCCEEDED expired with the then 60-minute RUNNING TTL, a re-sent remove opID looked like a **brand-new** operation: it was ACCEPTED again and the processor **decremented the catalog a second time**, so the platform retired an extra healthy machine — once per hour, per terminating NodeClaim. The TTL is now **24 h** (`karpenterBatchSucceededTTL`) and is **refreshed on every duplicate send**, so a persistently retrying client keeps its own tombstone alive; a remove is tombstoned the moment its decrement is published. **FAILED** records are not tombstoned: a FAILED op **must** stay retryable, and letting it expire is equivalent to re-accepting it.

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
|---|---|---|
| 0 | `UNKNOWN` | Never written; a corrupt/resurrected record — never re-accepted, never re-created |
| 1 | `ACCEPTED` | Request recorded, batch queued, processor not yet started |
| 2 | `RUNNING` | Processor active, catalog mutation in progress |
| 3 | `SUCCEEDED` | Operation complete (adds carry provider IDs; removes carry none, written at publish; also `committed; …` past the commit point and `node not retired: …` for a refused remove) |
| 4 | `FAILED` | Failed / cancelled / expired / permanently refused (`pool at maximum`, `pool not found`, …) — detail contains the reason |

State transitions that race (ACCEPTED→RUNNING vs. cancel) go through `casKarpenterNodeOpState`, a Redis optimistic transaction (`WATCH`/`MULTI`/`EXEC`) that only commits if the record still holds the expected state.

### 3.4 Provisioning Backend

`KarpenterBackend` (`karpenter_backend.go`; selected per edge) translates each batch into **one bulk platform mutation**: the accepted ops are grouped by pool+SKU into deltas (positive for adds, negative for removes — refused below the pool's minimum, never clamped into a phantom). The **oneclick** backend edits the `WorkspaceComputeInstance` catalog via `WorkspaceRPCService` (paas-api) in a single Get→apply deltas→Apply→Publish pass (the commit point), then polls the compute instance every 30 s until it reaches a terminal state; the **first-class** backend sets `scaling.desired = live count ± delta` on the MKS `Cluster`, `ApplyCluster`, then polls `GetClusterStatus`. The whole run is bounded by the batch deadline (90 min add / 60 min remove) only. See [§6 Worker Node SKU Catalog](#6-worker-node-sku-catalog).

---

## 4. Data Flows

### 4.1 Scale-Out (Node Provisioning)

```mermaid
sequenceDiagram
    autonumber
    participant K as Karpenter Core
    participant CP as CloudProvider
    participant NB as NodeBatcher
    participant BC as BrokerClient
    participant EB as edge-broker
    participant NPID as NodeProviderID Ctrl
    participant N as Kubernetes Node

    K->>CP: Create(NodeClaim)
    CP->>CP: Resolve RafayNodeClass<br/>Pick cheapest compatible InstanceType
    CP->>NB: Enqueue(operationID=NodeClaim.UID, req)
    Note over NB: First item starts the window —<br/>collect up to 10 items / 10 s
    NB->>BC: SendBatch(nodes[])
    BC->>EB: batch_add{batchId, nodes[]}
    EB-->>BC: batch_accepted{batchId}
    BC-->>NB: batchId stored in inProgress map
    NB-->>CP: BatchResult{} on broker ACK
    CP-->>K: NodeClaim{providerID="rafay://pending/<uid>", capacity}
    Note over K: Launched=True set in etcd
    Note over NB,EB: Background: status_poll every 30 s<br/>from the next tick (no initial delay).<br/>SUCCEEDED → recorded (adds: node join drives registration).<br/>FAILED → FailureHandler deletes the<br/>still-pending NodeClaim (fast reprovision;<br/>a permanent refusal holds the pool first).
    Note over K,N: ~12 minutes later (up to 60)
    N-->>N: Node joins cluster<br/>Rafay platform sets node.Spec.ProviderID
    NPID->>K: List NodeClaims (direct API reader — no cache)
    NPID->>K: Patch NodeClaim.Status.ProviderID = node.Spec.ProviderID
    K->>K: Registration reconciler: Registered=True
```

### 4.2 Scale-In (Node Deprovisioning)

```mermaid
sequenceDiagram
    autonumber
    participant K as Karpenter Disruption
    participant CP as CloudProvider
    participant NB as NodeBatcher
    participant BC as BrokerClient
    participant EB as edge-broker

    Note over K,CP: Karpenter calls Delete() on EVERY reconcile (~5 s)<br/>once the Node is drained and volumes detached,<br/>and releases the Node's finalizer ONLY on NodeClaimNotFoundError.
    K->>CP: Delete(NodeClaim)
    alt batcher.SucceededResult("<uid>-remove") is set
        Note over CP: "node not retired: …" → NotFound + event NodeNotRetired<br/>IDs named, ours missing → hold (RemoveRetiredOtherMachine)<br/>untargeted: Node gone/NotReady → NotFound ✓;<br/>Node still Ready → nil for up to RAFAY_REMOVE_SETTLE_WINDOW (60m)
        CP-->>K: NodeClaimNotFoundError — removal complete ✓<br/>(Karpenter releases the finalizer and finishes)
    end
    alt ProviderID is rafay://pending/<uid>  (resolvePendingRemoval — the add's own state decides)
        alt add SUCCEEDED at the broker
            CP->>CP: findNodeProviderID (apiReader, NodeOwnershipMu)<br/>reserve the joined node's ID on the NodeClaim, else untargeted
        else add FAILED (recorded by the failure handler)
            CP-->>K: NodeClaimNotFoundError (no RPC)
        else add still open
            CP->>NB: Cancel(ctx, NodeClaim.UID) — SYNCHRONOUS cancel_ops<br/>(only a still-ACCEPTED add op is cancelled)
            Note over CP: cancelled → NodeClaimNotFoundError;<br/>not cancellable → nil until the poller reports the add
        end
    end
    alt first remove for this NodeClaim: no Node carries the ProviderID,<br/>or it is Terminating with a stopped kubelet, deleted outside Karpenter
        CP-->>K: NodeClaimNotFoundError — nothing sent<br/>(a remove would retire one more healthy machine; event NodeRetiredExternally)
    end
    CP->>NB: EnqueueRemove(operationID=UID+"-remove", req{providerID, pool, sku})
    alt operationID already in flight at broker
        NB-->>CP: BatchResult{} immediately — NOTHING re-sent<br/>(suppresses the duplicate batch each 5 s reconcile would push)
    else first send
        Note over NB: Same 10 item / 10 s collection;<br/>removes go out as their own batch
        NB->>BC: SendBatchRemove(nodes[])
        BC->>EB: batch_remove{batchId, nodes[]}
        EB-->>BC: batch_accepted{batchId}
        NB-->>CP: BatchResult{} on broker ACK
    end
    CP-->>K: nil — Karpenter requeues in 5 s and calls Delete() again
    Note over EB: Processor applies NEGATIVE pool+sku deltas<br/>(refused below the pool minimum → "node not retired").<br/>Ops SUCCEEDED at publish (24 h tombstone).<br/>PaaS chooses which physical machine is removed —<br/>provider_id carried for future targeted removal.
    EB-->>NB: status_poll → SUCCEEDED
    Note over NB: Recorded in `succeeded`; the NEXT Delete() call<br/>(≤5 s later) acts on it and converges once the<br/>NodeClaim's own Node is gone or NotReady.
```

> **Node existence cannot be the completion signal — readiness can.** During termination **Karpenter itself holds the Node object alive with its own finalizer**, and only drops it once `Delete()` reports the instance gone. "Is there still a Node with this providerID?" is therefore circular and never converges — the broker's SUCCEEDED result is the external signal that says the platform accepted the removal. Because removal is untargeted, SUCCEEDED alone does not say *this* machine went; the finalizer keeps the Node object, not the kubelet's heartbeat, so `Delete()` converges on SUCCEEDED plus the Node being absent or NotReady (a normal termination therefore lasts as long as the platform's real retirement). `Delete()` previously returned `nil` forever after broker ACK, so NodeClaims and Nodes stayed `Terminating` indefinitely.

### 4.3 Pod Restart / Crash Recovery

```mermaid
graph TB
    START["Pod Restarts"]

    subgraph NPID_REC["NodeProviderIDController Recovery"]
        REENQ["Re-enqueue ALL NodeClaims\nwith rafay://pending/ ProviderID\n(controller-runtime startup behavior)"]
        LIST["List Nodes by nodepoolname label\nsku_name matched in code against\ninstance-type label OR NodeClass name"]
        PATCH["Patch NodeClaim.Status.ProviderID\n= node.Spec.ProviderID"]
        WAIT["Node not yet joined\nRequeue after 30 s"]
    end

    subgraph UNKNOWN["NodeClaims with Launched=Unknown\n(broker not yet ACKd)"]
        RECREATE["Karpenter lifecycle re-calls Create()"]
        DEDUP["NodeBatcher.Enqueue(same UID)\nBroker idempotency by operationID\nNo second node provisioned"]
    end

    subgraph STABLE["NodeClaims with Launched=True\n(broker already ACKd)"]
        RESUME["NodeProviderIDController resumes\nfrom etcd — no restart logic needed"]
        BR["batchresume: re-registers each pending\nNodeClaim's batch (karpenter.rafay.io/batch-id)\nso FAILED feedback still arrives"]
    end

    START --> REENQ
    START --> RECREATE
    START --> BR
    REENQ --> LIST
    LIST -->|Node found| PATCH
    LIST -->|Not yet| WAIT
    WAIT -->|30 s| LIST
    RECREATE --> DEDUP
    DEDUP --> LIST
    STABLE --> RESUME
```

### 4.4 BrokerClient Connection Lifecycle

```mermaid
graph TB
    CALL["Broker RPC\n(30 s deadline each)"]
    GC{"getConn(ctx)"}
    EXIST["Return existing conn"]
    DIAL["dialBroker(ctx)\ngrpc.NewClient(addr, opts...)"]
    ONCE["SendBatch / SendBatchRemove\ncallBrokerOnce — NO retry"]
    RETRY["PollBatchStatus / CancelOperations / GetKarpenterConfig\ncallBroker — ResetConnectBackoff and\nretry ONCE on Unavailable, SAME connection"]
    OK["Return result"]
    ERR["Error returned"]
    KEEP["Connection KEPT — never closed on a failure\n(closing would abort every other goroutine's stream;\ngRPC reconnects by itself)\nBatch send NOT re-issued\n(prevents duplicate catalog mutation)"]

    CALL --> GC
    GC -->|"conn != nil"| EXIST
    GC -->|"conn == nil"| DIAL
    EXIST --> ONCE
    EXIST --> RETRY
    DIAL --> ONCE
    DIAL --> RETRY
    ONCE --> OK
    RETRY --> OK
    ONCE --> ERR
    RETRY --> ERR
    ERR --> KEEP
```

### 4.5 NodeClass Readiness

```mermaid
graph LR
    RNC["RafayNodeClass created / updated"]
    CTRL["rafaynodeclass.Controller.Reconcile()"]
    VAL{"validateNodeClass\nnon-empty instanceTypes,\nnames set, cpu/memory parse"}
    PATCH["Status().Patch\nconditions[Ready]=True"]
    FAIL["Status().Patch\nconditions[Ready]=False\nreason ValidationFailed"]
    GATE["NodePool readiness gate cleared"]
    ACTIVE["NodePool active for provisioning"]

    RNC --> CTRL --> VAL
    VAL -->|valid| PATCH --> GATE --> ACTIVE
    VAL -->|invalid| FAIL
```

---

## 5. Core Packages

### 5.1 NodeBatcher Internals

The `NodeBatcher` decouples individual `Create()`/`Delete()` calls from the batch gRPC protocol, providing deduplication, in-flight suppression, and efficient grouping. Callers unblock at **broker ACK**; the status poller tracks broker-side completion in the background — and for removes that completion is what lets `Delete()` converge.

```mermaid
graph LR
    subgraph API["CloudProvider"]
        ENQUEUE["Enqueue (add) / EnqueueRemove (delete)\n← resultCh  blocks until broker ACK"]
        SUCCQ["Succeeded(operationID)\n← Delete() polls this to converge"]
        CANCELFN["Cancel(ctx, operationID) (applied, err)\nsynchronous cancel_ops (10 s timeout)"]
    end

    subgraph BATCHER["NodeBatcher  (pkg/rafay/batcher.go)"]
        Q["queue chan\nbuffered 256"]
        PMAP["pending map\noperationID → []chan BatchResult\ndedup + multi-waiter fan-out"]
        INFL["inFlight set\nACKed, not yet terminal\n→ re-enqueue sends NOTHING"]
        SUCCM["succeeded map\noperationID → time observed\n(retained 2 h)"]

        subgraph SENDER["batchSender goroutine"]
            COLLECT["collectBatch\nblocks for first item,\nthen 10 s window / 10 items max"]
            SPLIT["partition by kind\nadds → SendBatch\nremoves → SendBatchRemove"]
            ACK["registerAndAck\nmark in-flight, then resolve\nEVERY waiter at broker ACK"]
            IPMAP["inProgress map\nbatchID → unresolved items"]
        end

        subgraph POLLER["statusPoller goroutine\n30 s tick · no initial delay"]
            POLL["BrokerClient.PollBatchStatus\nper in-progress batch"]
            SUCC["SUCCEEDED\nrecord in `succeeded` (+detail, IDs), clear in-flight\n→ next Delete() acts on it"]
            FAIL["FAILED\ninvoke FailureHandler\n(adds: record op, delete pending NodeClaim)\nclear in-flight → retry may re-send"]
            CONT["ACCEPTED / RUNNING\nkeep in inProgress, poll next tick"]
            EXP["3 consecutive empty polls\nor age > 3 h (maxBatchAge)\n→ drop batch, items FAILED\n'batch expired at broker'"]
        end
    end

    ENQUEUE -->|"new operationID"| Q
    ENQUEUE -.->|"existing operationID\njoin waiter list"| PMAP
    ENQUEUE -.->|"already ACKed at broker\nresolve immediately, send nothing"| INFL
    SUCC --> SUCCM
    SUCCM -.-> SUCCQ
    Q --> COLLECT
    COLLECT --> SPLIT
    SPLIT --> ACK
    ACK --> IPMAP
    ACK --> ENQUEUE
    IPMAP --> POLL
    POLL --> SUCC
    POLL --> FAIL
    POLL --> CONT
    POLL --> EXP
    CONT --> IPMAP
    EXP --> FAIL
```

**Lock order (when both locks needed):** `mu → pendingMu`

**In-flight suppression.** Karpenter re-invokes `Delete()` **every 5 seconds** for the whole life of a node removal. Every operationID the broker has ACKed and not yet driven terminal is held in `inFlight`; re-enqueueing one resolves the caller immediately instead of sending a duplicate batch. Without this, each 5 s reconcile pushed another remove batch at the broker, flooding the edge's 16-slot queue with copies of the same removal. Operations leave the set as soon as they go terminal, so a genuine retry after a FAILED is never suppressed. Enqueue and ACK are atomic per operationID, so a concurrent caller during the ACK never queues a duplicate batch.

**Multi-waiter fan-out.** `pending` maps an operationID to a **slice** of result channels, so every caller waiting on the same operation is resolved together. With a single buffered channel per operation, only the first waiter could consume the result and the rest blocked until their context was cancelled.

### 5.2 Batch Stream Protocol

```mermaid
sequenceDiagram
    participant C as BrokerClient
    participant EB as edge-broker

    Note over C,EB: SendBatch / SendBatchRemove  (30 s deadline, never retried)
    C->>EB: Open BatchStreamOperations stream
    C->>EB: batch_add{batchId, nodes[{operationId, clusterID, projectID, instanceType, nodePool}]}<br/>or batch_remove{... + providerId}
    alt Accepted
        EB-->>C: batch_accepted{batchId}
    else Queue full
        EB-->>C: batch_rejected{batchId, reason}  → ErrBatchRejected
    end
    C->>EB: CloseSend

    Note over C,EB: PollBatchStatus  (every 30 s from the next tick after ACK; no initial delay)
    C->>EB: Open BatchStreamOperations stream
    C->>EB: status_poll{batchId}
    EB-->>C: batch_status{nodeResults[{operationId, state, detail, providerIds}]}
    C->>EB: CloseSend
    Note over C: SUCCEEDED → recorded in `succeeded`<br/>(removes: next Delete() converges)<br/>FAILED → FailureHandler<br/>ACCEPTED/RUNNING → poll again<br/>empty nodeResults → unknown batch (3× → expired)
```

### 5.3 Remove and Cancel Protocol

```mermaid
sequenceDiagram
    participant C as BrokerClient
    participant EB as edge-broker
    participant R as Redis
    participant P as paas-api

    Note over C,EB: Batch remove (Delete of a joined node)
    C->>EB: batch_remove{batchId, nodes[]}
    EB->>R: write {kind:delete, state:ACCEPTED} per op (15 min TTL)<br/>+ batchop index (3 h TTL)
    EB-->>C: batch_accepted{batchId}
    Note over EB: This edge's processor picks up the batch<br/>(per-cluster redis lock across replicas)
    EB->>R: CAS ACCEPTED→RUNNING per op (extends TTL to 2 h)
    EB->>P: One bulk catalog update:<br/>NEGATIVE pool+sku deltas (refused below the pool minimum)<br/>Apply + Publish ← commit point (60 min deadline incl. settle poll)
    EB->>R: SUCCEEDED at publish (no provider IDs; 24 h tombstone)<br/>or "node not retired: …" (nothing written) — FAILED only before the commit

    Note over C,EB: Cancel (Delete of a still-pending NodeClaim whose add is still open)
    C->>EB: cancel_ops{operationIds[]}
    EB->>R: CAS: only ops still ACCEPTED →<br/>FAILED "cancelled by client"
    EB-->>C: cancel_ack{cancelledOperationIds[]}
    Note over C: Synchronous — the ack decides Delete()'s answer;<br/>RUNNING/terminal ops untouched
```

> **Platform limitation:** the worker-node catalog is declarative (a desired count per pool+SKU), so a remove is a count decrement and the PaaS platform decides which physical machine is retired. `provider_id` is carried on `batch_remove` items so a future targeted-removal API can be adopted without a protocol change. Until then `Delete()` compensates on the provider side (no remove for a machine already gone; SUCCEEDED counts once the NodeClaim's own machine has stopped; a remove after which the platform retired another machine is held, never re-sent).

### 5.4 CloudProvider Interface

| Method | Behavior |
|---|---|
| `Create(NodeClaim)` | Resolves `RafayNodeClass` → filters compatible instance types → **stable-sorts by synthetic price and picks the cheapest** (spec order on ties) → enqueues to `NodeBatcher` → blocks until broker ACK → returns `NodeClaim{providerID="rafay://pending/<uid>", capacity, allocatable, labels, annotation karpenter.rafay.io/batch-id}`. `NodeProviderIDController` later patches the real ProviderID. A missing, not-Ready or empty NodeClass is `NodeClassNotReadyError` (Karpenter drops the NodeClaim at once). |
| `Delete(NodeClaim)` | **Converges across repeated calls.** Once the batcher reports the remove op (`<uid>-remove`) SUCCEEDED: a `node not retired` detail converges at once (event `NodeNotRetired`); IDs naming another machine, or an untargeted SUCCEEDED whose Node is still Ready after `RAFAY_REMOVE_SETTLE_WINDOW` (60 min), hold the NodeClaim (event `RemoveRetiredOtherMachine`, annotation `karpenter.rafay.io/remove-retired-other`, never re-sent); otherwise `NodeClaimNotFoundError` as soon as the Node is gone or NotReady — the only signal that releases the Node's termination finalizer. Before the first remove, a NodeClaim whose Node has already vanished, or is Terminating with a stopped kubelet after an external deletion, is reported gone **without** a remove (`NodeRetiredExternally`). A pending ProviderID is resolved from the add's own state (`resolvePendingRemoval`: SUCCEEDED → reserve the joined node and send the remove, untargeted if none; FAILED → NotFound; open → **synchronous** `Cancel`). Otherwise enqueues `EnqueueRemove(UID+"-remove", …)` and returns `nil` at broker ACK; Karpenter requeues and calls `Delete` again in 5 s (the batcher suppresses the duplicate send). |
| `Get(providerID)` | For `rafay://pending/` prefix: returns minimal NodeClaim (provisioning in progress). Otherwise the k8s Node carrying the ID (cache index on `spec.providerID`). **Not** the termination completion signal — see §4.2. |
| `List()` | `ListNodes` unsupported → falls back to listing **every** k8s Node whose `spec.providerID` carries the `rafay://` prefix. **No cluster-ID filter** — see the note below. |
| `GetInstanceTypes(NodePool)` | Returns `RafayNodeClass.spec.instanceTypes` parsed with `resource.ParseQuantity`; each type declares every well-known label (arch `amd64` / os `linux` by default, region `Exists`), `pods: 110`, `nodes: 1`, `nvidia.com/gpu` only when positive, an **overhead** (`RAFAY_VM_MEMORY_OVERHEAD_PERCENT` 7.5 %, `RAFAY_KUBE_RESERVED_CPU` 80m, `RAFAY_KUBE_RESERVED_MEMORY` 255Mi, 100Mi eviction — capacity stays nominal, allocatable drops) and one offering with a **synthetic price = 1.0 × vCPU + 0.125 × GiB memory**. Pools held in `PoolBackoff` report their offerings unavailable. Returns error if the list is empty or quantities are malformed. |
| `IsDrifted(NodeClaim)` | Always `("", nil)` — no **cloud-provider** drift (machine lifecycle belongs to the Rafay platform). Karpenter core's static (`karpenter.sh/nodepool-hash`), requirements and instance-type drift checks still run and precede this hook, so a NodePool template edit marks a live pool's provisioned NodeClaims `Drifted` — but every broker-rendered pool carries a Drifted-scoped `nodes: "0"` budget, so they are never rolled; adopted NodeClaims carry no hash annotation and are durably exempt. |
| `Name()` | `"rafay"` |

`NewCloudProvider` takes both the cached `kubeClient` and the uncached `apiReader`, the shared `PoolBackoff`, plus the cluster/project IDs read from `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` (optional — the broker identifies the cluster from the certificate) and the options `WithEventRecorder` / `WithRemoveSettleWindow`. Those env values are the sole source of Rafay identity on every broker request — a `RafayNodeClass` cannot override them.

> **Why `List()` must not filter by cluster ID.** A real ProviderID is `rafay://<nodepoolname>/<sku_name>/<hostname>` — the first segment is the **node pool**, not a cluster ID (see §7). Comparing it against `RAFAY_CLUSTER_ID` can never match, so `List()` returned an empty slice. That is dangerous, not merely useless: Karpenter's core **garbage-collection controller deletes any `Registered` NodeClaim whose ProviderID is absent from `List()`**, so every managed node was torn down the moment it briefly went `NotReady`.

### 5.5 NodeProviderIDController

**Purpose:** Resolves the real `spec.providerID` for NodeClaims that have a synthetic pending ProviderID, and patches `NodeClaim.Status.ProviderID`.

**Why it exists:** `Create()` returns immediately after broker ACK (typically seconds) to stay within Karpenter's 5-minute `launchTimeout`. Nodes typically take ~12 minutes to join, and up to 60 (the platform builds the whole machine). The controller bridges this gap inside the 60-minute `registrationTimeout` window.

**Reconcile flow:**

```
1. Skip if ProviderID does not start with "rafay://pending/" or NodeClaim is being deleted

2. Extract:
     nodePoolName ← nodeClaim.Labels[karpenter.sh/nodepool]
     skus         ← NodeClaimSKUs(nodeClaim) = {
                       nodeClaim.Labels[node.kubernetes.io/instance-type],  # SELECTED SKU
                       nodeClaim.Spec.NodeClassRef.Name }                   # legacy single-SKU

3. Lock cloudprovider.NodeOwnershipMu; read NodeClaimList from API server directly (apiReader, NOT cache)
     → build usedIDs = { status.providerID of non-pending claims }
                     ∪ { karpenter.rafay.io/adopted-provider-id of every claim }
     (prevents stale-cache double-assignment race; an adopted claim reserves its node)

4. List Nodes with label: nodepoolname=<nodePoolName>
     (sku_name is NOT a server-side selector — a NodeClaim may accept several values)

5. Find first unclaimed node where:
     • node.Labels[sku_name] ∈ skus
     • spec.providerID != "" and not in usedIDs
     • creationTimestamp > nodeClaim.creationTimestamp

6. If not found → Requeue after 30 s

7. Patch Node label karpenter.sh/registered=true (skipped when already set; failure → retry, claim stays pending)

8. Patch NodeClaim.Status.ProviderID = node.Spec.ProviderID  (optimistic lock)
     • Conflict → Requeue: true
     • NotFound → ignore (NodeClaim deleted during disruption)
```

**Key properties:**
- **`sku_name` matches the SELECTED instance type, not just the NodeClass name.** `node.sku_name` is the **platform's** SKU/instance-type name. A `RafayNodeClass` listing several `instanceTypes` — exactly the configuration cheapest-fit selection exists to serve — provisions a node whose `sku_name` is the *selected* type, which does not equal the NodeClass name. Matching on `NodeClassRef.Name` alone therefore **never resolved** such NodeClaims: they churned on the hourly registration timeout, orphaning a machine each cycle. A node now matches if its `sku_name` equals **either** the NodeClaim's `node.kubernetes.io/instance-type` label (stamped by `Create()` from the type it selected — the authoritative match) **or** the NodeClass name (legacy single-SKU convention). The same rule is applied by `CloudProvider.findNodeProviderID`.
- `MaxConcurrentReconciles: 1` — serializes read-decide-patch, preventing two NodeClaims from claiming the same node
- **Secondary watch on Nodes**: when a real Rafay node appears (non-empty `spec.providerID` + Rafay labels), matching pending NodeClaims are immediately enqueued — no waiting for the 30 s requeue timer
- **Stateless across restarts**: controller-runtime re-enqueues all `rafay://pending/` NodeClaims on startup automatically
- **`apiReader` vs `kubeClient`**: `usedIDs` always reads from API server to avoid stale informer cache

---

### 5.6 NodeAdoptionController

**Purpose:** Gives every worker node that already exists in the cluster a NodeClaim, so Karpenter manages the whole pool rather than only the nodes it provisioned itself.

**Why it exists:** a cluster is created with the node count from its catalog (`noOfSku: 3`), and those nodes carry `nodepoolname` / `sku_name` labels but no NodeClaim. Karpenter counts NodeClaims only, so an un-adopted pool reports `status.nodes: 0` — it provisions its full `limits` **on top of** the existing nodes, and `ValidateNodeDisruptable` refuses to consolidate them (*"node isn't managed by karpenter"*).

**`Reconcile` flow** (primary watch NodePool, secondary watch Node, `MaxConcurrentReconciles: 1`):

```
Skip if the NodePool is deleting or its nodeClassRef is not karpenter.rafay.io/RafayNodeClass
Skip if the NodePool is annotated karpenter.rafay.io/auto-scaling=false (inert pool)

lock cloudprovider.NodeOwnershipMu (held to the last Create of the pass)
poolNodes  ← Nodes labelled nodepoolname=<pool>                 (informer cache)
claimList  ← every NodeClaim                                    (apiReader — uncached)
claimedIDs ← { status.providerID } ∪ { karpenter.rafay.io/adopted-provider-id }
strip karpenter.sh/nodepool-hash from adopted claims that acquired one; re-pin hash-version

log the counts: pool nodes, pool nodeclaims, cluster-wide worker nodes

per unclaimed node:
  ├─ skip control-plane / no sku_name / non-rafay:// providerID
  ├─ skip unless the NodeReady condition is True   (wait for it; the Node watch re-triggers)
  ├─ candidateID = spec.providerID or rafay://<pool>/<sku>/<name>; if already claimed:
  │    stamp it back onto a node whose spec.providerID is empty (re-registered kubelet), skip
  ├─ skip if a pending NodeClaim newer than the node could still take it
  ├─ skip if sku_name ∉ RafayNodeClass instanceTypes, or labels fail the pool's requirements
  ├─ patch Node: spec.providerID (only if empty), karpenter.sh/registered=true
  └─ create NodeClaim annotated karpenter.rafay.io/adopted-provider-id=<providerID>
                    and karpenter.sh/nodepool-hash-version=<current> (no nodepool-hash)

requeue after 5m   (Node watch fires on label / providerID / Ready changes only, not heartbeats)
```

**Key properties:**

- **Only Ready nodes are adopted** (`nodeutils.GetCondition(node, NodeReady).Status == True`, the same test `Initialization` uses). A NodeClaim for a NotReady node would be counted towards the pool's limits — while uninitialized, `StateNode.Capacity()` fills any resource the node does not report from the NodeClaim's, so for a node whose kubelet never reported the SKU's declared figures are the whole capacity — and could never reach `Initialized`. Nothing reaps it either: `Liveness` reaps only claims that failed to **register**, and garbage collection only claims absent from `List()`, which this one is not. Waiting is free: the Node watch fires on the status update that flips the condition. A node that goes NotReady *after* adoption keeps its claim. The gate sits above the ownership checks (so a still-booting node gets a quiet deferral instead of label warnings), which is why the `notReady` log counter excludes nodes already claimed.
- **No machine is provisioned.** Karpenter's `Launch` reconciler calls `Create()` for any NodeClaim that is not yet `Launched`; the adoption annotation makes `Create()` return that ProviderID with **no broker call**, so the catalog and `noOfSku` are untouched. The marker is an annotation set at create time, not a status pre-patch — status is a separate subresource, and a create-then-patch sequence leaves a window in which `Launch` would order a second node.
- **Idempotent through the status gap.** An adopted NodeClaim has no status until `Launch` runs, so `claimedIDs` includes the annotation as well as `status.providerID`; NodeClaims are read via `apiReader` because a just-created one is not in the informer cache. Without both, a second pass would adopt the same node twice.
- **Complementary to §5.5.** The `NodeProviderIDController` binds a node to a pending NodeClaim only when the node is *newer* than the claim; adoption therefore skips any node newer than a pending claim for the same pool+SKU. The two halves never contend for the same node.
- **Refuses what Karpenter would replace.** The adoptability check mirrors the drift sub-controller (`areRequirementsDrifted` + `instanceTypeNotFound`), so a mismatched node (arm64 in an amd64 pool, an unknown SKU) is warned about and left unmanaged instead of being adopted and then drained.
- **Does not disturb running workloads.** Pool taints are not copied onto the NodeClaim (`Registration` syncs them onto the node, and `NoExecute` would evict its pods); well-known labels are read from the node, and topology labels are omitted when the node has none, so the SKU's synthetic zone `default` never overwrites real topology (safe only because the adoptability check is as strict about absent keys as drift is). No `karpenter.sh/nodepool-hash` annotation is set — and the exemption is durable: the claim carries the current `nodepool-hash-version`, and every pass strips a back-filled hash — so a later NodePool edit does not mark every adopted node drifted.

---

## 6. Worker Node SKU Catalog

The Worker Node SKU is a JSON array variable (`"Worker Node SKU"`) on the `WorkspaceComputeInstance` spec. Each row describes a pool/SKU combination and its desired node count.

```json
[
  { "poolname": "worker-pool-amd", "skuname": "oci-inst",     "noOfSku": 3 },
  { "poolname": "worker-pool-arm", "skuname": "oci-inst-arm", "noOfSku": 1 }
]
```

| Field | Source | Meaning |
|---|---|---|
| `poolname` | `NodePool.metadata.name` | Identifies which node pool this row applies to |
| `skuname` | `RafayNodeClass.spec.instanceTypes[i].name` | The instance type / SKU to provision |
| `noOfSku` | Managed by edge-broker | Desired number of worker nodes for this pool+SKU |

**Scale-out:** the broker groups each add batch by pool+SKU and applies all increments in **one** Apply + Publish, trimmed to each row's `maxNodeCount` (the overflow is refused `pool at maximum`).
**Scale-in:** remove batches become **negative** deltas in the same bulk pass; a decrement the row cannot absorb (at `minNodeCount`, at 0) is refused and the op converges SUCCEEDED `node not retired: …`.

> **Note:** The broker only ever moves `noOfSku` of rows opted in (`autoScaling: true`); it never creates, renames or re-types a row. A `{poolname, skuname}` row that does not exist refuses its adds (`pool not found`) and removes (`node not retired`); another SKU on the row is `pool sku mismatch`; a row not opted in is `pool not auto-scaling`. Matching is case-insensitive on pool and SKU names.

### RafayNodeClass → Catalog Mapping

```mermaid
graph LR
    subgraph RNC["RafayNodeClass"]
        IT["spec.instanceTypes[i].name\ne.g. oci-inst"]
    end

    subgraph NCL["NodeClaim / NodePool"]
        NP["karpenter.sh/nodepool label\ne.g. worker-pool-amd"]
    end

    subgraph CAT["Worker Node SKU Catalog\nWorkspaceComputeInstance variable"]
        ROW["{ poolname: worker-pool-amd\n  skuname:  oci-inst\n  noOfSku: 3 }"]
        SO["Scale-Out: noOfSku += batch delta\nApply + Publish (one bulk pass)"]
        SI["Scale-In:  noOfSku -= batch delta\n(clamped at 0) Apply + Publish"]
    end

    IT -->|skuname| ROW
    NP -->|poolname| ROW
    ROW --> SO
    ROW --> SI
```

---

## 7. Operation State Machine

```mermaid
stateDiagram-v2
    direction TB

    [*] --> ACCEPTED : batch_add or batch_remove\nnew operationID (or retry after FAILED)

    ACCEPTED --> ACCEPTED : Idempotent replay\nre-enqueue without Redis write\n(restart recovery)

    ACCEPTED --> RUNNING : Batch processor claims op under the per-cluster lock\n(Redis CAS — extends TTL to 2 h)

    ACCEPTED --> FAILED : cancel_ops → "cancelled by client"\nor queue-full race → "batch queue full; try again"\n(CAS — never clobbers RUNNING)

    RUNNING --> SUCCEEDED : add: platform settled, provider IDs recorded\nremove: decrement PUBLISHED (before the settle poll)\nalso "committed; …" past the commit point\nand "node not retired: …" for a refused remove

    RUNNING --> FAILED : platform FAILED before the commit point,\npermanent add refusal (pool at maximum / not found /\nsku mismatch / not auto-scaling / precondition),\nor "…; retry" (shutdown, lock timeout, panic)

    SUCCEEDED --> [*]
    FAILED --> [*]
```

Replay rules when an `operation_id` already exists in Redis: **RUNNING/SUCCEEDED** → skip (a live processor owns it / already done; SUCCEEDED refreshes the tombstone); **ACCEPTED** → re-enqueue without touching Redis; **FAILED** → overwrite with ACCEPTED (client retry); **UNKNOWN** → skip and log, never re-accepted. An ACCEPTED record that expires (15-min TTL; while an op waits in its edge's queue the broker re-arms that TTL every 5 minutes, and a broker restart is covered by the startup sweep, so expiry only follows an unrecovered broker death) is reported on the next poll as FAILED `"operation record expired"`.

**Batch lifecycle timing (client side):**

| Time | Action |
|---|---|
| `t = 0 s` | `SendBatch`/`SendBatchRemove` → recv `batch_accepted{batchId}` → **`resultCh` resolved — `Create()`/`Delete()` unblocks now** |
| `t ≤ 30 s` | First `status_poll{batchId}` on the next 30 s tick (no initial delay) |
| `t = +30 s, +60 s, …` | Subsequent polls every 30 s |
| terminal | `SUCCEEDED` → recorded in `succeeded` + cleared from `inFlight` (**removes: the next `Delete()` acts on this**; adds: registration is driven by the node joining) · `FAILED` → `FailureHandler` (adds: record the op, delete the still-pending NodeClaim) + cleared from `inFlight` so a retry can re-send |
| expiry | 3 consecutive empty polls or batch age > 3 h → batch dropped, remaining items FAILED `"batch expired at broker"` → `FailureHandler` |

---

## 8. NodeClaim Registration Timeline

```mermaid
sequenceDiagram
    participant TL as Timeline

    Note over TL: t = 0 s<br/>CloudProvider.Create() called

    Note over TL: t ≈ 5 s<br/>Broker ACK received<br/>Create() returns NodeClaim{providerID="rafay://pending/<uid>"}<br/>Launched=True written to etcd

    Note over TL: t ≈ 6 s<br/>Karpenter registration reconciler sets Registered=Unknown<br/>60-minute registrationTimeout timer starts

    Note over TL: t ≤ 35 s, then every 30 s<br/>status_poll from the next tick (RUNNING → SUCCEEDED;<br/>FAILED deletes the pending NodeClaim via FailureHandler)

    Note over TL: t ≈ 12 min (typical; up to 60 min)<br/>Node joins cluster<br/>Rafay platform sets node.Spec.ProviderID

    Note over TL: t ≈ 12 min  (triggered by Node watch)<br/>NodeProviderIDController labels the Node registered and patches<br/>NodeClaim.Status.ProviderID = node.Spec.ProviderID

    Note over TL: t ≈ 12 min<br/>Karpenter registration reconciler: Registered=True<br/>48-minute margin remaining before 60-min timeout fires
```

> **Note:** The `registrationTimeout` in `sigs.k8s.io/karpenter` has been patched to **60 minutes** (upstream default is 15 minutes). This is the Rafay fork `github.com/RafaySystems/karpenter-rafay` (branch `rafay-release-v1.14.x`), pinned by a commit pseudo-version through a `replace` directive in `go.mod` — it lives outside this repo. Nodes typically join at ~12 minutes, giving a 48-minute margin, but adding a node can take up to 60 minutes, so the timeout equals that bound with no margin; every other add-path deadline (broker 90 min, RUNNING record 2 h, batch index and client tracking 3 h) sits above it. Broker-side **FAILED** provisions do not wait for this timer — the batcher's `FailureHandler` deletes the pending NodeClaim within one poll interval, so Karpenter reprovisions in seconds.

### Timeout Summary for Node Join

| Timer | Value | Owner | What fires on expiry |
|---|---|---|---|
| `launchTimeout` | 5 min | Karpenter upstream | Delete NodeClaim if not Launched |
| `registrationTimeout` | **60 min** (patched) | Karpenter fork (`karpenter-rafay`) | Delete NodeClaim if not Registered (`Delete()` then sends an untargeted remove) |
| `maxBatchAge` (poller) | 3 h | `pkg/rafay/batcher.go` | Unresolved batch items treated as FAILED → `FailureHandler` |
| Broker add / remove processing deadline | 90 min / 60 min | edge-broker (server-side) | Add: `committed; … settle poll did not complete` if the write landed; remove: SUCCEEDED stays |
| Broker Redis op TTL | 15 min (ACCEPTED) / 2 h (RUNNING, FAILED) / 24 h (SUCCEEDED tombstone) | edge-broker (server-side) | Operation record expires → polls report FAILED (a tombstone must never expire within a client's retry loop) |

---

## 9. Identity and Security

### mTLS Client Certificate

```mermaid
graph LR
    subgraph CERT["mTLS Client Certificate\nedge-client-creds Secret"]
        O["Subject Organization O\ne.g. 7dkgjkx.cluster.example.com\nFirst DNS label = Edge hash ID: 7dkgjkx"]
        OU["Subject Org Unit OU\ne.g. broker.example.com\nBroker dial hostname"]
    end

    subgraph PROVIDER["karpenter-provider-rafay"]
        EID["EdgeID\nGetEdgeIDFromClientCert()"]
        HOST["Broker Host\nGetServerHostFromCert()"]
        SID["sessionid UUID\nSTREAM_ID env or auto-generated"]
    end

    subgraph BROKER["edge-broker"]
        MTLS["mTLS verification"]
        IDENT["common.GetEdgeClientInfo(ctx)\nExtract edge identity from cert O"]
        ROUTE["Stream routing\nby sessionid metadata header"]
    end

    O --> EID
    OU --> HOST
    EID -->|"gRPC connection"| IDENT
    HOST -->|"dial port 5448"| MTLS
    SID -->|"metadata: sessionid=STREAM_ID"| ROUTE
```

Both `karpenter-provider-rafay` and `edge-client` use the **same certificate material** (`client.crt`, `client.key`, `ca.crt`) from the `edge-client-creds` Secret mounted at `/opt/rcloud/certs/`.

### Concurrency Controls

| Layer | Mechanism | Purpose |
|---|---|---|
| **Broker** | Per-edge batch queue — one processor goroutine per edge, one batch at a time within an edge | Catalog mutations on one cluster never overlap within a broker pod; different clusters proceed in parallel |
| **Broker** | Per-cluster Redis lock `/edge/karpenter/lock/<edgeID>` (2 h; stale-holder takeover) | Two broker replicas never overlap on one cluster |
| **Broker** | Redis CAS (`WATCH`/`MULTI`/`EXEC`) on ACCEPTED→RUNNING and cancel transitions | A cancel can never clobber a concurrent claim of the op (and vice versa) |
| **NodeProviderIDController / NodeAdoptionController / `Delete()`** | `MaxConcurrentReconciles: 1` + `apiReader` + `cloudprovider.NodeOwnershipMu` | Prevents two NodeClaims from claiming the same node, across all three binders |
| **NodeBatcher** | `pending` map deduplication by `operationID` | Prevents duplicate batch items for the same NodeClaim (adds and removes alike) |

---

## 10. Provider ID Format

### Synthetic Pending (pre-join)

```
rafay://pending/<nodeclaim-uid>
```

Set by `Create()` immediately after broker ACK. Signals `NodeProviderIDController` that this NodeClaim is awaiting real ProviderID assignment. The `<nodeclaim-uid>` is the Kubernetes UID of the NodeClaim.

### Real Format (post-join, set by Rafay platform)

```
rafay://<nodepoolname>/<sku_name>/<hostname>
```

**Example:** `rafay://worker-pool-amd/oci-inst/payes-test-10-k97psrhs-w1-e6a5c`

| Segment | Source | Meaning |
|---|---|---|
| `nodepoolname` | Rafay platform | The **node pool** — matches the `nodepoolname` node label and the `karpenter.sh/nodepool` NodeClaim label. **This is not a cluster ID.** |
| `sku_name` | Rafay platform | The **platform's SKU / instance-type name** — matches the `sku_name` node label, and on the NodeClaim the `node.kubernetes.io/instance-type` label (the type `Create()` selected) or, under the legacy single-SKU convention, `spec.nodeClassRef.name` |
| `hostname` | Rafay platform | Kubernetes node name |

The Rafay platform sets `spec.providerID` on the node when it joins. The provider **never constructs** this format — it only reads it from `node.Spec.ProviderID`.

> ⚠️ **The first segment is the node pool, NOT a cluster ID.** `ParseProviderID` (`pkg/rafay/providerid.go`) splits a Rafay provider ID into its first path segment (`nodePoolName`) and the remainder (`"<sku_name>/<hostname>"`). It is **not** used to filter nodes by cluster: `listNodesFromKube` used to compare that first segment against `RAFAY_CLUSTER_ID`, which can never match, so `List()` always returned empty — and the core garbage-collection controller then deleted any `Registered` NodeClaim whose node briefly went `NotReady`. The cluster-ID filter is gone (see §5.4). A synthetic pending ID parses structurally with `nodePoolName == "pending"`; callers that must distinguish pending IDs check `PendingProviderIDPrefix` instead.

### ProviderID Lifecycle

```mermaid
graph LR
    A["Create() called"]
    B["rafay://pending/<uid>\nset immediately after broker ACK"]
    C["Node joins cluster\n~12 minutes later"]
    D["rafay://pool/sku/hostname\nset by Rafay platform on node.Spec.ProviderID"]
    E["NodeProviderIDController patches\nNodeClaim.Status.ProviderID = real ID"]
    F["Karpenter: Registered=True"]

    A --> B --> C --> D --> E --> F
```

---

## 11. Idempotency and Resilience

### Operation ID

Every operation is keyed by the NodeClaim UID: adds use `NodeClaim.UID` directly, removes use `NodeClaim.UID + "-remove"`. The UID is:
- **Stable across provider restarts** — Karpenter reuses the same NodeClaim object and UID on retry after a crash
- **Unique per desired node** — prevents double-provisioning (or double-decrementing) for the same logical node

When the broker receives a batch item with an `operation_id` that already exists in Redis:
- **RUNNING / SUCCEEDED:** skip — a live processor owns it or it already finished
- **ACCEPTED:** re-enqueue without touching Redis (restart recovery; harmless because the processor only claims ops via the ACCEPTED→RUNNING CAS)
- **FAILED:** retry path — record rewritten to ACCEPTED and re-processed

### Crash Safety Matrix

| State at pod restart | `Launched` in etcd | Broker knows? | Recovery path |
|---|---|---|---|
| Item in queue, not sent | `Unknown` | No | Karpenter re-calls `Create()` → fresh first request |
| Batch sent, awaiting ACK | `Unknown` | Maybe | Karpenter re-calls `Create()` → broker idempotency by operationID |
| Broker ACK'd, `Launched=True` set | `True` | Yes | `NodeProviderIDController` resumes from etcd; `batchresume` re-registers the batch from `karpenter.rafay.io/batch-id` so FAILED feedback still arrives; broker processes the batch independently |
| Real ProviderID patched | `True` | Yes | Fully stable — no action needed |

Removes recover the same way: Karpenter retries `Delete()` as long as the node object exists, and the `-remove` operationID dedups at both the batcher and the broker (24 h tombstone); the settle window restarts and a retired-other verdict is read back from the NodeClaim annotation.

### Retry Policy per RPC

- `SendBatch` / `SendBatchRemove` use `callBrokerOnce` — **never retried**, and the batch is not re-issued (retrying risks duplicating the catalog mutation). The shared connection is **never closed** on a failure: it is shared by the sender, the poller, cancels and the config sync, and closing it aborted every other goroutine's in-flight stream with `Canceled`; gRPC reconnects by itself. Recovery is via application-level paths: Karpenter re-calls `Create()`/`Delete()` with the same UID → broker idempotency.
- `PollBatchStatus` / `CancelOperations` / `GetKarpenterConfig` use `callBroker` — `ResetConnectBackoff()` and **retried once** on `Unavailable`, on the same connection. All are safe to repeat: the poll and config fetch are read-only and the cancel is CAS-guarded.

### Synchronous Cancellation

Deleting a NodeClaim whose add is still open at the broker calls `Cancel(ctx, UID)`: a synchronous `cancel_ops` (10 s timeout). Only operations **still ACCEPTED** at the broker are transitioned to FAILED `"cancelled by client"`; cancelled → `Delete()` finalizes the NodeClaim, not cancellable → the NodeClaim is kept until the poller reports the add's outcome, so a machine that lands has an owner (a SUCCEEDED add then gets an untargeted remove). A node that joined with no NodeClaim waiting for it (a lost fire-and-forget cancel from an older provider) is reclaimed only by the node-adoption controller — once Ready, and then only when empty under `WhenEmpty`; with adoption disabled it is an unowned machine that must be removed from the catalog by hand.

---

## 12. Timeouts Reference

| Layer | Constant | Value | Location |
|---|---|---|---|
| Batch: initial wait before first poll | — | none (next 30 s tick after ACK) | `pkg/rafay/batcher.go` |
| Batch: interval between status polls | `defaultPollInterval` | 30 s | `pkg/rafay/batcher.go` |
| Batch: collection window (starts at first item) | `defaultBatchWindow` | 10 s | `pkg/rafay/batcher.go` |
| Batch: max items per batch | `defaultMaxBatchSize` | 10 | `pkg/rafay/batcher.go` |
| Batch: max tracked age of a sent batch | `maxBatchAge` | 3 h (never shorten — a node add can take 60 min) | `pkg/rafay/batcher.go` |
| Batch: empty polls before declaring batch unknown | `maxConsecutiveEmptyPolls` | 3 | `pkg/rafay/batcher.go` |
| Batch: how long a SUCCEEDED operationID is remembered (drives `Delete()` convergence) | `succeededRetention` | 2 h | `pkg/rafay/batcher.go` |
| Cancel: broker cancel deadline | `cancelTimeout` | 10 s | `pkg/rafay/batcher.go` |
| Pool hold after a permanent add refusal | `DefaultPoolAtMaxCooldown` | 5 min (`RAFAY_POOL_AT_MAX_COOLDOWN`) | `pkg/cloudprovider/poolbackoff.go` |
| Failure handler: per-op FAILED record retention | `failedOpRetention` | 2 h | `pkg/cloudprovider/poolbackoff.go` |
| `Delete()`: settle window for an untargeted SUCCEEDED remove | `DefaultRemoveSettleWindow` | 60 min (`RAFAY_REMOVE_SETTLE_WINDOW`, floor) | `pkg/cloudprovider/cloudprovider.go` |
| Config: fetch deadline / error backoff / resync | `karpenterConfigCallTimeout` / `initialRetryDelay`→`maxRetryDelay` / `defaultSyncInterval` | 90 s / 5 s → 2 min / 10 min | `pkg/rafay/brokerclient.go`, `pkg/controllers/nodeconfig/controller.go` |
| Karpenter: `Delete()` retry interval while a node is terminating | `awaitInstanceTermination` requeue | 5 s | Karpenter core `pkg/controllers/node/termination/controller.go` |
| Per-RPC broker deadline (send/poll/cancel) | `brokerCallTimeout` | 30 s | `pkg/rafay/brokerclient.go` |
| NodeProviderIDController: requeue when no node | `nodeWaitRequeueTime` | 30 s | `pkg/controllers/nodeproviderid/controller.go` |
| Headroom: safety resync | `resyncInterval` | 5 min | `pkg/controllers/headroom/controller.go` |
| NodeClaim launch timeout | `launchTimeout` | 5 min | Karpenter upstream (fork) |
| NodeClaim registration timeout | `registrationTimeout` | **60 min** (patched from 15 min) | Karpenter fork `pkg/controllers/nodeclaim/lifecycle/liveness.go` |
| gRPC keepalive ping | `ClientParameters.Time` | 5 min | `pkg/broker/conn.go` |
| gRPC keepalive timeout | `ClientParameters.Timeout` | 30 s | `pkg/broker/conn.go` |
| gRPC keepalive without streams | `PermitWithoutStream` | false | `pkg/broker/conn.go` |
| gRPC max message size | — | 20 MB | `pkg/broker/conn.go` |
| Broker: Redis op TTL while ACCEPTED | `karpenterBatchAcceptedTTL` | 15 min | edge-broker `karpenter_batch_stream.go` |
| Broker: Redis op TTL once RUNNING / terminal FAILED | `karpenterBatchRunningTTL` | 2 h (add timeout + 30 min; 15 min for the queue-full / shutdown / lock-timeout rejections) | edge-broker `karpenter_batch_stream.go` |
| Broker: SUCCEEDED tombstone TTL (refreshed on re-send) | `karpenterBatchSucceededTTL` | **24 h** | edge-broker `karpenter_batch_stream.go` |
| Broker: batchID→opIDs index TTL | `karpenterBatchOpTTL` | 3 h | edge-broker `karpenter_batch_stream.go` |
| Broker: add / remove processing deadline (settle poll included) | `karpenterBatchAddTimeout` / `karpenterBatchRemoveTimeout` | 90 min / 60 min | edge-broker `processBatchAdd` / `processBatchRemove` |
| Broker: per-cluster lock TTL / wait / retry | `karpenterClusterLockTTL` / `karpenterClusterLockWait` / `karpenterClusterLockRetryInterval` | 2 h / 90 min / 15 s | edge-broker `karpenter_batch_stream.go` |
| Broker: waiting batches per edge / nodes per batch / idle processor reap | `karpenterBatchQueuePerEdgeMax` / `karpenterBatchMaxNodes` / `karpenterBatchEdgeIdleReap` | 16 / 64 / 1 h | edge-broker `karpenter_batch_stream.go` |
| Broker: platform settle poll interval | `oneclickPollInterval` / `firstClassPollInterval` | 30 s | edge-broker `karpenter_backend_oneclick.go` / `karpenter_backend_firstclass.go` |

---

## 13. Configuration Reference

### TLS / Connectivity

| Variable | Default | Description |
|---|---|---|
| `CERT_FOLDER` / `EDGE_CLIENT_CERT_FOLDER` | — | Directory with `client.crt`, `client.key`, `ca.crt` for mTLS |
| `SERVER_PORT` / `EDGE_CLIENT_SERVER_PORT` | `5448` | TLS gRPC port to edge-broker |
| `EDGE_BROKER_GRPC_INSECURE` | `false` | `true` = plaintext gRPC. ⚠️ **Not usable for node operations** — the broker derives the edge id from the mTLS client certificate, so `BatchStreamOperations` on the plaintext listener is rejected with `ErrorNoClientID` (`"NO CLIENT ID"`). Keep `false`. |
| `EDGE_BROKER_GRPC_PORT` | `5449` | Port when `EDGE_BROKER_GRPC_INSECURE=true` (see the caveat above); `BrokerClient` falls back to 5449 when unset |
| `EDGE_BROKER_GRPC_HOST` | (from cert OU) | Override broker dial host (required when insecure + no cert) |

Invalid (non-numeric) port values and invalid durations log a warning and fall back to the default instead of being silently ignored.

### Identity / Routing

| Variable | Default | Description |
|---|---|---|
| `EDGE_ID` | (from cert Subject O) | Logging label; warns if it disagrees with the cert |
| `STREAM_ID` | (auto UUID v4) | gRPC `sessionid` metadata header — auto-generated if unset |
| `RAFAY_CLUSTER_ID` | — | Rafay cluster ID stamped on every broker add/remove payload, for diagnostics. Optional — the broker identifies the cluster from the mTLS certificate. Sole source — no per-NodeClass override exists. |
| `RAFAY_PROJECT_ID` | — | Rafay project ID stamped on the same payloads. Optional — the broker derives the project itself and never widens its lookup by this hint. Sole source. |
| `RAFAY_POOL_AT_MAX_COOLDOWN` | `5m` | Hold on a NodePool after a permanent add refusal (`0` disables) |
| `RAFAY_REMOVE_SETTLE_WINDOW` | `60m` (floor) | How long `Delete()` waits for the machine behind an untargeted SUCCEEDED remove to stop before concluding the platform retired another one |
| `RAFAY_VM_MEMORY_OVERHEAD_PERCENT` / `RAFAY_KUBE_RESERVED_CPU` / `RAFAY_KUBE_RESERVED_MEMORY` | `7.5` / `80m` / `255Mi` | Instance-type overhead model (plus the kubelet's 100Mi eviction threshold); capacity stays nominal, allocatable drops |

### Controller Behavior

| Variable | Default | Description |
|---|---|---|
| `LEADER_ELECTION_NAMESPACE` | `""` in the fork; deployment sets `karpenter` | Namespace for the leader-election Lease |
| `LEADER_ELECTION_NAME` | `karpenter-leader-election` | Lease name; the `karpenter-provider-rafay-leader-election` Role restricts update/patch to it |
| `DISABLE_LEADER_ELECTION` | `false` | Disable leader election (local dev; no `KARPENTER_` prefix) |
| `MEMORY_LIMIT` | (set by the Deployment) | Karpenter sets the Go GC soft limit to 90 % of it |
| `HEADROOM_NAMESPACE` | `karpenter` | Namespace where headroom Deployments (pause pods) are created |
| `HEADROOM_CONFIG_NAMESPACE` | = `HEADROOM_NAMESPACE` | Namespace of the `headroom-policy` ConfigMap |
| `KARPENTER_CONFIG_BOOTSTRAP` / `KARPENTER_CONFIG_SYNC_INTERVAL` | `true` / `10m` | Fetch and re-apply the broker-rendered manifests every interval |
| `KARPENTER_ADOPT_EXISTING_NODES` | `true` | Adopt the pool's pre-existing worker nodes |

> **Why `karpenter` namespace for leader election?** Rafay's platform webhook blocks Lease writes in `rafay-system`. The controller uses `karpenter` namespace to avoid this restriction. The same namespace hosts the headroom pause-pod Deployments and their ConfigMap.

### Headroom Policy ConfigMap

The headroom controller reads a `headroom-policy` ConfigMap (data key `policy`, see `examples/policy_configmap.yaml`). Per pool: buffer fractions `cpu`/`memory`/`gpu` (**0–50 %** — the buffer is a fraction of *total* pool allocatable, so a larger fraction cannot converge once DaemonSet overhead is counted), per-pod slice sizes `podCPU`/`podMemory`/`podGPU` (defaults `500m`/`512Mi`; GPU buffers require an explicit whole-number `podGPU`), a `minPods` cold-start floor, an optional `maxPods` ceiling, and optional `tolerations` (**no blanket `Exists` toleration is added**). Replicas = max over configured resources of `ceil(fraction × Σ ready-node allocatable / per-pod size)`, floored at `minPods`, capped at `maxPods`, and **clamped at 5000** (`maxHeadroomReplicas`) so a typo in a per-pod size cannot flood the cluster or overflow the `int32` replica count. Per-pod sizes below `1m` CPU / `1Mi` memory, duplicate or non-DNS-1123 pool names, and malformed tolerations are rejected at parse time. Pause pods **pack** onto as few nodes as possible (a preferred `podAffinity` on `kubernetes.io/hostname`, no topology spread — a spread kept one pause pod per node forever and blocked `WhenEmpty` scale-in) and run under a restricted-PSS-compliant security context. A pool whose NodePool is missing, annotated `karpenter.rafay.io/auto-scaling: "false"` or has `limits.nodes: 0` is **inert**: no Deployment is created and an existing one is held at 0 replicas.

Robustness properties worth knowing:

- **A ConfigMap parse error keeps the buffer up.** A YAML typo makes `Reconcile` return an error *before* any garbage collection runs. It must never read as "no pools configured": falling through with an empty pool list would tear down **every** headroom Deployment in the cluster over one bad character. A ConfigMap that parses cleanly and declares *no* pools is a legitimate "headroom off" policy and does remove the Deployments. Unrelated keys holding a scalar or list, and a malformed fallback key next to a cleanly parsed well-known key, are skipped rather than failing the policy.
- **Data keys are scanned deterministically** (`policy`, then `config`, then all other keys sorted by name). Go map iteration is randomized, so an unsorted scan would let the applied policy flap between reconciles when two keys both declare pools.
- **Steady-state reconciles issue no Deployment Updates.** The reconciler mutates only the fields it owns, in place on the fetched pod template, so it does not clobber API-server defaults and then "differ" from the stored object on every pass.
- **The `rafay-headroom` PriorityClass is ensured on every Reconcile**, not just at startup (without it the pause pods are rejected at admission, silently disabling headroom): a cached `Get`, a `Create` only on NotFound. Its `Value` is immutable and the controller holds no update verb, so a pre-existing class with a different value or `preemptionPolicy` is reported with a **loud warning every reconcile** rather than passing silently — a non-negative value makes headroom pods un-preemptable, and `preemptionPolicy != Never` lets them evict real workloads. Fixing it means deleting the class.
- **The ConfigMap is watched through a cache scoped to that one object** (namespace + `metadata.name=headroom-policy` field selector), not the manager's cluster-wide ConfigMap informer; the policy is read uncached, one GET per sync. NodePools are watched too, so an inert/live flip is picked up before the 5-minute resync.

> ⚠️ **GPU headroom reserves capacity on the pool's EXISTING GPU nodes only — it cannot cold-start a GPU pool or drive scale-out, by design.** Instance types of SKUs whose ComputeProfile declares a positive `gpu_count` do advertise a temporary `nvidia.com/gpu` capacity (see `docs/architecture.md`, RafayNodeClass reference), so Karpenter *can* provision for a pending GPU pod — but the headroom controller keys off existing nodes. The GPU resource name is derived from the GPU nodes the pool already has; on a pool with no GPU nodes the `podGPU` request is **dropped with a warning** rather than guessed (a guess would only strand the pause pod in `Pending` forever, and would be wrong outright on an `amd.com/gpu` pool). Use `minPods` with a cpu/memory-sized pod to cold-start a GPU pool; the GPU buffer applies once nodes exist.

> **Required:** NodePools used with headroom must set `disruption.consolidationPolicy: WhenEmpty`. Pause pods keep buffer nodes non-empty, so `WhenEmptyOrUnderutilized` would consolidate them away and the replacement pods would scale them back up — an endless oscillation. The examples set `WhenEmpty` + `consolidateAfter: 5m`.

### Kubernetes Secrets

| Secret | Namespace | Contents |
|---|---|---|
| `edge-client-creds` | `rafay-system` | `client.crt`, `client.key`, `ca.crt` — mounted at `/opt/rcloud/certs/` |
| `karpenter-provider-rafay` | `rafay-system` | Optional `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` via `envFrom` |

RBAC (see `config/deploy/rbac.yaml`) is scoped to what the code performs — no Secrets or ServiceAccounts, lease reads only in the ClusterRole plus a namespaced `karpenter-provider-rafay-leader-election` Role for the election Lease — and grants full write on `apps/deployments`, get/list/watch/create on `scheduling.k8s.io/priorityclasses` and get/list/watch on `karpenter.sh/nodepools` for the headroom controller.

### edge-broker (Control Plane)

edge-broker reads its configuration through viper with `SetEnvPrefix("EDGE_BROKER")`; every key is an `EDGE_BROKER_`-prefixed variable, and only the four `*_SERVICE_ADDR` keys are also read under their bare name.

| Variable | Default | Description |
|---|---|---|
| `EDGE_BROKER_SERVER_PORT` | `5448` | mTLS listener port |
| `EDGE_BROKER_INTERNAL_PORT` | `5449` | Plaintext internal listener (`EdgeBrokerService`) |
| `EDGE_BROKER_CERT_FOLDER` | `/etc/rcloud/certs` | TLS certificate directory |
| `EDGE_BROKER_REDIS_ADDR` | `admin-redis:6379` | Redis address |
| `EDGE_BROKER_REDIS_DB` | `3` | Redis database index |
| `EDGE_BROKER_EDGE_HOST` | `edgesrv.rcloud-admin.svc.cluster.local` | Edge service address |
| `EDGE_BROKER_EDGE_PORT` | `50701` | Edge service port |
| `EDGE_BROKER_INFRA_API_SERVER_ADDR` | `infra-apiserver:7000` | Infra API server (v2 cluster resolution) |
| `WORKSPACE_SERVICE_ADDR` (or `EDGE_BROKER_…`) | `paas-api:6000` | PaaS workspace RPC service address |
| `COMPUTE_PROFILE_SERVICE_ADDR` (or `EDGE_BROKER_…`) | `paas-api:6001` | PaaS compute-profile RPC (node SKUs for `GetKarpenterConfig`) |
| `MKS_SERVICE_ADDR` / `DEV_STORAGE_SERVICE_ADDR` (or `EDGE_BROKER_…`) | `paas-api:6011` / `paas-api:6010` | First-class MKS cluster / profile RPCs |
| `KARPENTER_NODEPOOL_CPU_LIMIT` / `_MEMORY_LIMIT` / `_GPU_LIMIT` | `1000` / `1000Gi` / `16` | Capacity limits rendered only on live pools **without** a catalog `maxNodeCount`; raised to one node of the SKU when lower |
| `EDGE_BROKER_PAAS_SERVICE_USERMETA_ID` | (empty) | Service identity for PaaS calls |
| `EDGE_BROKER_PAAS_SERVICE_USERNAME` | (empty) | Username for PaaS calls |

### RafayNodeClass CRD (Sample)

```yaml
apiVersion: karpenter.rafay.io/v1alpha1
kind: RafayNodeClass
metadata:
  name: oci-inst
spec:
  instanceTypes:
    - name: oci-inst         # must match "skuname" in Worker Node SKU catalog
      cpu: "8"
      memory: "16Gi"
      architectures: ["amd64"]
      operatingSystems: ["linux"]
```

The readiness controller validates every entry (non-empty name, parseable `cpu`/`memory`); a bad spec yields `Ready=False` with reason `ValidationFailed` instead of failing at `Create()` time.

---

## 14. Key Dependencies

| Module | Version | Role |
|---|---|---|
| `sigs.k8s.io/karpenter` | v1.14.1 (replace → Rafay fork `github.com/RafaySystems/karpenter-rafay`, branch `rafay-release-v1.14.x`, pinned by commit pseudo-version) | Core framework: operator, controllers, `CloudProvider` interface. Forked to patch `registrationTimeout` to 60 min. |
| `sigs.k8s.io/controller-runtime` | v0.23.3 | Reconciler infrastructure, Manager, typed client |
| `github.com/awslabs/operatorpkg` | (see go.mod) | Operator lifecycle, `status.Condition`, `controller.Controller` |
| `github.com/RafaySystems/edge-common` | pinned commit pseudo-version in `go.mod` (same commit as `edge-broker/go.mod`; no `replace`) | `rep.edge.v1` Karpenter batch + config protocol (add / remove / status / cancel; `GetKarpenterConfig`) and generated Go |
| `google.golang.org/grpc` | v1.72.2 | gRPC client (`grpc.NewClient`, lazy connect) |
| `k8s.io/client-go` | v0.35.1 | Kubernetes client |
| `k8s.io/api`, `k8s.io/apimachinery` | v0.35.1 | Aligned with client-go / Karpenter |
| `github.com/samber/lo` | v1.52.0 | Functional helpers (`Filter`, `Map`, `Assign`) |
| `github.com/google/uuid` | v1.6.0 | UUID v4 for auto-generated `STREAM_ID` and batch IDs |

---

## 15. Design Decisions

### One batch protocol for adds and removes

Node-add requests are batched because multiple pods may become unschedulable simultaneously and Karpenter issues concurrent `Create()` calls. Removes ride the same `NodeBatcher` and the same `BatchStreamOperations` stream (as `batch_remove` frames in their own batches), so both directions share collection, deduplication, idempotency, status polling and failure handling. The former per-operation `KarpenterNodeService.StreamOperations` delete path has been removed.

### ACK-unblock for both Create() and Delete()

`Create()` blocks only until the broker acknowledges the batch (typically seconds), then returns `rafay://pending/<uid>`. This satisfies Karpenter's 5-minute `launchTimeout` without waiting for the node to actually join (~12 minutes typical, up to 60); the `NodeProviderIDController` resolves the real `spec.providerID` within the 60-minute `registrationTimeout` window. No stream is held open for the duration of the work.

`Delete()` likewise returns at broker ACK — but it does **not** stop there, because Karpenter's termination controller re-invokes it on every reconcile and releases the Node's finalizer **only** on a `NodeClaimNotFoundError`. Actual departure is tracked by the status poller: it records the operationIDs the broker reports SUCCEEDED, and the next `Delete()` (≤5 s later) returns `NodeClaimNotFoundError` once the NodeClaim's own Node is gone or NotReady. The 5 s retry loop is cheap because the batcher suppresses duplicate sends for in-flight operations.

Node existence is deliberately **not** the completion signal: during termination Karpenter holds the Node object alive with its own finalizer, which it drops only once `Delete()` reports the instance gone — so polling for the Node's disappearance is circular and never converges. Node readiness is a valid signal for the opposite question (the finalizer keeps the object, not the heartbeat), which is how `Delete()` tells the platform retired this machine rather than another.

### Removal is a catalog count decrement

The PaaS worker-node catalog is declarative (desired count per pool+SKU), so the broker applies removes as negative deltas (refused below the pool's minimum) and **the platform chooses which physical machine is retired**. This is a platform limitation; `provider_id` is carried on every `batch_remove` item so a targeted-removal API can be adopted later without a protocol change. Until then the provider compensates: no remove for a machine that is already gone (the GC-cascade guard and `NodeRetiredExternally`), a SUCCEEDED remove counts as done only once the NodeClaim's own machine has stopped, and a remove after which the platform demonstrably retired another machine is held with `RemoveRetiredOtherMachine` rather than re-sent.

### Synchronous cancel instead of a compensating remove

Deleting a NodeClaim whose node never joined used to have no safe remedy. Now the provider consults the add's own state: a FAILED add needs nothing, a SUCCEEDED add gets an (untargeted) remove, and an add still open is answered by a synchronous `cancel_ops` — only an op still queued (ACCEPTED) at the broker is cancelled (CAS-guarded), so a provision that is already running is never half-undone; the NodeClaim is kept until the add settles so the machine that lands has an owner. A node that joined with no NodeClaim at all (a lost fire-and-forget cancel from an older provider) is reclaimed only by node adoption, once Ready and then only when empty; with adoption disabled it must be removed from the catalog by hand.

### FAILED provisions delete the pending NodeClaim

The status poller hands terminal FAILED results to a `FailureHandler`. For adds, the wired handler records the operation in `PoolBackoff` and deletes the NodeClaim whose UID matches the operationID — only if it still carries a pending ProviderID and is not already being deleted — so Karpenter reprovisions within seconds instead of waiting out the 60-minute registration timeout. Remove failures are log-only: the operationID is cleared from the in-flight set, and Karpenter's ongoing `Delete()` retries re-send the removal (the broker rewrites the FAILED record to ACCEPTED).

Permanent refusals are deliberately not retried at once. When the broker's detail starts `pool at maximum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling` or `pool precondition` — it refused the node for a reason no retry fixes — the handler first marks the NodePool in a shared `PoolBackoff`; `GetInstanceTypes` then reports the pool's offerings unavailable for the cooldown (`RAFAY_POOL_AT_MAX_COOLDOWN`, default 5 min), so the scheduler leaves the pods pending rather than creating a NodeClaim the broker would refuse again; the hold is recorded as a `PoolAtPlatformMaximum` (maximum) or `PoolRefusedByPlatform` (other refusals) Warning event on the NodePool. Every instance type also advertises `nodes: 1` in its capacity, so `NodePool.spec.limits.nodes` holds within a scheduling round and Karpenter does not over-ask in the first place; the broker, for its part, applies the part of a batch that fits and refuses only the overflow. Details in `docs/architecture.md`, "Pool maximum: three layers".

SUCCEEDED results are **recorded**, not merely logged. For adds that is incidental — registration is driven by the node joining. For **removes** it is the convergence signal: it is what lets `Delete()` return `NodeClaimNotFoundError` and release the Node's termination finalizer.

### Synthetic pricing gives consolidation a gradient

The private cloud has no price list, so each instance type's single offering carries a synthetic price (1.0 per vCPU + 0.125 per GiB of memory). `Create()` picks the cheapest compatible type (stable sort — NodeClass spec order breaks ties) and Karpenter's consolidation can prefer replacing underutilized nodes with smaller ones.

### ProviderID resolved from Kubernetes Node, not broker response

Rather than relying on the broker to return a provider ID, the provider reads `node.Spec.ProviderID` directly from the Kubernetes node once it joins. This decouples the provider from the broker's response format and uses the node's own identity as the canonical identifier.

### NodeProviderIDController: single-threaded with uncached reads

`MaxConcurrentReconciles: 1` serializes the read-decide-patch cycle for node assignment. `apiReader` (direct API server reads, bypassing informer cache) ensures a just-patched ProviderID is visible to the next reconcile, preventing two pending NodeClaims from claiming the same node. `Delete()`'s `findNodeProviderID` uses the same uncached reader for the same reason.

### NodeClaims as sole source of truth

No in-memory state is load-bearing across a pod restart. NodeClaims in etcd are the sole source of truth. `NodeProviderIDController` recovers on startup by re-processing all existing `rafay://pending/` NodeClaims. Broker idempotency (per operationID) ensures no duplicate provisioning or removal.

### No retry on batch sends; bounded everything else

`SendBatch`/`SendBatchRemove` use `callBrokerOnce` — retrying a send that partially executed (catalog already mutated but response not received) would double-provision. `PollBatchStatus`/`CancelOperations`/`GetKarpenterConfig` are safe to retry once, on the same connection (the shared connection is never closed by an RPC failure). Every batch RPC carries a 30 s deadline, in-progress batches are dropped after 3 h or 3 consecutive empty status polls (items handed to the failure handler), and a broker-side queue-full condition is reported in-band (`batch_rejected`) without killing the stream.

### Headroom as preemptible Deployments

Proactive scale-out uses the proven cluster-overprovisioner pattern: one Deployment of negative-priority pause pods per pool, sized from the policy ConfigMap, packed onto as few nodes as possible (preferred pod affinity) so `WhenEmpty` can still reclaim buffer nodes. Real workloads preempt the pause pods instantly; the ReplicaSet's pending replacements make Karpenter provision replacement capacity. This requires `consolidationPolicy: WhenEmpty` on participating NodePools (see [§13](#13-configuration-reference)).

### No broker query API

`GetNode` and `ListNodes` return sentinel errors; the provider falls back to listing Kubernetes `Node` objects filtered by `spec.providerID` prefix `rafay://`. The Kubernetes API is the ground truth for what is currently running.

### Parity with `edge-client`

TLS certificate path, broker host extraction, port environment variable names, keepalive parameters (5-min ping / 30 s timeout, no pings without active streams), max message size (20 MB), and `sessionid` UUID generation all mirror `edge-client` so the same certificate material and deployment patterns apply without additional configuration.
