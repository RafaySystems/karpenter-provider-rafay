# Specification: Karpenter-based autoscaling (Rafay / edge-broker)

This document describes **end-to-end** behavior for node autoscaling using **Karpenter** with **`karpenter-provider-rafay`** (cluster-side) and **`edge-broker`** (server-side). Upstream Karpenter semantics (NodePool, NodeClaim, disruption, registration) apply unless overridden below. The narrative and failure analysis live in [docs/architecture.md](docs/architecture.md) and [docs/crash_safety.md](docs/crash_safety.md); the broker's own reference is `edge-broker/docs/karpenter-autoscaling-api.md`.

**Repositories**

| Role | Path |
|------|------|
| Cloud provider + operator binary | `karpenter-provider-rafay` (this repo) |
| gRPC `KarpenterBatchService` / `KarpenterConfigService` implementation | `edge-broker` (`pkg/context/karpenter_batch_stream.go`, `karpenter_batch_recovery.go`, `karpenter_backend.go`, `karpenter_backend_oneclick.go`, `karpenter_backend_firstclass.go`, `karpenter_config.go`, `karpenter_config_render.go`, `karpenter_workspace.go`) |
| Hand-written wire types / generated client | `edge-common` (`pkg/edge/v1/edge_karpenter_batch.go`, `edge_karpenter_config.go`, package `rep.edge.v1`) |

---

## 1. Features and functionality

### 1.1 Cluster side (`karpenter-provider-rafay`)

- **Karpenter v1.14.1 integration** (the Rafay fork `github.com/RafaySystems/karpenter-rafay`, branch `rafay-release-v1.14.x`, which raises the NodeClaim `registrationTimeout` to **60 minutes**): registers the upstream operator, core controllers (provisioning, disruption, consolidation, …), `Cluster` state and `InstanceTypeStore`; supplies a **Rafay** `CloudProvider` implementation (`Name() == "rafay"`).
- **`RafayNodeClass`**: custom `NodeClass` carrying a catalog of **instance types** (name, cpu, memory, optional `nvidia.com/gpu`, zone, architectures, operating systems) and nothing else — no Rafay cluster/project identity. `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` on the controller Deployment are optional labels for broker payloads; the broker identifies the cluster from the mTLS client certificate. A small **readiness** controller sets `Ready=True` (or `Ready=False`/`ValidationFailed`) so NodePools can reference the class. Normally the class and its NodePools are **rendered by edge-broker** and applied by the `nodeconfig` controller (`KARPENTER_CONFIG_BOOTSTRAP`, default `true`).
- **Scale-out (`Create`)**: resolves the `RafayNodeClass`, picks the **cheapest** compatible instance type (synthetic price), enqueues a node add on the `NodeBatcher` with **`operation_id` = `NodeClaim.UID`**, and returns as soon as the broker **ACKs the batch** with **`status.providerID = "rafay://pending/<uid>"`**, capacity, allocatable and labels. The real ProviderID is patched later by the `NodeProviderIDController` from the joined Node (see §3.2). A missing / not-Ready / empty NodeClass is reported as `NodeClassNotReadyError` so Karpenter drops the NodeClaim at once.
- **Scale-in (`Delete`)**: enqueues a batch remove with **`operation_id` = `NodeClaim.UID + "-remove"`** and returns `nil` at broker ACK; Karpenter re-invokes it every 5 s and it **converges** on `NodeClaimNotFoundError` once the broker reports the remove SUCCEEDED (and, for an untargeted remove, once the NodeClaim's Node is gone or NotReady — see §4).
- **No broker Get/List**: `Get` / `List` fall back to Kubernetes **`Node`** objects matched on **`spec.providerID`** (`Get`: exact string through the `spec.providerID` field index; `List`: **every** Node whose ID has the prefix **`rafay://`** — there is deliberately **no cluster-ID filter**, see §3.1).
- **No cloud-provider drift**: `IsDrifted` is always empty. Karpenter core's own static/requirements/instance-type drift still runs, but every broker-rendered live pool carries a Drifted-scoped `nodes: "0"` disruption budget, so drift is observed and never rolled.
- **Transport**: one shared **gRPC** connection over **mTLS** (client certificate = edge identity); each broker call opens **one short-lived `BatchStreamOperations` stream** (30 s deadline). Metadata **`sessionid`** = `STREAM_ID` (auto-generated UUID v4 when unset).
- **Helper controllers**: `nodeproviderid` (real ProviderID), `rafaynodeclass` (readiness), `headroom` (pause-pod buffer), `nodeconfig` (catalog → manifests), `nodeadoption` (NodeClaims for pre-existing worker nodes), `batchresume` (re-polls add batches after a restart).

### 1.2 Broker side (`edge-broker`)

- **`KarpenterBatchService.BatchStreamOperations`**: bidi stream. The client sends one of `batch_add`, `batch_remove`, `status_poll`, `cancel_ops` and receives `batch_accepted`, `batch_rejected`, `batch_status` or `cancel_ack`. The edge id comes from the mTLS peer certificate (`common.GetEdgeClientInfo`); a stream without one is rejected with **`ErrorNoClientID`** (`"NO CLIENT ID"`), so the service cannot be used over the plaintext internal listener.
- **`KarpenterConfigService.GetKarpenterConfig`**: unary; renders the cluster's worker-node catalog as `RafayNodeClass` / `NodePool` YAML. `codes.NotFound` **only** when the cluster has no autoscaling source, and then the message contains `ErrKarpenterConfigUnavailable` (`"karpenter config unavailable for this edge"`); transient faults are `Unavailable`, misconfiguration `FailedPrecondition`, everything else `Internal`.
- **Operation record (Redis)**: key **`/edge/karpenter/nodeop/{operation_id}`**, JSON (`kind`: `add` | `delete`, `state`, `detail`, `provider_ids`, `edge_id`, `project_id`, `cluster_id`). TTL by state: **15 min** while ACCEPTED, **2 h** once RUNNING and on a terminal FAILED (15 min for the `batch queue full; try again` / `broker shutting down; retry` / `cluster locked by another broker; retry` rejections), **24 h tombstone** once SUCCEEDED (refreshed on every duplicate send). Batch index **`/edge/karpenter/batchop/{batch_id}`** (`operation_ids`, `edge_id`): **3 h**.
- **Queue**: one in-memory FIFO and one processor goroutine **per edge** (capacity 16 waiting batches per edge, processors reaped after 1 h idle); batches of one edge run one at a time, different edges in parallel. Across broker replicas a per-cluster Redis lock (`/edge/karpenter/lock/{edgeID}`, 2 h TTL) serializes the catalog read-modify-write. A batch may carry at most **64** nodes. On restart `sweepOrphanedKarpenterOps` re-queues every still-ACCEPTED operation.
- **Backends** (`KarpenterBackend`, selected per edge): **oneclick** edits the `Worker Node SKU` catalog on the workspace compute instance (`Get → bulk increment/decrement → Apply → Publish`, then polls the compute instance every 30 s); **first-class** edits `scaling.desired` on the MKS `Cluster` (`ApplyCluster`, then polls `GetClusterStatus`). Both apply **one bulk mutation per batch**, grouped by `{pool, sku}`.
- **Deadlines**: `processBatchAdd` **90 min** (`karpenterBatchAddTimeout`), `processBatchRemove` **60 min** (`karpenterBatchRemoveTimeout`); the settle poll is bounded by that context only. Adding a node can take up to **60 minutes** (the whole infrastructure is created), so nothing on the add path may be shorter.
- **Commit point**: a failure after the platform mutation was committed (`Apply`+`Publish` / `ApplyCluster` succeeded) never produces a retryable FAILED — the ops are SUCCEEDED with detail `committed; …`. Removes are tombstoned SUCCEEDED the moment the decrement is **published**, before the settle poll; a settle failure keeps SUCCEEDED with detail `catalog decremented; compute instance did not settle`.
- **Permanent refusals**: an add the platform cannot ever accept is FAILED with a detail starting `pool at maximum` (the part of the batch that fits is still applied), `pool not found`, `pool sku mismatch`, `pool not auto-scaling` or `pool precondition`. A remove that cannot be applied (pool at its minimum, unknown pool, SKU mismatch, pool not auto-scaling) converges as **SUCCEEDED** with detail `node not retired: <reason>` — no catalog change.
- **Provider IDs**: real `rafay://<nodepoolname>/<sku_name>/<hostname>`; synthetic `rafay://<nodepoolname>/<sku_name>/synthetic-<8 hex>` when the platform did not expose the hostname. Removes report none (the platform picks the machine).

### 1.3 Upstream Karpenter (reference)

- **Registration**: `NodeClaim` **`Registered=True`** only when a **`Node`** exists with **`spec.providerID` == `NodeClaim.status.providerID`** (field-indexed list; `sigs.k8s.io/karpenter/pkg/utils/nodeclaim`, `pkg/controllers/nodeclaim/lifecycle/registration.go`). `registrationTimeout` is **60 min** in the fork (`lifecycle/liveness.go`); `launchTimeout` is 5 min.
- **Termination**: the node termination controller drains the Node, waits for volume detachment, and only then calls **`CloudProvider.Delete`** on every reconcile (5 s) until it returns `NodeClaimNotFoundError`; the Node's `karpenter.sh/termination` finalizer is released on that error only.

---

## 2. Inputs and outputs

### 2.1 Kubernetes API (user-facing)

| Input | Purpose |
|--------|---------|
| **`NodePool`** (`karpenter.sh/v1`) | Desired capacity, limits, disruption budgets, template referencing a `NodeClass`. Rendered by the broker per catalog row (`karpenter.rafay.io/managed-by=edge-broker`); a row not opted into autoscaling renders **inert** (`limits.nodes: "0"`, all-reasons `nodes: "0"` budget, `karpenter.rafay.io/auto-scaling: "false"`). Every rendered pool carries `spec.template.spec.expireAfter: Never`. |
| **`NodeClaim`** (`karpenter.sh/v1`) | Concrete scale unit; created by Karpenter from scheduling pressure, or by the `nodeadoption` controller for a pre-existing worker node (annotation `karpenter.rafay.io/adopted-provider-id`). |
| **`RafayNodeClass`** (`karpenter.rafay.io/v1alpha1`) | Instance catalog only (`spec.instanceTypes`, each entry requires `name`, `cpu`, `memory`); carries no cluster/project identity, so the same object is reusable across clusters. |

| Output | Meaning |
|--------|---------|
| **`NodeClaim.status.providerID`** | `rafay://pending/<uid>` at `Create()`; replaced by the joined Node's `spec.providerID` (`rafay://<pool>/<sku>/<hostname>`) by the `NodeProviderIDController`; must match the Node for registration. |
| **`NodeClaim.metadata.annotations`** | `karpenter.rafay.io/batch-id` (the broker batch the add was sent in; drives `batchresume`), `karpenter.rafay.io/adopted-provider-id` (adopted node), `karpenter.rafay.io/remove-retired-other` (the broker retired a different machine; see §4). |
| **`NodeClaim.status.conditions`** (`Launched`, `Registered`, `Initialized`, `Ready`, …) | Standard Karpenter lifecycle. |
| **`Node`** | Must expose **`spec.providerID`** matching the claim for healthy registration; the `nodeadoption` controller fills it in (`rafay.BuildProviderID`) when the platform left it empty. |
| **NodePool events** (Warning) | `PoolAtPlatformMaximum`, `PoolRefusedByPlatform`, `NodeNotRetired`, `NodeRetiredExternally`, `RemoveRetiredOtherMachine` — recorded by the provider so `kubectl describe nodepool` explains what the platform did. |

### 2.2 Controller environment (`karpenter-provider-rafay`)

| Variable | Role |
|----------|------|
| `CERT_FOLDER` / `EDGE_CLIENT_CERT_FOLDER` | mTLS cert material (`client.crt`, `client.key`, `ca.crt`). |
| `SERVER_PORT` / `EDGE_CLIENT_SERVER_PORT` | Broker TLS port (default **5448**). |
| `EDGE_BROKER_GRPC_INSECURE`, `EDGE_BROKER_GRPC_HOST`, `EDGE_BROKER_GRPC_PORT` | Optional plaintext dial (port defaults to **5449** inside `BrokerClient`). **Not usable for batch operations** — the broker rejects the stream with `NO CLIENT ID`. |
| `STREAM_ID` | gRPC **`sessionid`** metadata; auto UUID if unset. |
| `RAFAY_CLUSTER_ID`, `RAFAY_PROJECT_ID` | Optional labels stamped on broker payloads; the broker resolves the cluster from the certificate and never widens its lookup by `project_id`. |
| `EDGE_ID` | Logging only; **broker identity** comes from cert **Subject Organization (O)**. |
| `KARPENTER_CONFIG_BOOTSTRAP` (`true`), `KARPENTER_CONFIG_SYNC_INTERVAL` (`10m`) | Fetch and server-side-apply the broker-rendered manifests at startup and every interval (an unchanged revision still re-applies). |
| `KARPENTER_ADOPT_EXISTING_NODES` (`true`) | Create a NodeClaim per pre-existing worker node. |
| `RAFAY_POOL_AT_MAX_COOLDOWN` (`5m`) | Hold on a NodePool after a permanent add refusal. |
| `RAFAY_REMOVE_SETTLE_WINDOW` (`60m`, floor 60m) | How long `Delete()` waits for the machine behind an untargeted SUCCEEDED remove to stop before concluding the platform retired another machine. |
| `RAFAY_VM_MEMORY_OVERHEAD_PERCENT` (`7.5`), `RAFAY_KUBE_RESERVED_CPU` (`80m`), `RAFAY_KUBE_RESERVED_MEMORY` (`255Mi`) | Instance-type overhead model (Allocatable = Capacity − overhead − 100Mi eviction threshold). |
| `HEADROOM_NAMESPACE` (`karpenter`), `HEADROOM_CONFIG_NAMESPACE` (= `HEADROOM_NAMESPACE`) | Headroom Deployments and policy ConfigMap. |
| `LEADER_ELECTION_NAMESPACE` (deployment sets `karpenter`), `DISABLE_LEADER_ELECTION` | Karpenter fork options (`LEADER_ELECTION_NAME` defaults to `karpenter-leader-election`). |

### 2.3 gRPC stream contract (client → broker)

| Step | Client sends | Broker responds (typical) |
|------|----------------|---------------------------|
| 1 | **`batch_add`** `{batch_id, nodes[{operation_id, cluster_id, project_id, instance_type, node_pool_name}]}` (≤ 64 nodes) | **`batch_accepted{batch_id}`** after the ACCEPTED records and the batch index are written and the batch is queued; or **`batch_rejected{reason}`** in-band (`batch queue full`, `batch too large: …`, `batch id belongs to another edge`) with nothing written. |
| 2 | **`status_poll`** `{batch_id}` | **`batch_status{nodeResults[{operation_id, state, detail, providerIds}]}`**; an unknown/expired batch (or another edge's) answers an **empty** list; an expired op inside a known batch is `FAILED "operation record expired"`. |
| 1′ | **`batch_remove`** `{batch_id, nodes[... + provider_id]}` | As for `batch_add`, records of kind `delete`. `provider_id` may be empty (untargeted) for a NodeClaim whose machine never registered. |
| 3 | **`cancel_ops`** `{operation_ids[]}` (≤ 64) | **`cancel_ack{cancelled_operation_ids[]}`** — only ops still ACCEPTED are moved to `FAILED "cancelled by client"` (Redis compare-and-set). |

**Broker stream rules** (`edge-broker/pkg/context/karpenter_batch_stream.go`):

- Edge id from the mTLS peer certificate (`ErrorNoClientID` if missing).
- Every record carries the authenticated `edge_id`; polls and cancels only see the calling edge's records.
- Per-op idempotency on receipt: absent/FAILED → write ACCEPTED and enqueue; ACCEPTED → re-enqueue without rewrite; RUNNING → skip; SUCCEEDED → skip and refresh the tombstone; UNKNOWN → skip and log.
- The stream ends with RPC status OK on `io.EOF` / client cancel; the provider opens one short-lived stream per call.

### 2.4 Provider ID (contractual I/O)

- **Produced by**: the Rafay platform on the joined Node (`spec.providerID`), the `nodeadoption` controller for a pre-existing node that lacks one (`rafay.BuildProviderID`), and the broker on a SUCCEEDED add (`provider_ids`, real or `synthetic-<8 hex>`, best effort — the provider binds NodeClaims from Node labels, not from this field).
- **Consumed by**: Karpenter registration and garbage collection (`List()`); `Delete()` (`provider_id` on the remove item, carried for targeted removal and used to recognise which machine the platform retired).

---

## 3. Constraints and rules

### 3.1 Identity and correlation

- **`operation_id`**: required on every item; broker uses it as the Redis key suffix and for idempotency. Adds use `NodeClaim.UID`, removes `NodeClaim.UID + "-remove"` — both deterministic, so any number of retries map to the same record.
- **Provider ID format**: **`rafay://<nodepoolname>/<sku_name>/<hostname>`** — three segments, **pool first**. `rafay.ParseProviderID` / `BuildProviderID` (`pkg/rafay/providerid.go`) are the reference. `List()` filters Nodes on the `rafay://` prefix only: a cluster-ID comparison against the first segment can never match, and an empty `List()` makes Karpenter's garbage collector delete every Registered NodeClaim whose node blips NotReady.
- **Node ↔ NodeClaim binding** is by labels: Nodes are listed by `nodepoolname`, and `sku_name` must be in `NodeClaimSKUs(nodeClaim)` (the `node.kubernetes.io/instance-type` label; the NodeClass name only for a NodeClaim with no instance-type label). The three read-decide-write cycles that bind a Node (`NodeAdoptionController`, `NodeProviderIDController`, `Delete()`'s pending resolution) are serialized by `cloudprovider.NodeOwnershipMu`.

### 3.2 Client-side add completion (`karpenter-provider-rafay`)

- **ACK unblocks `Create()`**; SUCCEEDED is not on the registration critical path — the joined Node is.
- **Poll cadence**: the status poller ticks every **30 s** and polls every tracked batch on the next tick after its ACK (no initial delay). A batch is dropped after **3** consecutive empty responses or **3 h** of tracking (`maxBatchAge`), its unresolved items treated as `FAILED "batch expired at broker"`.
- **FAILED feedback**: the failure handler deletes the still-pending NodeClaim (Karpenter reprovisions in seconds); a permanent refusal first holds the NodePool back (`PoolBackoff`, `RAFAY_POOL_AT_MAX_COOLDOWN`) and records a `PoolAtPlatformMaximum` / `PoolRefusedByPlatform` Warning event. Every FAILED add is remembered per operation for 2 h so a later `Delete()` of that pending NodeClaim knows no machine is coming.
- **Restart**: `Create()` stamps `karpenter.rafay.io/batch-id`; `batchresume` re-registers pending NodeClaims' batches with the batcher at startup.

### 3.3 Broker / platform

- **Deadlines**: 90 min add / 60 min remove per batch; RUNNING records outlive the add deadline by 30 min; the batch index by 3 h.
- **Catalog policy (oneclick)**: only rows opted in (`autoScaling: true`) are moved; no row is ever created, renamed or re-typed; an add past `maxNodeCount` is trimmed (`pool at maximum`), a decrement below `minNodeCount` / 0 is refused as `node not retired`.
- **Busy platform**: an instance/cluster with an operation in progress is waited out (poll every 30 s, bounded by the batch deadline), never failed as a version conflict.
- **Ambiguous errors** on the commit call (Unavailable, DeadlineExceeded, Canceled, Unknown) are resolved by re-reading the instance/cluster: landed ⇒ committed, not landed ⇒ retryable.

### 3.4 Karpenter / API

- **`Node` registration**: exact string equality between `Node.spec.providerID` and `NodeClaim.status.providerID`.
- **No HTTP Rafay API** in this provider: only edge-broker gRPC.

---

## 4. Edge cases and expected behavior

| Scenario | Expected behavior |
|----------|---------------------|
| **Node joins with empty `spec.providerID`** | `NodeAdoptionController` (or `NodeProviderIDController` for a pending claim) stamps `rafay://<pool>/<sku>/<hostname>` and `karpenter.sh/registered=true`; registration then proceeds. |
| **Duplicate `operation_id`** | Broker idempotency (§2.3): at most one catalog mutation per id. |
| **Queue full for this edge** (16 waiting batches) | `batch_rejected "batch queue full"`, nothing written; the provider returns `ErrBatchRejected` and Karpenter retries `Create()`/`Delete()`. |
| **Broker restart before processing** | ACCEPTED ops are re-queued by the startup sweep within seconds; if that does not happen the 15-min ACCEPTED TTL surfaces `FAILED "operation record expired"` and the client re-sends. |
| **Broker restart mid-batch (RUNNING)** | Not swept (the catalog write may have landed); the record expires after 2 h and the next poll reports `operation record expired`. |
| **Platform provisioning fails** | Add: FAILED with the platform detail (oneclick restores the increment for machines that were not built); the failure handler deletes the pending NodeClaim → fresh NodeClaim, fresh operation id. |
| **Provider ID never resolved** (broker SUCCEEDED, node never registers) | Karpenter's 60-min `registrationTimeout` deletes the NodeClaim; `Delete()` sees the add SUCCEEDED and sends an **untargeted** remove so the catalog count is corrected. |
| **`Delete()` on a pending NodeClaim** | Add FAILED ⇒ `NodeClaimNotFoundError`, no RPC; add SUCCEEDED ⇒ the joined node (if any) is reserved onto the NodeClaim and a remove is sent; otherwise a **synchronous** `cancel_ops`: cancelled ⇒ `NodeClaimNotFoundError`, not cancellable (RUNNING/finished) ⇒ `nil` until the poller reports the add's outcome. |
| **Remove SUCCEEDED, machine still Ready** | Untargeted removes: `Delete()` keeps the NodeClaim Terminating until the Node is gone or NotReady, for up to `RAFAY_REMOVE_SETTLE_WINDOW`; after that it records `RemoveRetiredOtherMachine`, annotates `karpenter.rafay.io/remove-retired-other`, and holds the NodeClaim until no Node carries its ID (operator runbook: remove the Node's `karpenter.sh/termination` finalizer once the machine is retired or re-registered). |
| **Remove refused for good** (`node not retired: …`) | `Delete()` returns `NodeClaimNotFoundError`, records `NodeNotRetired` once; the machine keeps running without a Node object — restart its kubelet (or re-register it from the platform) and `nodeadoption` adopts it back. |
| **Node already gone before the first `Delete()`** (GC path, or the platform retired it and deleted/drained the Node) | No remove is sent (a remove would retire one more healthy machine); `NodeRetiredExternally` is recorded when the Node was Terminating with a stopped kubelet. |
| **`Delete` with empty `providerID`** | `NodeClaimNotFoundError`. |
| **`List()`** | Nodes without the `rafay://` prefix are ignored; there is no cluster filter. |
| **`GetKarpenterConfig` NotFound with the marker** | The `nodeconfig` controller treats it as "no config for this cluster" and re-checks at the sync interval; any other error is retried on a 5 s → 2 min backoff. |
| **Autoscaling off on the compute instance** | The broker still ships every pool, rendered inert; the provider applies it, neutralising previously live NodePools. |

### 4.1 Ordering invariant (client ↔ broker)

The broker writes the per-op ACCEPTED records and the batch index **before** replying `batch_accepted`, and the provider persists the pending ProviderID (and the batch-id annotation) only after that ACK — so after the ACK both sides can lose their process and recover; before it, nothing is recorded anywhere and Karpenter simply calls again.

---

## 5. Sequence (concise)

**Scale-out**

1. Karpenter calls **`CloudProvider.Create(NodeClaim)`**.
2. Provider enqueues the add; the batcher collects up to 10 items / 10 s and sends **`batch_add`**; the broker writes ACCEPTED records + index, queues the batch, replies **`batch_accepted`**.
3. **`Create()` returns** `status.providerID = rafay://pending/<uid>`; Karpenter patches `Launched=True`.
4. The broker processor (one batch at a time per edge) applies the catalog / desired-count increment, polls the platform until it settles (up to 90 min), writes SUCCEEDED (+ provider ids) or FAILED.
5. The provider polls `status_poll` every 30 s: FAILED → failure handler; SUCCEEDED → recorded.
6. The Node joins (typically ~12 min, up to 60) with labels `nodepoolname` + `sku_name`; `NodeProviderIDController` labels it `karpenter.sh/registered=true` and patches **`status.providerID`**; Karpenter sets **`Registered=True`**.

**Scale-in**

1. Karpenter's termination controller drains the Node, waits for volumes, then calls **`CloudProvider.Delete(NodeClaim)`** every 5 s.
2. Provider sends **`batch_remove`** (`<uid>-remove`, `provider_id`), returns `nil` at ACK; duplicates are suppressed while in flight.
3. The broker publishes the decrement (ops SUCCEEDED at publish), the platform retires a machine of its choosing.
4. The provider observes SUCCEEDED; `Delete()` returns `NodeClaimNotFoundError` once the machine is gone (Node absent or NotReady), and the finalizer is released.

---

## 6. Related source files

| Area | File(s) |
|------|---------|
| Provider entry | `karpenter-provider-rafay/cmd/controller/main.go` |
| Create/Delete/List/Get, failure handler, events | `karpenter-provider-rafay/pkg/cloudprovider/cloudprovider.go` |
| Pool backoff, refusal-detail contract | `karpenter-provider-rafay/pkg/cloudprovider/poolbackoff.go` |
| Broker client + deadlines | `karpenter-provider-rafay/pkg/rafay/brokerclient.go` |
| Batcher (queue, poller, cancel, expiry) | `karpenter-provider-rafay/pkg/rafay/batcher.go` |
| Stream helpers | `karpenter-provider-rafay/pkg/rafay/batch_stream.go` |
| Provider ID parse / build | `karpenter-provider-rafay/pkg/rafay/providerid.go` |
| Helper controllers | `karpenter-provider-rafay/pkg/controllers/{nodeproviderid,nodeadoption,nodeconfig,headroom,batchresume,rafaynodeclass}` |
| Broker stream + Redis + queue | `edge-broker/pkg/context/karpenter_batch_stream.go`, `karpenter_node_stream.go`, `karpenter_batch_recovery.go` |
| Broker backends | `edge-broker/pkg/context/karpenter_backend.go`, `karpenter_backend_oneclick.go`, `karpenter_backend_firstclass.go` |
| Catalog helpers / workspace constants | `edge-broker/pkg/context/karpenter_workspace.go` |
| Config rendering | `edge-broker/pkg/context/karpenter_config.go`, `karpenter_config_render.go` |
| Service registration | `edge-broker/main.go` (`RegisterKarpenterBatchServiceServer`, `RegisterKarpenterConfigServiceServer` on both listeners; `RegisterEdgeBrokerServiceServer` on the internal one only) |

---

*This spec reflects the code layout at authoring time; re-verify function-level behavior after major refactors.*
