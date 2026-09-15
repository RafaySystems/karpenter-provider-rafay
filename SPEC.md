# Specification: Karpenter-based autoscaling (Rafay / edge-broker)

This document describes **end-to-end** behavior for node autoscaling using **Karpenter** with **`karpenter-provider-rafay`** (cluster-side) and **`edge-broker`** (server-side). Upstream Karpenter semantics (NodePool, NodeClaim, disruption, registration) apply unless overridden below.

**Repositories**

| Role | Path |
|------|------|
| Cloud provider + operator binary | `karpenter-provider-rafay` (this repo) |
| gRPC `KarpenterNodeService` implementation | `edge-broker` (`pkg/context/karpenter_node_stream.go`, `karpenter_node_lifecycle.go`, `karpenter_workspace.go`) |
| Protobuf / generated client types | `edge-common` (`rep.edge.v1`) |

---

## 1. Features and functionality

### 1.1 Cluster side (`karpenter-provider-rafay`)

- **Karpenter v1.11 integration**: Registers upstream Karpenter operator, core controllers (provisioning, disruption, consolidation, etc.), `Cluster` state, and `InstanceTypeStore`; supplies a **Rafay** `CloudProvider` implementation (`Name() == "rafay"`).
- **`RafayNodeClass`**: Custom `NodeClass` carrying a catalog of **instance types** (name, CPU, memory, zone) and nothing else — no Rafay cluster/project identity. Identity is always **`RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID`** on the controller Deployment, set per cluster, so one `RafayNodeClass` manifest is portable across every cluster. A small **readiness** controller sets `Ready=True` so NodePools can reference the class.
- **Scale-out (`Create`)**: Resolves `RafayNodeClass`, picks the **first** instance type compatible with the `NodeClaim` requirements and resource requests, calls **`AddNodes`** on the broker client with **`operation_id` = `NodeClaim.UID`**, and returns a `NodeClaim` with **`status.providerID`**, capacity, and merged labels. **`providerID` is only populated after the add stream completes with broker-reported success** (see §3.2).
- **Scale-in (`Delete`)**: Calls **`RemoveNode(providerID, NodeClaim.UID)`** on the broker stream. Maps `ErrNodeNotFound` to Karpenter’s not-found error.
- **No broker Get/List**: `Get` / `List` fall back to listing **`Node`** objects in the API and matching **`spec.providerID`** (exact string for `Get`; `List` only includes IDs with prefix **`rafay://`** and optional cluster segment filter via `ParseProviderID`).
- **No drift**: `IsDrifted` is always empty (Rafay / PaaS owns node shape after provision).
- **Transport**: One shared **gRPC** connection; **one bidi `StreamOperations` stream per add or remove**; mTLS (or optional insecure dial for dev). Metadata **`sessionid`** = stream ID for broker routing.

### 1.2 Broker side (`edge-broker`)

- **`KarpenterNodeService.StreamOperations`**: Single long-lived **Recv** loop per client stream. The client must send **`NodeAdd`** or **`NodeDelete`** first (binds the stream to one **`operation_id`**), then **status poll** messages; the broker **only sends** on the stream from this loop (async work updates **Redis**).
- **Operation record (Redis)**: Key **`/edge/karpenter/nodeop/{operation_id}`**, JSON `karpenterNodeOpRecord` (`kind`: `add` | `delete`, `state`, `detail`, `provider_ids`), **TTL 60 minutes** (refreshed on patches).
- **Add lifecycle** (`computeInstanceKarpenterNodeLifecycle`):
  - **Precheck**: `CheckKarpenterStreamNodeAddAllowed` may reject new adds with **`ResourceExhausted`** if the edge’s **workspace compute instance** status is **`INPROGRESS`** (catalog busy).
  - **Apply**: `ApplyKarpenterStreamNodeAdd` drives **PaaS workspace compute instance** worker-node catalog sync (`applyComputeInstanceNodeAdd`), then returns **synthetic provider IDs** via `streamAddProviderIDs`:
    - Format: **`rafay://{clusterId}/{nodePoolName}/{instanceType}/node-{8-char-id}`** (8-char id from `common.NewID()`).
  - **Idempotent replay**: If Redis already has **`kind: add`** for the same `operation_id`, broker sends **ACCEPTED** (`idempotent_replay`) and starts **`SyncKarpenterStreamNodeAddFromComputeInstance`** (updates Redis from compute instance status **without** re-applying catalog).
- **Delete lifecycle**: Parses **`provider_id`** with **`parseWorkerNodeCatalogFromProviderID`**: requires **at least four path segments** after `rafay://` (`cluster`, **pool**, **instance type**, node suffix). Legacy **`rafay://cluster/node`** fails delete with “must be … for catalog delete”.
- **Status polling (server)**: On **`Status`**, broker reads Redis and sends **`StatusUpdate`** with current state. If **add** + **`SUCCEEDED`** + **non-empty `provider_ids`**, broker sends a second message **`AddResult`**. If **delete** + **`SUCCEEDED`**, sends **`DeleteResult`**.
- **Worker timeouts**: `runKarpenterStreamAdd` / `runKarpenterStreamDelete` use a **50-minute** context timeout per operation; idempotent sync path uses **5 minutes**.

### 1.3 Upstream Karpenter (reference)

- **Registration**: `NodeClaim` **`Registered=True`** only when a **`Node`** exists with **`spec.providerID` == `NodeClaim.status.providerID`** (field-indexed list; see `sigs.k8s.io/karpenter/pkg/utils/nodeclaim` and `pkg/controllers/nodeclaim/lifecycle/registration.go`).
- **Termination**: After nodes are deleted as needed, **`CloudProvider.Delete`** is invoked when **`status.providerID` != ""** (lifecycle `finalize`).

---

## 2. Inputs and outputs

### 2.1 Kubernetes API (user-facing)

| Input | Purpose |
|--------|---------|
| **`NodePool`** (`karpenter.sh/v1`) | Desired capacity, limits, disruption budgets, template referencing a `NodeClass`. |
| **`NodeClaim`** (`karpenter.sh/v1`) | Concrete scale unit; created by Karpenter from scheduling pressure. |
| **`RafayNodeClass`** (`karpenter.rafay.io/v1alpha1`) | Instance catalog only (`spec.instanceTypes`); carries no cluster/project identity, so the same object is reusable across clusters. |

| Output | Meaning |
|--------|---------|
| **`NodeClaim.status.providerID`** | Broker-issued stable ID for this claim; must match joining **`Node.spec.providerID`** for registration. |
| **`NodeClaim.status.conditions`** (`Launched`, `Registered`, `Initialized`, `Ready`, …) | Standard Karpenter lifecycle. |
| **`Node`** | Must expose **`spec.providerID`** matching the claim for healthy registration. |

### 2.2 Controller environment (`karpenter-provider-rafay`)

| Variable | Role |
|----------|------|
| `CERT_FOLDER` / `EDGE_CLIENT_CERT_FOLDER` | mTLS cert material (`client.crt`, `client.key`, `ca.crt`). |
| `SERVER_PORT` / `EDGE_CLIENT_SERVER_PORT` | Broker TLS port (default **5448**). |
| `EDGE_BROKER_GRPC_INSECURE`, `EDGE_BROKER_GRPC_HOST`, `EDGE_BROKER_GRPC_PORT` | Optional plaintext dial (e.g. **5449**). |
| `STREAM_ID` | gRPC **`sessionid`** metadata; auto UUID if unset. |
| `RAFAY_CLUSTER_ID`, `RAFAY_PROJECT_ID` | Sole source of Rafay cluster/project identity for broker payloads; `RafayNodeClass` has no override. |
| `EDGE_ID` | Logging only; **broker identity** comes from cert **Subject Organization (O)**. |

### 2.3 gRPC stream contract (client → broker)

| Step | Client sends | Broker responds (typical) |
|------|----------------|---------------------------|
| 1 | **`NodeAdd`** + **`operation_id`** (required) | **`StatusUpdate` ACCEPTED**; starts async add (Redis **ACCEPTED** → **RUNNING** → **SUCCEEDED** or **FAILED**). |
| 2 | **`Status`** poll (same `operation_id` as bound stream) | **`StatusUpdate`** with Redis state; on terminal **add success**, **`AddResult`** with `provider_ids`. |
| 1′ | **`NodeDelete`** + **`operation_id`** | **`StatusUpdate` ACCEPTED**; async delete. |
| 2′ | **`Status`** | **`StatusUpdate`**; on **delete success**, **`DeleteResult`**. |

**Broker stream rules** (`edge-broker/pkg/context/karpenter_node_stream.go`):

- **`sessionid`** metadata required (`ErrNoStreamID` if missing).
- **Edge ID** from mTLS / `GetEdgeClientInfo` (`ErrorNoClientID` if missing).
- **One `operation_id` per stream** after first `NodeAdd`/`NodeDelete`; status **`operation_id`** must match **bound** id.

### 2.4 Provider ID (contractual I/O)

- **Produced by**: Broker add path (`streamAddProviderIDs` after successful catalog apply), and idempotent sync may fill `provider_ids` when compute reaches **SUCCESS**.
- **Consumed by**: Karpenter **`NodeClaim`**; **`Node.spec.providerID`** on join; broker **delete** parsing (pool + instance type for catalog row).

---

## 3. Constraints and rules

### 3.1 Identity and correlation

- **`operation_id`**: Required on every add/delete frame; broker uses it as **Redis key suffix** and for idempotency. **Client uses `NodeClaim.UID`** on create/delete for traceability.
- **Stream binding**: First `NodeAdd`/`NodeDelete` fixes the stream to that `operation_id`; mixing operations on one stream is rejected.
- **Provider ID format (broker-generated add)**:
  - **`rafay://<clusterId>/<nodePoolName>/<instanceType>/node-<suffix>`** (four path segments after scheme; see `edge-broker/pkg/context/karpenter_node_lifecycle.go` `streamAddProviderIDs`).
  - **`ParseProviderID` in the provider** splits on the **first** `/` only (`clusterID` / remainder); **`listNodesFromKube`** additionally requires prefix **`rafay://`** and optional **cluster ID** match to controller `RAFAY_CLUSTER_ID`.

### 3.2 Client-side add completion (`karpenter-provider-rafay`)

- **`pollTimeout`**: **60 minutes** per `AddNodes` / `RemoveNodes` stream context.
- **Poll cadence**: **120 s** initial wait after **ACCEPTED**, then **30 s** between status sends (`karpenter_node_stream.go`).
- **Provider IDs on success**: The client treats the operation as finished only after **`KARPENTER_NODE_OPERATION_STATE_SUCCEEDED`** and a non-empty provider id set (either **`AddResult`** immediately after that status, or a **buffered** early `AddResult` reconciled at `SUCCEEDED`). Bare `AddResult` before `SUCCEEDED` does **not** complete the client call.

### 3.3 Broker / platform

- **Redis TTL**: Operation records expire after **60 minutes** unless rewritten.
- **Catalog mutex**: New adds may be rejected (**`ResourceExhausted`**) while workspace compute instance is **`INPROGRESS`**.
- **Delete**: **`provider_id`** must match **four-segment** Rafay path for catalog removal; legacy two-segment IDs are **not** accepted for delete on this lifecycle.

### 3.4 Karpenter / API

- **`Node` registration**: **Exact string equality** between **`Node.spec.providerID`** and **`NodeClaim.status.providerID`** (upstream `AllNodesForNodeClaim` field selector).
- **No HTTP Rafay API** in this provider: only **edge-broker gRPC** for add/remove.

---

## 4. Edge cases and expected behavior

| Scenario | Expected behavior |
|----------|---------------------|
| **Broker returns success but node has empty `spec.providerID`** | **`Registered`** stays **`Unknown`** (`NodeNotFound`); disruption may log “no associated node”; fix is **platform/kubelet/CCM** to set `spec.providerID` to the broker ID (or align broker ID with what the node will report). |
| **Duplicate `operation_id` for different kind** | Broker errors: `operation_id` already in use for **non-add**. |
| **Replay same add `operation_id`** | Broker **ACCEPTED** + idempotent sync goroutine; Redis state refreshed from compute instance; no second catalog apply. |
| **Status poll before NodeAdd** | Broker error: send **`node_add` or `node_delete` before status**. |
| **Status `operation_id` ≠ bound stream** | Broker error: must match bound id. |
| **Redis miss on status** | Broker sends **`FAILED`** / `unknown operation` and continues loop. |
| **Workspace compute fails** | Redis → **FAILED** with detail; client sees failed status on poll → **`CreateError`**. |
| **Client stream cancelled mid-operation** | Async broker work may still run until timeout; Redis reflects outcome; client may retry with **same** or new `operation_id` per Karpenter. |
| **`RemoveNode` when instance already gone** | Provider maps **`ErrNodeNotFound`** → **`NodeClaimNotFoundError`** for Karpenter finalizer handling. |
| **`Delete` with empty `providerID`** | **`NodeClaimNotFoundError`** (cannot call broker delete). |
| **`List` / synthetic cloud** | Nodes without **`rafay://`** prefix on **`spec.providerID`** are **ignored** by `listNodesFromKube` even if they exist. |
| **Upstream “cluster sync” logs** | Long **unregistered** windows can trigger Karpenter **ERROR**-level “waiting on cluster sync” (upstream `Cluster.Synced`); not controlled by this provider. |
| **Broker `SUCCEEDED` without `provider_ids` in Redis (add)** | Status sent as success; **no `AddResult`** frame; **client** may error on missing second message unless a pending `AddResult` was buffered (broker should persist `provider_ids` before `SUCCEEDED` for add—see `runKarpenterStreamAdd` patch order: IDs set in same patch as **SUCCEEDED**). |

### 4.1 Ordering invariant (client ↔ broker)

The **edge-broker** implementation sends **`AddResult`** only **after** a status response that includes **`SUCCEEDED`** and non-empty `provider_ids` (`karpenter_node_stream.go` lines 222–234). That matches the **provider** rule of completing **`Create`** only after **`SUCCEEDED`** (and non-empty IDs), avoiding treating a premature catalog response as final.

---

## 5. Sequence (concise)

**Scale-out**

1. Karpenter calls **`CloudProvider.Create(NodeClaim)`**.
2. Provider opens **`StreamOperations`**, sends **`NodeAdd`** + **`operation_id`**, receives **ACCEPTED**.
3. Provider polls **Status** until **SUCCEEDED** + **`AddResult`** (or buffered early result at `SUCCEEDED`).
4. Provider returns **`NodeClaim`** with **`status.providerID`** and capacity; Karpenter patches **`Launched=True`**.
5. When a **`Node`** joins with **matching `spec.providerID`**, Karpenter sets **`Registered=True`** (upstream registration controller).

**Scale-in**

1. Karpenter lifecycle calls **`CloudProvider.Delete(NodeClaim)`** when terminating and **`providerID` != ""`**.
2. Provider streams **`NodeDelete`** + **`operation_id`**, polls until **SUCCEEDED** + **`DeleteResult`**.

---

## 6. Related source files

| Area | File(s) |
|------|---------|
| Provider entry | `karpenter-provider-rafay/cmd/controller/main.go` |
| Create/Delete/List/Get | `karpenter-provider-rafay/pkg/cloudprovider/cloudprovider.go` |
| Broker client + timeouts | `karpenter-provider-rafay/pkg/rafay/brokerclient.go` |
| Stream add/delete recv | `karpenter-provider-rafay/pkg/rafay/karpenter_node_stream.go` |
| Provider ID parse | `karpenter-provider-rafay/pkg/rafay/providerid.go` |
| Broker stream + Redis | `edge-broker/pkg/context/karpenter_node_stream.go` |
| Broker add/delete + ID shape | `edge-broker/pkg/context/karpenter_node_lifecycle.go` |
| Delete parse / workspace constants | `edge-broker/pkg/context/karpenter_workspace.go` |
| Service registration | `edge-broker/main.go` (`RegisterKarpenterNodeServiceServer`) |

---

*This spec reflects the code layout at authoring time; line-level behavior should be re-verified after major refactors.*
