# Architecture: karpenter-provider-rafay

## Overview

`karpenter-provider-rafay` is a [Karpenter](https://karpenter.sh/) cloud provider plugin for the **Rafay private cloud / managed Kubernetes** platform. It enables Karpenter's autoscaler to provision and deprovision nodes on Rafay-managed clusters by routing all infrastructure operations through Rafay's **edge-broker** gRPC relay — the same communication channel used by the `edge-client` agent that runs inside every Rafay-managed cluster.

This is not a public cloud provider (no AWS/GCP/Azure APIs). Node add and remove go over **`rep.edge.v1.KarpenterNodeService.StreamOperations`** on edge-broker (bidi stream per operation), with the same TLS certificate material and dial semantics as `edge-client`. A small `pkg/brokerproto` tree mirrors `rep.edge.v1` **EdgeCommand** shapes for reference; the running controller uses **`github.com/RafaySystems/edge-common/pkg/edge/v1`** generated types for the Karpenter node stream.

---

## Repository Layout

```
karpenter-provider-rafay/
├── cmd/
│   └── controller/          # Binary entry point (main.go)
├── config/
│   ├── crd/                 # CRD YAML: RafayNodeClass + upstream Karpenter (NodePool, NodeClaim; tag matches go.mod)
│   ├── deploy/              # Kubernetes deployment, RBAC, SA, Secret manifests
│   └── kustomization.yaml   # Apply with: kubectl apply -k config (includes crd/ + deploy/)
├── examples/                # Example RafayNodeClass, NodePool, test pod YAMLs
├── pkg/
│   ├── apis/v1alpha1/       # RafayNodeClass CRD type, spec/status, deepcopy
│   ├── broker/              # gRPC dial helpers, TLS credential loading, session ID
│   ├── brokerproto/         # Optional EdgeCommand-shaped protos (reference); runtime uses edge-common rep.edge.v1
│   ├── cloudprovider/       # Karpenter CloudProvider interface implementation
│   ├── controllers/
│   │   └── rafaynodeclass/  # Reconciler: sets Ready status on RafayNodeClass objects
│   ├── operator/            # Scheme registration (init)
│   └── rafay/               # Client interface, BrokerClient implementation, helpers
├── Dockerfile               # Multi-stage: golang:1.24-alpine builder → distroless/static (go.mod `go` directive may be newer; image/toolchain should match release CI)
├── Dockerfile.dev           # Prebuilt linux/arm64 binary → distroless:debug (dev push flow)
├── Makefile
└── go.mod
```

---

## Component Map

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                          │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                  karpenter-provider-rafay Pod                │  │
│  │                                                              │  │
│  │  ┌─────────────┐   ┌───────────────────┐  ┌─────────────┐  │  │
│  │  │  Karpenter  │   │  CloudProvider    │  │  RafayNode  │  │  │
│  │  │  Core       │──▶│  (cloudprovider/) │  │  Class Ctrl │  │  │
│  │  │  Controllers│   │                   │  │  (readiness)│  │  │
│  │  │  (upstream) │   │  Create / Delete  │  └─────────────┘  │  │
│  │  └─────────────┘   │  Get / List       │                   │  │
│  │                    │  GetInstanceTypes │  ┌─────────────┐  │  │
│  │                    └────────┬──────────┘  │  Karpenter  │  │  │
│  │                             │             │  Operator   │  │  │
│  │                             │             │  (upstream) │  │  │
│  │                    ┌────────▼──────────┐  └─────────────┘  │  │
│  │                    │  BrokerClient     │                   │  │
│  │                    │  (rafay/)         │                   │  │
│  │                    │  AddNodes         │                   │  │
│  │                    │  RemoveNode       │                   │  │
│  │                    └────────┬──────────┘                   │  │
│  └─────────────────────────────┼────────────────────────────────┘  │
│                                │ gRPC (mTLS / port 5448)           │
└────────────────────────────────┼─────────────────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │     Rafay Edge-Broker   │
                    │  KarpenterNodeService   │
                    │  StreamOperations (bidi)│
                    └─────────────────────────┘
                                 │
                    ┌────────────▼────────────┐
                    │   Rafay Control Plane   │
                    │  (node provisioning)    │
                    └─────────────────────────┘
```

---

## Core Packages

### `cmd/controller` — Entry Point

`main.go` wires the entire operator:

1. Initializes the upstream Karpenter operator (`coreoperator.NewOperator`), which sets up the controller-runtime Manager, leader election, metrics, and webhook server.
2. Reads all configuration from environment variables (TLS certs, broker host/port, cluster/project IDs).
3. Resolves **edge identity** from the TLS client certificate **Subject Organization (O)** — edge-broker uses this from the mTLS peer cert; optional `EDGE_ID` is for logging only and may warn if it disagrees with the cert.
4. Constructs a `rafay.BrokerClient` using the resolved gRPC connection parameters.
5. Builds raw `CloudProvider`, `metrics.Decorate(cp)`, and `state.NewCluster(...)` for Karpenter v1.11 wiring.
6. Calls `corecontrollers.NewControllers(...)` with the **decorated** provider for metrics, the **undecorated** `cp`, `clusterState`, and `InstanceTypeStore`, then appends the `rafaynodeclass` controller.
7. Starts all controllers via `op.WithControllers(...).Start(ctx)`.

### `pkg/apis/v1alpha1` — Custom API Type

Defines the `RafayNodeClass` CRD:

```
RafayNodeClass
├── spec
│   ├── clusterID       string    # Rafay cluster ID (overrides env var)
│   ├── projectID       string    # Rafay project ID (overrides env var)
│   └── instanceTypes   []InstanceTypeSpec
│       ├── name        string    # e.g. "standard-4-8"
│       ├── cpu         string    # e.g. "4"
│       ├── memory      string    # e.g. "8Gi"
│       └── zone        string
└── status
    └── conditions      []Condition   # Ready=True when reconciled
```

Also registers the scheme and implements `DeepCopyObject` for controller-runtime compatibility.

### `pkg/rafay` — Client abstraction and broker streams

**`Client` interface:**

```go
type Client interface {
    AddNodes(ctx context.Context, req AddNodesRequest) (*AddNodesResponse, error)
    RemoveNode(ctx context.Context, providerID string) error
    GetNode(ctx context.Context, providerID string) (*NodeInfo, error)
    ListNodes(ctx context.Context, clusterID string) ([]*NodeInfo, error)
}
```

`GetNode` and `ListNodes` intentionally return sentinel errors (`ErrGetNodeUnsupported`, `ErrListNodesUnsupported`) on the `BrokerClient` implementation — the edge-broker has no query API; those paths fall back to reading Kubernetes `Node` objects directly.

**`ParseProviderID(id string) (clusterID, nodeID string, err error)`** — parses the `rafay://clusterID/nodeID` format.

**`BrokerClient`**: one shared `*grpc.ClientConn` (lazy, mutex-protected); each `AddNodes` / `RemoveNodes` opens a **new** bidi stream on **`KarpenterNodeService.StreamOperations`**. Stream metadata includes **`sessionid`** (configured stream ID).

**`karpenter_node_stream.go`**: sends the first frame (`AddNodesRequest` / `RemoveNodesRequest`), then polls inbound frames until the broker reports a terminal `OperationStatus` or **`pollTimeout` (60 minutes)** in `brokerclient.go` elapses. Between polls: **120s** initial delay, then **30s** interval (constants in `karpenter_node_stream.go`).

**Correlation on add**: `AddNodesRequest.OperationID` is set to **`NodeClaim.UID`**; the client accepts the broker’s operation id when echoed. **`operation_id` is not persisted** across controller restarts — if the process dies mid-stream, Karpenter re-invokes `Create` and opens a new stream.

### `pkg/broker` — gRPC Connection Helpers

- `NewSecureConn(host, port, certFolder)` — loads `client.crt`, `client.key`, `ca.crt` from `certFolder`, dials with mTLS
- `NewInsecureConn(host, port)` — plaintext gRPC (dev/test only)
- `ExtractEdgeIDFromCert(certPath)` — reads the Subject Organization from the TLS certificate to derive the edge identity
- `NewSessionID()` — generates a UUID v4 for the gRPC `sessionid` metadata header
- Dial uses **30s** default timeout; client keepalive **30s** time / **30s** timeout (see `pkg/broker/conn.go`).

### `pkg/brokerproto` — Reference types (optional)

Generated Go types under **`rep.edge.v1`** that mirror **EdgeCommand**-style messages. The **live** node add/remove path uses **`github.com/RafaySystems/edge-common/pkg/edge/v1`** (`AddNodesRequest`, `RemoveNodesRequest`, stream frames) — keep `brokerproto` in sync with broker contracts when you need local types without importing all of edge-common, but do not assume the controller sends `EdgeCommand` on the Karpenter stream.

### `pkg/cloudprovider` — Karpenter Interface Implementation

Implements `sigs.k8s.io/karpenter/pkg/cloudprovider.CloudProvider`:

| Method | Behavior |
|---|---|
| `Create(NodeClaim)` | Resolves `RafayNodeClass`, selects compatible instance type, calls `AddNodes` with **`OperationID` = NodeClaim UID**, sets `status.providerID` + capacity on the returned `NodeClaim` |
| `Delete(NodeClaim)` | Calls `RemoveNode(providerID)`; maps `ErrNodeNotFound` → `NodeClaimNotFoundError` |
| `Get(providerID)` | Calls `GetNode` (unsupported), falls back to scanning k8s `Node` list by `spec.providerID` |
| `List()` | Calls `ListNodes` (unsupported), falls back to listing k8s `Node` objects with `rafay://` prefix |
| `GetInstanceTypes(NodePool)` | Returns `RafayNodeClass.spec.instanceTypes` or 6 built-in t-shirt sizes |
| `IsDrifted(NodeClaim)` | Always `("", nil)` — no drift detection |
| `Name()` | `"rafay"` |

**Instance type selection** in `Create()`: filters the available instance types against `NodeClaim.Spec.Requirements` (arch, capacity-type, zones, etc.) and `Spec.Resources.Requests` (CPU/memory). Picks the first match.

**Default instance types** (used when `RafayNodeClass.spec.instanceTypes` is empty):

| Name | CPU | Memory |
|---|---|---|
| `standard-2-4` | 2 | 4Gi |
| `standard-4-8` | 4 | 8Gi |
| `standard-8-16` | 8 | 16Gi |
| `standard-16-32` | 16 | 32Gi |
| `standard-32-64` | 32 | 64Gi |
| `standard-48-96` | 48 | 96Gi |

### `pkg/controllers/rafaynodeclass` — Readiness Controller

A minimal controller-runtime reconciler. On every `RafayNodeClass` event (create/update), if the object is not being deleted, it patches `status.conditions[Ready]=True`. This is required because upstream Karpenter's NodePool controller gates provisioning on the referenced NodeClass being `Ready`.

Registration: `controllerruntime.NewControllerManagedBy(m).For(&v1alpha1.RafayNodeClass{})`, `MaxConcurrentReconciles: 10`, named `rafaynodeclass.readiness`.

---

## CRDs Deployed

| CRD | Group | Defined In |
|---|---|---|
| `rafaynodeclasses.karpenter.rafay.io` | `karpenter.rafay.io/v1alpha1` | This repo |
| `nodeclaims.karpenter.sh` | `karpenter.sh/v1` | Upstream Karpenter (YAML in `config/crd/` should match **Karpenter v1.11.x** in `go.mod`, e.g. `status.nodes`, scale subresource) |
| `nodepools.karpenter.sh` | `karpenter.sh/v1` | Upstream Karpenter (same version alignment) |

Apply CRDs and deploy together: **`kubectl apply -k config`**.

---

## Data Flows

### Scale-Out (Node Provisioning)

```
Pod unschedulable
  └─▶ Karpenter provisioner creates NodeClaim
        └─▶ CloudProvider.Create(nodeClaim)
              ├─ GET RafayNodeClass (k8s API)
              ├─ Select compatible InstanceType
              └─▶ BrokerClient.AddNodes(AddNodesRequest{..., OperationID: nodeClaim.UID})
                    ├─ getConn() → mTLS gRPC dial to edge-broker (lazy, one shared conn)
                    ├─ NewStreamOperations(ctx) with metadata sessionid = STREAM_ID
                    ├─ Send(AddNodesRequest) as first frame
                    └─ Recv poll loop (initial 120s, then 30s; overall cap pollTimeout 60m)
                          └─ Return NodeClaim{status.providerID="rafay://clusterID/nodeID"}
  └─▶ Karpenter watches for k8s Node with matching spec.providerID
  └─▶ Node registered → NodeClaim transitions to Ready
```

### Scale-In (Node Deprovisioning)

```
Disruption controller: consolidate / expire NodeClaim
  └─▶ CloudProvider.Delete(nodeClaim)
        └─▶ BrokerClient.RemoveNode(providerID)
              ├─ NewStreamOperations → Send(RemoveNodesRequest{...})
              └─ Recv poll loop (same timing / 60m cap as add) → success or ErrNodeNotFound
  └─▶ Karpenter drains and deletes the Kubernetes Node object
```

### Reconciliation / Garbage Collection (List)

```
Karpenter periodic reconcile
  └─▶ CloudProvider.List()
        ├─ BrokerClient.ListNodes() → ErrListNodesUnsupported
        └─ fallback: kubeClient.List(NodeList)
              └─ filter: spec.providerID prefix "rafay://"
              └─ filter: clusterID segment matches configured clusterID
              └─ Return synthetic NodeClaims from Node objects
```

### NodeClass Readiness

```
RafayNodeClass created/updated
  └─▶ rafaynodeclass.Controller.Reconcile()
        └─ kubeClient.Status().Patch → status.conditions[Ready]=True
  └─▶ NodePool readiness gate satisfied
  └─▶ NodePool active for provisioning
```

### Connection Management

```
BrokerClient (single *grpc.ClientConn, protected by sync.Mutex)
  ├─ Lazy-initialized on first RPC call
  ├─ Per-operation: new bidi StreamOperations on shared conn
  ├─ Routing: gRPC metadata sessionid (stream ID); add correlates via OperationId (NodeClaim UID)
  ├─ On codes.Unavailable: tear down conn, re-dial once, retry
  └─ Stream closed after each operation
```

### Client-side timeouts (summary)

| Layer | Constant / behavior | Value |
|---|---|---|
| gRPC dial | `defaultDialTimeout` (`pkg/broker/conn.go`) | 30s |
| gRPC keepalive | ping / permit | 30s / 30s |
| Stream poll (per add/remove) | `pollTimeout` (`pkg/rafay/brokerclient.go`) | 60 minutes |
| Between inbound polls | `karpenterNodePollInitialDelay` / `karpenterNodePollInterval` | 120s / 30s |

---

## Configuration Reference

All configuration is via environment variables:

| Variable | Default | Description |
|---|---|---|
| `CERT_FOLDER` / `EDGE_CLIENT_CERT_FOLDER` | — | Directory containing `client.crt`, `client.key`, `ca.crt` for mTLS |
| `SERVER_PORT` / `EDGE_CLIENT_SERVER_PORT` | `5448` | TLS gRPC port to edge-broker |
| `EDGE_BROKER_GRPC_INSECURE` | `false` | Set `true` for plaintext gRPC (dev only) |
| `EDGE_BROKER_GRPC_PORT` | `5449` | Port for insecure mode |
| `EDGE_BROKER_GRPC_HOST` | (from cert OU) | Override broker dial host |
| `EDGE_ID` | (from cert Subject O) | Logging label for edge identity |
| `STREAM_ID` | (auto UUID v4) | gRPC `sessionid` metadata header |
| `RAFAY_CLUSTER_ID` | — | Default cluster ID (overridden by `RafayNodeClass.spec.clusterID`) |
| `RAFAY_PROJECT_ID` | — | Default project ID (overridden by `RafayNodeClass.spec.projectID`) |
| `LEADER_ELECTION_NAMESPACE` | `karpenter` | Namespace for leader election Lease objects |
| `KARPENTER_DISABLE_LEADER_ELECTION` | `false` | Disable leader election (local dev) |

> **Note:** Leader election is placed in the `karpenter` namespace (not `rafay-system`) because Rafay's platform webhook blocks Lease writes in `rafay-system`.

Kubernetes Secrets consumed:

| Secret | Namespace | Contents |
|---|---|---|
| `edge-client-creds` | `rafay-system` | `client.crt`, `client.key`, `ca.crt` — mounted at `/opt/rcloud/certs/` |
| `karpenter-provider-rafay` | `rafay-system` | Optional `RAFAY_CLUSTER_ID` / `RAFAY_PROJECT_ID` overrides via `envFrom` |

---

## Key Dependencies

| Module | Version | Role |
|---|---|---|
| `sigs.k8s.io/karpenter` | v1.11.1 | Core framework, operator, controllers, CloudProvider interface |
| `sigs.k8s.io/controller-runtime` | v0.23.3 | Reconciler infrastructure, Manager, client |
| `github.com/awslabs/operatorpkg` | (see go.mod) | Operator lifecycle, `status.Condition`, `controller.Controller` |
| `github.com/RafaySystems/edge-common` | replace → local | `rep.edge.v1` Karpenter node stream protos and generated Go |
| `google.golang.org/grpc` | v1.72.2 | gRPC client for edge-broker |
| `google.golang.org/protobuf` | v1.36.11 | Protobuf serialization |
| `k8s.io/client-go` | v0.35.1 | Kubernetes client (direct require) |
| `k8s.io/api`, `k8s.io/apimachinery` | **replaced** to v0.35.1 | Align with client-go / Karpenter (edge-common may list older pseudo-versions) |
| `github.com/samber/lo` | v1.52.0 | Functional helpers (Filter, Map) |
| `github.com/google/uuid` | v1.6.0 | UUID v4 for `sessionid` when `STREAM_ID` unset |

---

## Design Decisions

**No HTTP/REST calls to Rafay API.** All operations go through the edge-broker gRPC relay. This means the provider works within the same trust boundary as the `edge-client` agent already deployed in the cluster.

**No state in the provider.** Karpenter's CRDs (`NodeClaim`, `NodePool`) and Kubernetes `Node` objects are the sole source of truth. The provider itself is stateless beyond the cached gRPC connection. In-flight broker correlation uses **`operation_id`** (NodeClaim UID on add) only for the lifetime of that stream.

**No Get/List API on the broker.** `GetNode` and `ListNodes` are not supported by the edge-broker protocol. The provider falls back to reading Kubernetes `Node` objects directly, filtering by `spec.providerID` prefix `rafay://`.

**No drift detection.** `IsDrifted` always returns empty, deferring node lifecycle management entirely to Rafay's control plane.

**No admission webhooks.** There are no mutating or validating webhooks in this provider.

**Parity with `edge-client`.** The TLS certificate path, host extraction from cert OU, port environment variables, and session ID generation all mirror `edge-client` behavior so the same certificate material can be reused without additional configuration.

**Provider ID format:** `rafay://clusterID/nodeID` — parsed by `ParseProviderID` to recover the cluster and node identifiers from any Kubernetes `Node.spec.providerID` field.
