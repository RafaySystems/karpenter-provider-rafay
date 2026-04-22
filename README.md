# Karpenter Provider for Rafay (Private Cloud)

This provider integrates [Karpenter](https://karpenter.sh) with Rafay for private cloud. When Karpenter needs to provision or deprovision nodes, it **sends commands to edge-broker over gRPC** (same path as `edge-client`).

## Prerequisites

- Go 1.22+
- A Kubernetes cluster with `KUBECONFIG` set
- TLS client material for edge-broker (`client.crt`, `client.key`, `ca.crt`) — same as edge-client; optional **`STREAM_ID`** / **`EDGE_ID`** (see [environment variables](#2-set-environment-variables))

## Running locally

### 1. Install CRDs

Install Karpenter core CRDs (NodeClaim, NodePool from upstream **v1.11.1**, matching `go.mod`) and the Rafay `RafayNodeClass` CRD:

```bash
kubectl apply -f config/crd/
```

To refresh upstream CRDs from a newer Karpenter release, replace `karpenter.sh_*.yaml` in `config/crd/` from `kubernetes-sigs/karpenter` at `pkg/apis/crds/` for that tag.

### 2. Set environment variables

All traffic uses **edge-broker** (no HTTP API mode).

| Variable | Description |
|----------|-------------|
| `CERT_FOLDER` or `EDGE_CLIENT_CERT_FOLDER` | Directory with `client.crt`, `client.key`, `ca.crt` (**required** for default TLS dial; same as edge-client). |
| `SERVER_PORT` or `EDGE_CLIENT_SERVER_PORT` | **TLS** gRPC port (default **5448** if unset). Same as edge-client **`EDGE_CLIENT_SERVER_PORT`**. |
| `EDGE_BROKER_GRPC_INSECURE` | If unset or **`false`**, use **secured** gRPC only (same as edge-client). Set **`true`** only for a plaintext broker listener (e.g. rcloud internal **5449**). |
| `EDGE_BROKER_GRPC_PORT` | Port when **`EDGE_BROKER_GRPC_INSECURE=true`** (default **5449** if unset). |
| `EDGE_BROKER_GRPC_HOST` | Optional gRPC host override when using insecure or TLS; if unset, host comes from **client.crt** OU. |
| `EDGE_ID` | Optional. Broker uses **client.crt** `Subject Organization (O)` for identity (same as edge-client). If set, should match `O`; used for logging. |
| `STREAM_ID` | Optional. gRPC metadata **`sessionid`**. **Unset:** a UUID v4 is generated at startup (same pattern as edge-client `common.NewID()`). **Set** to the value from edge-client logs (`using session`) if your broker requires the same stream id as the running edge-client. |
| `RAFAY_CLUSTER_ID` | Cluster id for add-node payloads (optional on NodeClass override) |
| `RAFAY_PROJECT_ID` | Project id when required (optional) |

For a local run, **`./scripts/run-local.sh`** expects `EDGE_CLIENT_CERT_FOLDER` (or `CERT_FOLDER`) to be set; it sets defaults for `RAFAY_CLUSTER_ID` and broker port only.

### 3. Run the controller

**Option A – Makefile**

```bash
make deps   # first time: go mod tidy
make run
```

**Option B – Script**

```bash
chmod +x scripts/run-local.sh
export EDGE_CLIENT_CERT_FOLDER=/path/to/certs
./scripts/run-local.sh
```

**Option C – Go run**

```bash
export EDGE_CLIENT_CERT_FOLDER=/path/to/certs
go run ./cmd/controller
```

The controller uses your current `KUBECONFIG` to talk to the cluster. For a single-node local cluster you may need to disable leader election (see Karpenter docs for the appropriate flag or env).

### 4. Create a RafayNodeClass and NodePool

After the controller is running, create a `RafayNodeClass` and a Karpenter `NodePool` that references it. See **`examples/nodepool.yaml`** or:

```yaml
apiVersion: karpenter.rafay.io/v1alpha1
kind: RafayNodeClass
metadata:
  name: default
spec:
  clusterID: "my-cluster"
  projectID: "my-project"
---
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: default
spec:
  template:
    spec:
      nodeClassRef:
        group: karpenter.rafay.io
        kind: RafayNodeClass
        name: default
  # limits, disruption, etc.
```

## Building

```bash
make build
# Binary: bin/karpenter-provider-rafay
```

## Docker image

Build and push use the same registry pattern as [edgesrv](https://github.com/RafaySystems/edgesrv):

- **Base image (build):** `registry-proxy.dev.rafay-edge.net/golang:1.24`
- **Dev push target:** `registry.dev.rafay-edge.net/<DEV_USER>/karpenter-provider-rafay:<branch>-<date>-<time>`

```bash
# Build image (default tag: karpenter-provider-rafay:latest)
make docker-build

# Push to dev registry (tags as DEV_TAG and pushes)
make push-it          # after docker-build
make push             # docker-build + push-it

# Override user for dev tag (default: $USER)
DEV_USER=myteam make push
```

Custom tag:

```bash
IMG=registry.dev.rafay-edge.net/myorg/karpenter-provider-rafay:v0.1.0 make docker-build
docker push registry.dev.rafay-edge.net/myorg/karpenter-provider-rafay:v0.1.0
```

The image runs as non-root (`nonroot:nonroot`). Configure the deployment with the same [environment variables](#2-set-environment-variables) (e.g. via a Secret and `envFrom`).

## Deploy

Manifests target **`rafay-system`** (must already exist—e.g. from the platform / `edge-client`). Configure TLS (`edge-client-creds`), **`EDGE_CLIENT_SERVER_PORT`**, **`RAFAY_CLUSTER_ID`** (and optional **`RAFAY_PROJECT_ID`**). **`STREAM_ID`** / **`EDGE_ID`** are optional (see table).

```bash
make deploy
```

`kubectl apply -f config/deploy` applies **every** YAML in that directory. If you still have an old `config/deploy/kustomization.yaml`, remove it (`rm -f config/deploy/kustomization.yaml`) or use **`make deploy`**, which applies only the known manifests.

Kustomize: `kubectl apply -k config` (uses `config/kustomization.yaml`).

Optional Secret for extra env: `config/deploy/secret.example.yaml` (`kubectl apply -n rafay-system`).

## How it works

**Parity with edge-client (no extra platform env in the Deployment):**

| What | edge-client | this controller |
|------|-------------|-----------------|
| Broker host for TLS | `client.crt` **OU** | Same (`GetServerHostFromCert` / dial) |
| Port | `EDGE_CLIENT_SERVER_PORT` | Same env names |
| TLS material | `EDGE_CLIENT_CERT_FOLDER` | `CERT_FOLDER` or `EDGE_CLIENT_CERT_FOLDER` |
| Edge id at broker | **Cert** `Subject O` (mTLS) | Same (broker reads peer cert); optional `EDGE_ID` for logs |
| Session id (`sessionid`) | `common.NewID()` UUID v4 each run | `STREAM_ID` env **or** `broker.NewSessionID()` (same UUID v4 pattern) |

- **Broker gRPC**: The provider keeps a **long-lived `grpc.ClientConn`** using the **same TLS dial as edge-client** (`GetEdgeClientCredentials` + secure gRPC to cert OU host and `EDGE_CLIENT_SERVER_PORT`) unless **`EDGE_BROKER_GRPC_INSECURE=true`**. Node create/delete use the **same** **`EdgeCommandService.Execute`** bidi stream as edge-client (`/rep.edge.v1.EdgeCommandService/Execute`), with **`sessionid`** metadata from **`STREAM_ID`** or an **auto-generated UUID** (edge-client pattern: `common.NewID()` per process). Each add/remove opens a short stream, sends **`EdgeCommand`** (Karpenter request), and reads the matching response.
- **Create**: When Karpenter creates a `NodeClaim`, the provider sends **KarpenterAddNodeRequest** on that stream and receives provider IDs (`rafay://cluster-id/node-id`). The NodeClaim is updated with that provider ID.
- **Delete**: When Karpenter deletes a NodeClaim, the provider sends **KarpenterRemoveNodeRequest** for that provider ID.
- **List / Get**: There is no separate cloud list API over the broker; the provider **lists Kubernetes Nodes** by `spec.providerID` so garbage collection and reconciliation see in-cluster state.

Rafay is responsible for actually adding/removing nodes; this provider drives that through **edge-broker** and **edge-client**.

## Troubleshooting

- **`no matches for kind "Kustomization"` when applying `config/deploy`** — Delete `config/deploy/kustomization.yaml` if present (Kustomize belongs in `config/kustomization.yaml`), or run **`make deploy`** instead of `kubectl apply -f config/deploy`.
- **`Index with name field:status.providerID does not exist`** — Apply `config/crd/` **before** the controller runs so Karpenter can register NodeClaim field indexes. If CRDs were added later, **restart** the controller Deployment.
- **`set CERT_FOLDER or EDGE_CLIENT_CERT_FOLDER`** — Mount TLS and set the cert directory env (same as edge-client; see `config/deploy/deployment.yaml`).
- **`no nodepools found`** — Create a Karpenter `NodePool` that references your `RafayNodeClass` (see [Create a RafayNodeClass and NodePool](#4-create-a-rafaynodeclass-and-nodepool)).
- **`ignoring nodepool, not ready`** — The `NodePool` stays inactive until the referenced `RafayNodeClass` has **`Ready=True`** on its status. The controller runs a **`rafaynodeclass.readiness`** reconciler that sets this; upgrade to a build that includes it, or patch `RafayNodeClass` status manually if needed.
- **`rafay.drift.validate` denied creating leader election Lease** — The controller runs in `rafay-system` but stores the Lease in namespace **`karpenter`** (`LEADER_ELECTION_NAMESPACE`, see `namespace-karpenter-leader.yaml`). Apply that manifest and restart the pod. Single-replica alternative: set **`DISABLE_LEADER_ELECTION=true`** (no Lease, not for HA).
- **`rpc error: code = Unimplemented desc = unknown service rep.edge.v1.EdgeCommandService`** — The broker must expose **`EdgeCommandService`** on the TLS endpoint (same as edge-client). If only a plaintext listener exposes it, set **`EDGE_BROKER_GRPC_INSECURE=true`**, **`EDGE_BROKER_GRPC_PORT`**, and optionally **`EDGE_BROKER_GRPC_HOST`**.
- **`NO CLIENT ID`** — Edge-broker uses **`sessionid`** metadata **and** the **mTLS client certificate** **Subject Organization (`O`)** (`GetEdgeClientInfo` in edge-common). Fix: ensure **`client.crt`** includes **`O=<edge-id>`**; use a path where **mTLS reaches the broker**. If routing requires the **same** session as edge-client, set **`STREAM_ID`** from edge-client logs (`using session`); otherwise the controller generates a session id like edge-client does at startup.
