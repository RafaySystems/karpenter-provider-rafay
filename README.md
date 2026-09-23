# Karpenter Provider for Rafay (Private Cloud)

This provider integrates [Karpenter](https://karpenter.sh) with Rafay for private cloud. When Karpenter needs to provision or deprovision nodes, it **sends commands to edge-broker over gRPC** (same path as `edge-client`).

## Prerequisites

- Go 1.22+
- A Kubernetes cluster with `KUBECONFIG` set
- TLS client material for edge-broker (`client.crt`, `client.key`, `ca.crt`) — same as edge-client; optional **`STREAM_ID`** / **`EDGE_ID`** (see [environment variables](#2-set-environment-variables))

## Running locally

### 1. Install CRDs

Install Karpenter core CRDs (NodeClaim, NodePool from upstream **v1.14.1**, matching `go.mod`) and the Rafay `RafayNodeClass` CRD:

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
| `EDGE_ID` | Optional. For logs we use the **first DNS label** of `Subject Organization (O)` (`strings.Split(O, ".")[0]`, same as edge hash id). If set, it is normalized the same way and should match that label; the broker still reads the **full** `O` from the mTLS peer cert. |
| `STREAM_ID` | Optional. gRPC metadata **`sessionid`**. **Unset:** a UUID v4 is generated at startup (same pattern as edge-client `common.NewID()`). **Set** to the value from edge-client logs (`using session`) if your broker requires the same stream id as the running edge-client. |
| `RAFAY_CLUSTER_ID` | Cluster id stamped on add/remove-node payloads. The only source — `RafayNodeClass` has no per-class override. |
| `RAFAY_PROJECT_ID` | Project id when required (optional) |
| `KARPENTER_CONFIG_BOOTSTRAP` | Default **`true`**: fetch this cluster's `RafayNodeClass` / `NodePool` objects from edge-broker and apply them (see [Node pool bootstrap](#node-pool-bootstrap-from-edge-broker)). Set **`false`** when the objects are managed by hand or by GitOps. |
| `KARPENTER_CONFIG_SYNC_INTERVAL` | How often to re-fetch that config after the first success (Go duration, default **`10m`**). |
| `KARPENTER_ADOPT_EXISTING_NODES` | Default **`true`**: create a `NodeClaim` for each worker node the platform built before Karpenter ran, so a pool's existing nodes count towards its limits and can be consolidated (see [Adopting the cluster's existing worker nodes](#5-adopting-the-clusters-existing-worker-nodes)). Set **`false`** to leave them outside Karpenter's control. |

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

### 4. Node pool bootstrap from edge-broker

**You normally do not have to write `RafayNodeClass` / `NodePool` manifests at all.**

On startup the controller calls **`rep.edge.v1.KarpenterConfigService.GetKarpenterConfig`** on edge-broker. The broker reads the cluster's workspace compute instance — the same **"Worker Node Pool"** catalog that scale-out and scale-in mutate — plus the `ComputeProfile` behind each node SKU, and returns ready-to-apply manifests:

| PaaS | Kubernetes |
|------|------------|
| catalog row `poolname` | `NodePool.metadata.name` |
| catalog row `skuname` | `RafayNodeClass.metadata.name` and `spec.instanceTypes[0].name` |
| catalog row `labels` / `annotations` / `taints` | `NodePool.spec.template` metadata and taints |
| `ComputeProfile` `ocpus` / `memory_in_gbs` / `shape` | instance type `cpu` / `memory` / `architectures` |

The controller server-side-applies them (node classes first), labels each object **`karpenter.rafay.io/managed-by=edge-broker`**, records the broker's config revision in **`karpenter.rafay.io/config-revision`**, and re-syncs every `KARPENTER_CONFIG_SYNC_INTERVAL` so a pool added or resized in the catalog reaches the cluster without a restart.

```bash
kubectl get nodepools,rafaynodeclasses -l karpenter.rafay.io/managed-by=edge-broker
```

Behavior worth knowing:

- **Pool names are a contract.** `NodePool.metadata.name` must equal the catalog `poolname`, because the provider sends the `karpenter.sh/nodepool` label as `node_pool_name` when it asks the broker to scale. Renaming a generated NodePool breaks scale-out.
- **Autoscaling toggle is honoured.** If the compute instance has **Auto Scaling** set to `false`, the controller logs it and applies nothing. Objects already applied are left in place.
- **Nothing is ever pruned.** A pool that disappears from the broker's response is logged, not deleted — the broker also drops a pool when its node SKU cannot be read, and deleting a `NodePool` drains every node under it. Removing a pool stays a deliberate operator action.
- **Applies are forced.** Server-side apply runs with `force: true`, so a field previously owned by a hand-run `kubectl apply` is taken over rather than wedging every resync on a conflict. Use `KARPENTER_CONFIG_BOOTSTRAP=false` on clusters where the broker should not be the owner.
- **Warnings are surfaced.** A SKU whose `ComputeProfile` cannot be read makes the broker drop just that pool and return a warning, which the controller logs on every sync.

### 5. Adopting the cluster's existing worker nodes

**A pool's nodes usually exist before Karpenter does.** The cluster is created with the node count from its "Worker Node Pool" catalog — `pool1` has 3 nodes — and those nodes carry the pool's identity as labels (`nodepoolname=pool1`, `sku_name=oci-inst`) but no `NodeClaim`. Karpenter only counts NodeClaims, so without help it would see an **empty** pool: it would provision up to the pool's full `limits` on top of the 3 nodes already running, and it could never consolidate them (`ValidateNodeDisruptable` rejects a node with no NodeClaim: *"node isn't managed by karpenter"*).

So when a `NodePool` appears — from the bootstrap above or applied by hand — the controller counts the pool's nodes, works out which of them no NodeClaim owns, and creates one per node:

```bash
$ kubectl logs -n rafay-system deploy/karpenter-provider-rafay | grep nodeadoption
nodeadoption: prepared node test-auto-2-e-e949w9-w0-af1cc for adoption (providerID=rafay://pool1/oci-inst/test-auto-2-e-e949w9-w0-af1cc)
nodeadoption: adopted node test-auto-2-e-e949w9-w0-af1cc into pool "pool1" as nodeclaim pool1-x4k2p (providerID=rafay://pool1/oci-inst/test-auto-2-e-e949w9-w0-af1cc, instanceType=oci-inst)
nodeadoption: pool "pool1" has 3 node(s) in the cluster and 0 nodeclaim(s) (3 worker node(s) across all pools); adopted 3, skipped 0

$ kubectl get nodeclaims -L karpenter.sh/nodepool
NAME           TYPE       NODE                             READY   AGE   NODEPOOL
pool1-x4k2p    oci-inst   test-auto-2-e-e949w9-w0-af1cc    True    30s   pool1
```

Each node is tied to its NodeClaim by **`spec.providerID`**, which the controller fills in when the platform left it empty, using the format the platform itself writes:

```yaml
spec:
  providerID: rafay://pool1/oci-inst/test-auto-2-e-e949w9-w0-af1cc
#              rafay://<nodepoolname>/<sku_name>/<hostname>
```

Behaviour worth knowing:

- **No node is provisioned by adoption.** The NodeClaim is annotated `karpenter.rafay.io/adopted-provider-id`, and `CloudProvider.Create()` treats that as "this machine is already running" — it returns the annotated ProviderID without calling edge-broker. Nothing is added to the catalog and `noOfSku` does not change.
- **Adopted nodes become disruptable.** That is the point — the pool becomes Karpenter's to size — but it means a pre-existing node that goes empty can be consolidated away after `consolidateAfter`. Set **`KARPENTER_ADOPT_EXISTING_NODES=false`** on clusters where the original nodes must stay untouched.
- **Running pods are not disturbed.** The pool's taints are **not** copied onto adopted NodeClaims (Karpenter syncs a NodeClaim's taints onto its node, and a `NoExecute` taint would evict the pods already there), and topology labels are read from the node rather than inferred from the SKU, so real zone information is never overwritten.
- **Only `Ready` nodes are adopted.** A node whose kubelet has not reported `Ready` is left completely untouched — no `providerID`, no NodeClaim — and picked up within seconds of becoming Ready. Adopting one early would charge the pool's limits for capacity nothing can schedule on, and a node that never comes up would leave a NodeClaim that nothing reaps at all (broker-rendered pools set `expireAfter: Never`). A node that goes `NotReady` *after* adoption keeps its NodeClaim, and is not counted as waiting. The log line says how many genuinely are:

  ```
  nodeadoption: pool "pool1" has 3 node(s) in the cluster and 2 nodeclaim(s) (3 worker node(s) across all pools); adopted 2, skipped 0, waiting for 1 node(s) to become Ready
  ```
- **A node Karpenter is still waiting for is left alone.** If a scale-out has an unresolved NodeClaim that this node could satisfy, adoption skips it and lets the ProviderID resolution path bind the two.
- **Mismatches are reported, not forced.** A node whose `sku_name` is not in the pool's `RafayNodeClass`, or whose labels do not satisfy the pool's requirements (an arm64 node in an amd64 pool), is logged as a warning and left unmanaged — a NodeClaim for it would be read as drifted and the node drained.
- **Control-plane nodes are never adopted**, even if they carry a `nodepoolname` label.

### 6. Writing the manifests yourself (optional)

With `KARPENTER_CONFIG_BOOTSTRAP=false`, create a `RafayNodeClass` and a Karpenter `NodePool` that references it. Set **`RAFAY_CLUSTER_ID`** / **`RAFAY_PROJECT_ID`** on the controller per cluster (not in the CR) so the same YAML can be reused everywhere. See **`examples/nodepool.yaml`** or:

```yaml
apiVersion: karpenter.rafay.io/v1alpha1
kind: RafayNodeClass
metadata:
  name: default
spec: {}
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
make compile   # binary: bin/karpenter-provider-rafay
make build     # container image (see "Docker image" below)
```

### Dependencies

The build is self-contained: a fresh clone — including the Jenkins image build, which only sees the clone — needs nothing outside this repository except GitHub credentials for the private `github.com/RafaySystems/*` modules.

- **`github.com/RafaySystems/edge-common`** is pinned in `go.mod` to a commit pseudo-version, the same commit that `edge-broker` pins, so both ends of the Karpenter batch protocol are generated from one proto definition. Bump it with `make update-deps` (tracks `main`) or `GOPRIVATE=github.com/RafaySystems/* go get github.com/RafaySystems/edge-common@<sha>`, and keep it in step with `edge-broker/go.mod`.
- **`sigs.k8s.io/karpenter`** is replaced in `go.mod` by the Rafay fork [github.com/RafaySystems/karpenter-rafay](https://github.com/RafaySystems/karpenter-rafay), branch `rafay-release-v1.14.x`: upstream v1.14.1 plus one commit that raises the NodeClaim `registrationTimeout` from 15 to 60 minutes, which upstream does not expose as a setting ([kubernetes-sigs/karpenter#357](https://github.com/kubernetes-sigs/karpenter/issues/357)). The replace pins a commit pseudo-version. To move to a newer fork commit:

  ```bash
  go mod edit -replace sigs.k8s.io/karpenter=github.com/RafaySystems/karpenter-rafay@rafay-release-v1.14.x   # or @<sha>
  GOPRIVATE=github.com/RafaySystems/* go mod tidy   # rewrites the branch name as a commit pseudo-version
  ```

  Then refresh `config/crd/karpenter.sh_*.yaml` from the fork's `pkg/apis/crds/` (keep the header and printer-column tweaks noted in those files). Never point the replace at a sibling checkout; CI cannot see one.

To iterate on unpushed edge-common changes, use a Go workspace instead of editing `go.mod` (`go.work` is git-ignored):

```bash
go work init . ../edge-common   # go build / go test now use the sibling checkout
rm go.work go.work.sum          # back to the pinned version
```

`vendor/` (`make vendor`) is git-ignored and not used by the image build. If you keep one, refresh it with `make vendor` after every `go.mod` change, and remove it while a `go.work` is in place — workspace mode and a `go mod vendor` tree do not mix.

## Docker image

Build and push use the same registry pattern as [edgesrv](https://github.com/RafaySystems/edgesrv):

- **Base image (build):** `registry-proxy.dev.rafay-edge.net/golang:1.26-alpine` (must satisfy the `go` directive in `go.mod`; the image sets `GOTOOLCHAIN=local`). The Alpine version is left floating on purpose: Alpine-pinned tags such as `1.26-alpine3.22` stop being rebuilt once that Alpine release ages out and then fall behind the Go patch level the `go` directive needs.
- **Build inputs:** the repository checkout plus the `BUILD_USR`/`BUILD_PWD` build args (GitHub credentials for the private modules). These are exactly what the Jenkins `buildAndScan`/`pushImage` library passes, so the `Jenkinsfile` build needs no extra `--build-context`.
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
