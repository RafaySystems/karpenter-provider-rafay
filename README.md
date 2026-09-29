# Karpenter Provider for Rafay (Private Cloud)

This provider integrates [Karpenter](https://karpenter.sh) with Rafay for private cloud. When Karpenter needs to provision or deprovision nodes, it **sends batched add/remove requests to edge-broker over mTLS gRPC** (`KarpenterBatchService.BatchStreamOperations`, using the same certificate material and dial conventions as `edge-client`).

## Prerequisites

- Go 1.26.6+ (the `go` directive in `go.mod`)
- A Kubernetes cluster with `KUBECONFIG` set
- TLS client material for edge-broker (`client.crt`, `client.key`, `ca.crt`) — same as edge-client; optional **`STREAM_ID`** / **`EDGE_ID`** (see [environment variables](#2-set-environment-variables))

## Running locally

### 1. Install CRDs

Install Karpenter core CRDs (NodeClaim, NodePool from the Rafay fork of Karpenter **v1.14.1**, matching `go.mod`) and the Rafay `RafayNodeClass` CRD:

```bash
kubectl apply -f config/crd/
```

To refresh the Karpenter CRDs after moving the fork replace in `go.mod`, copy `karpenter.sh_*.yaml` from the fork's `pkg/apis/crds/` at that commit and keep the header and printer-column tweaks noted in those files (see [Dependencies](#dependencies)). `karpenter.rafay.io_rafaynodeclasses.yaml` is hand-maintained together with `pkg/apis/v1alpha1` (`TestMsCRDSchemaMatchesGoTypes` pins the two together); the copies under `infra-states` must mirror `config/crd`.

### 2. Set environment variables

All traffic uses **edge-broker** (no HTTP API mode).

| Variable | Description |
|----------|-------------|
| `CERT_FOLDER` or `EDGE_CLIENT_CERT_FOLDER` | Directory with `client.crt`, `client.key`, `ca.crt` (**required** for default TLS dial; same as edge-client). |
| `SERVER_PORT` or `EDGE_CLIENT_SERVER_PORT` | **TLS** gRPC port (default **5448** if unset). Same as edge-client **`EDGE_CLIENT_SERVER_PORT`**. |
| `EDGE_BROKER_GRPC_INSECURE` | If unset or **`false`**, use **secured** gRPC only (same as edge-client). ⚠️ **`true` cannot serve node operations**: the broker derives the edge id from the mTLS client certificate, and a `BatchStreamOperations` stream on its plaintext internal listener (`5449`) is rejected with `NO CLIENT ID`. Keep `false`. |
| `EDGE_BROKER_GRPC_PORT` | Port when **`EDGE_BROKER_GRPC_INSECURE=true`** (default **5449** if unset). |
| `EDGE_BROKER_GRPC_HOST` | Optional gRPC host override when using insecure or TLS; if unset, host comes from **client.crt** OU. |
| `EDGE_ID` | Optional. For logs we use the **first DNS label** of `Subject Organization (O)` (`strings.Split(O, ".")[0]`, same as edge hash id). If set, it is normalized the same way and should match that label; the broker still reads the **full** `O` from the mTLS peer cert. |
| `STREAM_ID` | Optional. gRPC metadata **`sessionid`**. **Unset:** a UUID v4 is generated at startup (same pattern as edge-client `common.NewID()`). **Set** to the value from edge-client logs (`using session`) if your broker requires the same stream id as the running edge-client. |
| `RAFAY_CLUSTER_ID` | Optional. Cluster id stamped on add/remove-node payloads and the config request for diagnostics — the broker identifies the cluster from the mTLS certificate, so provisioning works without it (startup logs an Info line when unset). The only source — `RafayNodeClass` has no per-class override. |
| `RAFAY_PROJECT_ID` | Optional. Project id stamped on the same payloads; the broker derives the project itself and never widens its lookup by this value. |
| `KARPENTER_CONFIG_BOOTSTRAP` | Default **`true`**: fetch this cluster's `RafayNodeClass` / `NodePool` objects from edge-broker and apply them (see [Node pool bootstrap](#4-node-pool-bootstrap-from-edge-broker)). Set **`false`** when the objects are managed by hand or by GitOps. |
| `KARPENTER_CONFIG_SYNC_INTERVAL` | How often to re-fetch and re-apply that config after the first success (Go duration, default **`10m`**). |
| `KARPENTER_ADOPT_EXISTING_NODES` | Default **`true`**: create a `NodeClaim` for each worker node the platform built before Karpenter ran, so a pool's existing nodes count towards its limits and can be consolidated (see [Adopting the cluster's existing worker nodes](#5-adopting-the-clusters-existing-worker-nodes)). Set **`false`** to leave them outside Karpenter's control. |
| `RAFAY_POOL_AT_MAX_COOLDOWN` | Default **`5m`**: how long a NodePool is held back from provisioning after the broker refused an add for a reason no retry can fix (`pool at maximum`, `pool not found`, `pool sku mismatch`, `pool not auto-scaling`, `pool precondition`). `0` disables the hold. |
| `RAFAY_REMOVE_SETTLE_WINDOW` | Default **`60m`** (values below are raised to it): how long `Delete()` waits for the machine behind a removed NodeClaim to actually stop (the platform picks which machine a decrement retires) before concluding it retired a different one. |
| `RAFAY_VM_MEMORY_OVERHEAD_PERCENT`, `RAFAY_KUBE_RESERVED_CPU`, `RAFAY_KUBE_RESERVED_MEMORY` | Instance-type overhead model (defaults **`7.5`**, **`80m`**, **`255Mi`**; plus the kubelet's 100Mi eviction threshold): capacity stays the SKU's nominal size, allocatable is what pods can actually use. Read once at startup; an invalid value keeps the default with a warning. |
| `HEADROOM_NAMESPACE`, `HEADROOM_CONFIG_NAMESPACE` | Namespace of the headroom pause-pod Deployments (default **`karpenter`**) and of the `headroom-policy` ConfigMap (defaults to `HEADROOM_NAMESPACE`). See [docs/headroom.md](docs/headroom.md). |

For a local run, **`./scripts/run-local.sh`** expects `EDGE_CLIENT_CERT_FOLDER` (or `CERT_FOLDER`) to be set; it sets defaults for `RAFAY_CLUSTER_ID` and the broker port, and exports `DISABLE_LEADER_ELECTION=true` (the Karpenter fork's env name; there is no `KARPENTER_` prefix).

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

The controller uses your current `KUBECONFIG` to talk to the cluster. For a single-node local cluster set `DISABLE_LEADER_ELECTION=true` (the script does); otherwise the Lease is written to `LEADER_ELECTION_NAMESPACE`, which the fork defaults to `""` and the Deployment sets to `karpenter`.

### 4. Node pool bootstrap from edge-broker

**You normally do not have to write `RafayNodeClass` / `NodePool` manifests at all.**

On startup the controller calls **`rep.edge.v1.KarpenterConfigService.GetKarpenterConfig`** on edge-broker. The broker reads the cluster's workspace compute instance — the same **"Worker Node Pool"** catalog that scale-out and scale-in mutate — plus the `ComputeProfile` behind each node SKU, and returns ready-to-apply manifests:

| PaaS | Kubernetes |
|------|------------|
| catalog row `poolname` | `NodePool.metadata.name` |
| catalog row `skuname` | `RafayNodeClass.metadata.name` and `spec.instanceTypes[0].name` |
| catalog row `labels` / `annotations` / `taints` | `NodePool.spec.template` metadata and taints |
| `ComputeProfile` `ocpus` / `memory_in_gbs` / `shape` / `gpu_count` | instance type `cpu` / `memory` / `architectures` / `nvidia.com/gpu` (only when `gpu_count` is positive) |
| catalog row `autoScaling` / `minNodeCount` / `maxNodeCount` | live vs inert NodePool; `karpenter.rafay.io/min-nodes` annotation; `spec.limits.nodes` + `karpenter.rafay.io/max-nodes` |

The controller server-side-applies them (node classes first), labels each object **`karpenter.rafay.io/managed-by=edge-broker`**, records the broker's config revision in **`karpenter.rafay.io/config-revision`**, and re-syncs every `KARPENTER_CONFIG_SYNC_INTERVAL` so a pool added or resized in the catalog reaches the cluster without a restart. **Every sync re-applies** even when the revision is unchanged (the revision only lowers the log line to `V(2)`), so a managed object that was deleted or hand-edited is restored within one interval.

```bash
kubectl get nodepools,rafaynodeclasses -l karpenter.rafay.io/managed-by=edge-broker
```

Behavior worth knowing:

- **Pool names are a contract.** `NodePool.metadata.name` must equal the catalog `poolname`, because the provider sends the `karpenter.sh/nodepool` label as `node_pool_name` when it asks the broker to scale. Renaming a generated NodePool breaks scale-out.
- **Autoscaling is opt-in per catalog row, and the toggle is honoured both ways.** A row with `autoScaling: true` renders a live NodePool; any other row renders **inert** (`spec.limits.nodes: "0"`, an all-reasons `nodes: "0"` disruption budget, annotation `karpenter.rafay.io/auto-scaling: "false"`), so the pool is visible in `kubectl get nodepools` but can neither scale out nor have its nodes touched. When no row opts in the broker reports `auto_scaling: false` and still ships every pool in the inert shape; the controller applies it, so a previously live NodePool is neutralised on the next sync rather than left scaling. Nodes already provisioned are left in place.
- **Expiry is off.** Every rendered NodePool carries `spec.template.spec.expireAfter: Never` — the platform, not Karpenter, owns machine lifecycle, and a removal is an untargeted count decrement, so age-based rotation would only churn healthy machines.
- **Drift is observed, never acted on.** Live pools render `spec.disruption.budgets: [{nodes: "10%"}, {nodes: "0", reasons: [Drifted]}]`. A catalog edit to labels, annotations, taints or the SKU still marks the pool's NodeClaims `Drifted`, but Karpenter never rolls them; such edits reach existing machines only through the platform or an explicit operator delete of the NodeClaim.
- **Nothing is ever pruned.** A pool that disappears from the broker's response is logged, not deleted — the broker also drops a pool when its node SKU cannot be read, and deleting a `NodePool` drains every node under it. Removing a pool stays a deliberate operator action.
- **Applies are forced.** Server-side apply runs with `force: true`, so a field previously owned by a hand-run `kubectl apply` is taken over rather than wedging every resync on a conflict. Use `KARPENTER_CONFIG_BOOTSTRAP=false` on clusters where the broker should not be the owner.
- **Warnings are surfaced.** A SKU whose `ComputeProfile` cannot be read, a pool or SKU name that is not a valid label value, or a catalog label that contradicts a rendered requirement makes the broker drop just that pool (or label) and return a warning, which the controller logs on every sync.
- **"No config" is quiet, everything else retries.** A `NotFound` from the broker that carries `karpenter config unavailable for this edge` means the cluster has no autoscaling source yet; the controller re-checks at the sync interval. Any other error (broker unreachable, misconfiguration) is retried on a 5 s → 2 min backoff.

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
- **Only `Ready` nodes are adopted.** A node whose kubelet has not reported `Ready` is left completely untouched — no `providerID`, no NodeClaim — and picked up within seconds of becoming Ready (the Node watch fires on label, `spec.providerID` and Ready-condition changes; plain heartbeats are ignored). Adopting one early would charge the pool's limits for capacity nothing can schedule on, and a node that never comes up would leave a NodeClaim that nothing reaps at all (broker-rendered pools set `expireAfter: Never`). A node that goes `NotReady` *after* adoption keeps its NodeClaim, and is not counted as waiting. The log line says how many genuinely are:

  ```
  nodeadoption: pool "pool1" has 3 node(s) in the cluster and 2 nodeclaim(s) (3 worker node(s) across all pools); adopted 2, skipped 0, waiting for 1 node(s) to become Ready
  ```
- **A node Karpenter is still waiting for is left alone.** If a scale-out has an unresolved NodeClaim that this node could satisfy, adoption skips it and lets the ProviderID resolution path bind the two.
- **Mismatches are reported, not forced.** A node whose `sku_name` is not in the pool's `RafayNodeClass`, or whose labels do not satisfy the pool's requirements (an arm64 node in an amd64 pool), is logged as a warning and left unmanaged — a NodeClaim for it would be read as drifted and the node drained.
- **Adopted NodeClaims are exempt from static drift, durably.** They carry `karpenter.sh/nodepool-hash-version` but never `karpenter.sh/nodepool-hash`; if Karpenter back-fills the hash (a hash-version bump, a `kubectl replace` of the NodePool), the next adoption pass strips it again.
- **A node that re-registers is recognised.** If a Node comes back with an empty `spec.providerID` (kubelet restart, platform re-registration) and a NodeClaim already claims the ID adoption would build for it, the ID is stamped back onto the Node instead of creating a second NodeClaim.
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
- **Build inputs:** the repository checkout plus GitHub credentials for the private modules — either a mounted `--secret id=netrc,src=~/.netrc` (preferred; `make build` passes it automatically when `~/.netrc` exists and `BUILD_USER` is unset) or the `BUILD_USR`/`BUILD_PWD` build args, which are exactly what the Jenkins `buildAndScan`/`pushImage` library passes, so the `Jenkinsfile` build needs no extra `--build-context`. The secret wins when both are present; the netrc is deleted inside the same `RUN`, so no builder layer keeps it.
- **Tests gate the image:** the `go test` step fails the amd64 image build on a failure (other target platforms print "skipping unit tests"). The build context includes `config/crd` because `TestMsCRDSchemaMatchesGoTypes` reads the CRD.
- **`Dockerfile.fips`** (`USE_FIPS_NODE=true` in the `Jenkinsfile`) builds on ubi9 with Red Hat's go-toolset and `GOTOOLCHAIN=local`; it gates on the toolset satisfying `go.mod`'s `go 1.26.6` before downloading anything. As of 2026-09 no go-toolset ships that version, so FIPS builds fail at the gate with a clear message rather than pulling a non-FIPS toolchain.
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

Manifests target **`rafay-system`** (must already exist—e.g. from the platform / `edge-client`). Configure TLS (`edge-client-creds`) and **`EDGE_CLIENT_SERVER_PORT`**; **`RAFAY_CLUSTER_ID`** / **`RAFAY_PROJECT_ID`** / **`STREAM_ID`** / **`EDGE_ID`** are optional (see table). The Deployment runs under the restricted Pod Security Standard (non-root, read-only root filesystem, no capabilities), exposes `/healthz` and `/readyz` on port 8081 (readiness fails while the Karpenter CRDs are missing), and sets `MEMORY_LIMIT` from its own memory limit so Karpenter's Go GC soft limit tracks the cgroup. `rbac.yaml` carries a ClusterRole scoped to what the code performs (no Secrets, no ServiceAccounts, lease reads only) plus a `karpenter-provider-rafay-leader-election` Role in the `karpenter` namespace for the election Lease — change `LEADER_ELECTION_NAME` / `LEADER_ELECTION_NAMESPACE` and that Role has to follow. The copies under `infra-states` must mirror these files.

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

- **Broker gRPC**: The provider keeps a **long-lived `grpc.ClientConn`** using the **same TLS dial as edge-client** (`GetEdgeClientCredentials` + secure gRPC to cert OU host and `EDGE_CLIENT_SERVER_PORT`). Node add/remove, status polls and cancels each open a **short-lived `rep.edge.v1.KarpenterBatchService.BatchStreamOperations` stream** (30 s deadline) with **`sessionid`** metadata from **`STREAM_ID`** or an **auto-generated UUID**; the cluster's NodePool manifests come from the unary `KarpenterConfigService.GetKarpenterConfig`. A transport failure is retried once on the same connection for polls/cancels/config and never for batch sends (a re-send could duplicate a catalog mutation); the connection is never closed on an error.
- **Create**: When Karpenter creates a `NodeClaim`, the provider enqueues a node add (`operation_id` = NodeClaim UID) into a batch (up to 10 nodes / 10 s window) and sends `batch_add`. `Create()` returns as soon as the broker **ACKs** the batch, with the synthetic ProviderID `rafay://pending/<uid>`. The broker raises the pool's count in the platform catalog; when the machine joins, the `NodeProviderIDController` patches the real ProviderID (`rafay://<nodepoolname>/<sku_name>/<hostname>`) from the Node and Karpenter registers it. A broker-reported FAILED add deletes the pending NodeClaim so Karpenter reprovisions in seconds; a permanent refusal (`pool at maximum`, `pool not found`, …) first holds the NodePool back and records a `PoolAtPlatformMaximum` / `PoolRefusedByPlatform` Warning event on it.
- **Delete**: When Karpenter terminates a NodeClaim (after draining the Node), the provider sends `batch_remove` (`<uid>-remove`, carrying the ProviderID) and returns at ACK; Karpenter re-invokes `Delete()` every 5 s and it converges on `NodeClaimNotFoundError` once the broker reports the remove SUCCEEDED and the machine is actually gone (the platform picks which machine a decrement retires, so the provider waits — up to `RAFAY_REMOVE_SETTLE_WINDOW` — for the NodeClaim's own Node to disappear or go NotReady). A NodeClaim whose Node already vanished, or whose add never produced a machine, is finalized without sending a remove. Events `NodeNotRetired`, `NodeRetiredExternally` and `RemoveRetiredOtherMachine` on the NodePool explain the odd cases.
- **List / Get**: There is no cloud list API over the broker; the provider **lists Kubernetes Nodes** whose `spec.providerID` starts with `rafay://` (no cluster filter — an empty list would make Karpenter's garbage collector delete healthy NodeClaims) so garbage collection and reconciliation see in-cluster state.

Rafay is responsible for actually adding/removing nodes; this provider drives that through **edge-broker**. See [docs/architecture.md](docs/architecture.md) for the full picture and [docs/crash_safety.md](docs/crash_safety.md) for the failure analysis.

## Troubleshooting

- **`no matches for kind "Kustomization"` when applying `config/deploy`** — Delete `config/deploy/kustomization.yaml` if present (Kustomize belongs in `config/kustomization.yaml`), or run **`make deploy`** instead of `kubectl apply -f config/deploy`.
- **`Index with name field:status.providerID does not exist`** — Apply `config/crd/` **before** the controller runs so Karpenter can register NodeClaim field indexes. If CRDs were added later, **restart** the controller Deployment.
- **`set CERT_FOLDER or EDGE_CLIENT_CERT_FOLDER`** — Mount TLS and set the cert directory env (same as edge-client; see `config/deploy/deployment.yaml`).
- **`no nodepools found`** — With the default bootstrap the broker renders the NodePools from the catalog (see [Node pool bootstrap](#4-node-pool-bootstrap-from-edge-broker)); check the controller log for `nodeconfig:` lines. With `KARPENTER_CONFIG_BOOTSTRAP=false`, create a Karpenter `NodePool` that references your `RafayNodeClass` (see [Writing the manifests yourself](#6-writing-the-manifests-yourself-optional)).
- **`ignoring nodepool, not ready`** — The `NodePool` stays inactive until the referenced `RafayNodeClass` has **`Ready=True`** on its status. The controller runs a **`rafaynodeclass.readiness`** reconciler that sets this; upgrade to a build that includes it, or patch `RafayNodeClass` status manually if needed.
- **`rafay.drift.validate` denied creating leader election Lease** — The controller runs in `rafay-system` but stores the Lease in namespace **`karpenter`** (`LEADER_ELECTION_NAMESPACE`, see `namespace-karpenter-leader.yaml`). Apply that manifest and restart the pod. Single-replica alternative: set **`DISABLE_LEADER_ELECTION=true`** (no Lease, not for HA).
- **`rpc error: code = Unimplemented desc = unknown service rep.edge.v1.KarpenterBatchService`** — The broker is too old to serve the batch protocol. Upgrade edge-broker; there is no fallback transport. Do **not** point the provider at the plaintext internal listener (`EDGE_BROKER_GRPC_INSECURE=true`, `:5449`): `KarpenterBatchService` is registered there but every stream is rejected with `NO CLIENT ID`, because the broker derives the edge id from the mTLS client certificate and a plaintext peer has none.
- **`NO CLIENT ID`** — Edge-broker identifies the caller from the **mTLS client certificate** **Subject Organization (`O`)** (`GetEdgeClientInfo` in edge-common). Fix: ensure **`client.crt`** includes **`O=<edge-id>`** and use a path where **mTLS reaches the broker** (`EDGE_BROKER_GRPC_INSECURE` unset). `STREAM_ID` only matters for routing: set it from edge-client logs (`using session`) if the broker must see the same session as the running edge-client; otherwise the controller generates one at startup.
- **NodePool event `PoolRefusedByPlatform` / `PoolAtPlatformMaximum`** — The broker refused an add for a reason a retry cannot fix (the message is the broker's detail: pool at its `maxNodeCount`, pool not in the catalog, SKU mismatch, row not opted into autoscaling, missing PaaS profile). The pool is held back for `RAFAY_POOL_AT_MAX_COOLDOWN`; fix the catalog and the next sync lifts it.
- **NodePool event `NodeNotRetired`** — A remove converged but the platform refused the count change (pool at its `minNodeCount`, unknown pool, …). The machine keeps running without a Node object: restart its kubelet or re-register it from the platform and the adoption controller adopts it back.
- **NodeClaim stuck `Terminating` with event `RemoveRetiredOtherMachine`** — The platform retired a different machine than the one Karpenter drained (removes are untargeted). The drained Node is kept until its machine is gone; once it has been retired or re-registered, remove the Node's `karpenter.sh/termination` finalizer.
