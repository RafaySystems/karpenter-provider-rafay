# Headroom Controller

`pkg/controllers/headroom/` — proactive scale-out via per-pool overprovisioning Deployments of
low-priority pause pods (the proven cluster-overprovisioner pattern).

## Why Headroom Exists

Node provisioning through the Rafay PaaS takes ~10 minutes and the broker serializes catalog
updates per cluster. Purely reactive scaling (pod Pending → provision) therefore leaves real
workloads waiting 10+ minutes. The headroom controller keeps a configurable buffer of capacity
pre-reserved by placeholder pods, so real workloads land **immediately** (by preempting a
placeholder) while the buffer is replenished in the background.

```
Real workload arrives (priority ≥ 0)
  → kube-scheduler preempts a headroom pod (priority -1000)   ← workload runs NOW
  → ReplicaSet replaces the evicted pod; replacement goes Pending
  → its nodeSelector (karpenter.sh/nodepool=<pool>) makes Karpenter's
    provisioner create a NodeClaim in exactly that pool
  → ~10 min later the new node joins and the buffer is restored
```

## Buffer Model

Per pool, per resource:

```
buffer   = fraction × Σ allocatable of the pool's READY nodes
replicas = max over configured resources of ceil(buffer / per-pod size)
replicas = max(replicas, minPods)        ← cold-start floor
replicas = clamp(replicas, 0, 5000)      ← maxHeadroomReplicas
```

- The buffer is denominated in **absolute per-pod slices** (`podCPU`/`podMemory`/`podGPU`,
  defaults 500m / 512Mi / none), NOT derived from node size — preempting one pod frees a
  predictable amount of capacity.
- Pod requests == limits (Guaranteed QoS); each pod holds exactly its slice.
- Only **Ready** nodes count toward the allocatable total, so nodes still joining don't inflate
  the buffer.
- With zero ready nodes the buffer is zero and replicas == `minPods` exactly. This is the
  **cold start** path: an empty pool with `minPods: 2` gets two Pending pause pods, which make
  Karpenter provision the pool's first nodes before any real workload arrives.
- The replica count is **clamped to `[0, 5000]`** (`maxHeadroomReplicas`). The per-pod size is the
  divisor of the replica math, so a typo there (or an absurd `minPods`) could otherwise ask for a
  runaway number of pause pods; the clamp turns that into a loud warning and a bounded pod count
  instead of an `int32` overflow (negative replicas, rejected by the API server) or a pod flood.
  Per-pod sizes below `1m` CPU / `1Mi` memory are rejected at parse time for the same reason.
- GPU buffers have no default per-pod size — `gpu:` without `podGPU:` is ignored.

### GPU headroom: reserve-only, cannot cold-start a pool

> ⚠️ A GPU buffer reserves capacity on the pool's **EXISTING** GPU nodes. It **cannot cold-start a
> GPU pool.**

The headroom controller reads GPU capacity off the pool's nodes, never off the NodeClass, so a pool
with no GPU nodes yet gives it nothing to size a buffer against. Consequences:

- The GPU extended-resource name is **derived from the GPU nodes the pool already has** (all nodes
  of a Rafay pool share one SKU, so at most one of `nvidia.com/gpu` / `amd.com/gpu` is present).
- On a pool with **no** GPU nodes, the `podGPU` request is **dropped with a warning** rather than
  guessed. A guessed request would only strand the pause pod in `Pending` forever — and on an
  `amd.com/gpu` pool, guessing `nvidia.com/gpu` would be wrong outright.
- To **cold-start** a GPU pool, use `minPods` with a cpu/memory-sized pod. The GPU buffer then
  applies once GPU nodes exist.

> Instance types now advertise a **temporary** hardcoded `nvidia.com/gpu` capacity, so Karpenter
> itself *can* provision for a pending GPU pod (see the RafayNodeClass reference in
> [architecture.md](architecture.md#rafaynodeclass-crd-portable--same-manifest-on-every-cluster)).
> The headroom controller is deliberately left keyed off existing nodes; when that temporary field is
> replaced by real per-SKU accelerator capacity, sourcing `gpuKey` from the instance type would let a
> GPU buffer drive scale-out as well.

### Per-pod sizing trade-off

This was the main design decision (many small pods vs few large pods):

| Aspect | Small pods (e.g. 500m) | Large pods (e.g. 2.5 CPU) |
|---|---|---|
| Scheduling flexibility | High | Low |
| Preemption precision | Fine | Coarse |
| Bin-packing efficiency | Good | Poor |
| Risk of over-scaling | Low | High |
| Operational simplicity | Medium | High |

The controller doesn't pick a side: per-pod size is configurable per pool, with small defaults.
Size pods at (or slightly above) the typical request of the workloads you expect to burst —
preempting one placeholder then frees exactly one workload's worth of capacity.

## Deployment Pattern

For every configured pool the controller maintains one Deployment `headroom-<pool>` in
`HEADROOM_NAMESPACE`:

- labels `rafay.io/headroom: "true"` + `rafay.io/headroom-pool: <pool>` (on Deployment and pods);
- pause container `registry.k8s.io/pause:3.9`, `terminationGracePeriodSeconds: 0` (instant
  eviction);
- `priorityClassName: rafay-headroom` — value **-1000** (any normal pod, priority ≥ 0, can preempt)
  and `preemptionPolicy: Never` (headroom pods never evict anything themselves). Ensured
  idempotently on **every reconcile**, not just at startup — see [PriorityClass](#priorityclass);
- `nodeSelector: karpenter.sh/nodepool: <pool>` — ties the Pending replacement to a named
  NodePool so the provisioner knows exactly what to scale;
- topology spread on `kubernetes.io/hostname` (maxSkew 1, ScheduleAnyway) — placeholders spread
  across nodes so preempting one frees capacity on the node where the workload wants to land;
- tolerations: **only** the ones listed in the pool's config. There is deliberately NO blanket
  `Exists` toleration — headroom pods must respect taints that real workloads cannot cross,
  otherwise the buffer would reserve capacity the workloads can't use.

Using Deployments (instead of the bare pods an earlier version created directly) delegates
replacement of preempted pods to the ReplicaSet controller — the headroom controller only
manages the replica count. Legacy bare pods (headroom label, no ownerReference) are garbage
collected, as are Deployments of pools removed from the config.

## Configuration

ConfigMap `headroom-policy` in `HEADROOM_CONFIG_NAMESPACE`, data key `policy` (also accepts
`config`, then falls back to scanning the remaining keys for one that yields a non-empty
`pools` list). Config changes apply immediately (the ConfigMap is watched).

**A parse error keeps the buffer up — it never GCs it.** This is the error contract that matters,
because every reconcile garbage-collects the Deployment of any pool *not* in the returned list:

| ConfigMap state | Result | Effect on Deployments |
|---|---|---|
| Absent | `(nil, nil)` | Buffer torn down |
| Parses cleanly, declares **no** pools | `(nil, nil)` | Buffer torn down — a legitimate "headroom off" policy |
| **Fails to parse** (YAML typo, bad quantity, duplicate pool name) | **error** | **Deployments are KEPT.** `Reconcile` returns the error and short-circuits *before* any GC runs |

A YAML typo must never read as "no pools configured": falling through with an empty pool list would
tear down **every** headroom Deployment in the cluster over a single bad character.

Two further parse-time guarantees:

- **Duplicate pool names are rejected.** Two entries for one pool would apply two conflicting specs
  to the same Deployment on every sync.
- **Data keys are scanned in a deterministic order** — `policy`, then `config`, then every other key
  sorted by name. Go map iteration is randomized, so an unsorted fallback scan would let the applied
  policy *flap* between reconciles (churning Deployments) whenever two keys both declare a `pools`
  list. If a second key also declares pools it is ignored, with a warning naming the key that won.

Per pool entry:

| Field | Required | Default | Meaning |
|---|---|---|---|
| `name` | yes | — | Karpenter NodePool `.metadata.name`, exact match |
| `cpu` / `memory` / `gpu` | no | 0 (no buffer) | Buffer fraction of total pool allocatable, `"30%"` or `"30"` (0–100) |
| `podCPU` | no | `500m` | Per-pod CPU slice |
| `podMemory` | no | `512Mi` | Per-pod memory slice |
| `podGPU` | no | none | Per-pod GPU count; **required** for a GPU buffer |
| `minPods` | no | 0 | Replica floor even with zero ready nodes (cold start); must be ≥ 0 |
| `tolerations` | no | none | corev1.Toleration-shaped entries (operator/effect validated) |

Working example (see [examples/policy_configmap.yaml](../examples/policy_configmap.yaml)):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: headroom-policy
  namespace: karpenter
data:
  policy: |
    pools:
      - name: worker-pool-amd    # must match NodePool .metadata.name exactly
        cpu: 30%                 # keep 30% of the pool's total CPU pre-reserved
        memory: 20%
        podCPU: 500m
        podMemory: 512Mi
        minPods: 2               # cold-start floor
      - name: worker-pool-gpu
        cpu: 10%
        memory: 10%
        gpu: 25%                 # GPU buffer requires podGPU below (no default)
        podCPU: "1"
        podMemory: 2Gi
        podGPU: "1"
        tolerations:             # list exactly the taints headroom pods may cross
          - key: nvidia.com/gpu
            operator: Exists
            effect: NoSchedule
```

## REQUIRED: `consolidationPolicy: WhenEmpty`

NodePools with a headroom buffer **must** use `WhenEmpty` consolidation
(see [examples/nodepool.yaml](../examples/nodepool.yaml)):

```yaml
disruption:
  consolidationPolicy: WhenEmpty
  consolidateAfter: 5m
```

Why: pause pods keep buffer nodes permanently non-empty but lightly utilized. With
`WhenEmptyOrUnderutilized`, Karpenter would consolidate a freshly provisioned buffer node away,
the evicted pause pods would go Pending, Karpenter would provision a replacement, and so on —
an endless provision/consolidate oscillation (expensive at 10 min per provision). `WhenEmpty`
only reclaims a node once every pod, including headroom pods, has left it — which happens
naturally when the controller scales the Deployment down (config change, pool shrink).

## Reconciliation

Watch-based reconciler (replaced an earlier 30 s ticker), `MaxConcurrentReconciles: 1`, every
reconcile is a **full sync** of all pools — all triggers funnel into one synthetic request:

- the `headroom-policy` ConfigMap in the config namespace (config changes apply immediately);
- Nodes carrying `karpenter.sh/nodepool` (allocatable totals changed);
- headroom-labeled Deployments in the pod namespace (drift repair / self-heal);
- a 5-minute safety resync (`RequeueAfter`) for anything the watches miss.

Each sync: ensure the PriorityClass, load the policy (**aborting on a parse error, before any GC**),
apply (`CreateOrUpdate`) the Deployment per configured pool, then GC legacy bare pods and
Deployments of unconfigured pools.

**Steady-state reconciles issue no Deployment Updates.** The reconciler mutates only the fields it
owns, **in place on the fetched pod template**. Assigning a whole `PodTemplateSpec` would drop every
field the API server defaults (`restartPolicy`, `dnsPolicy`, `schedulerName`, `securityContext`,
per-container `terminationMessagePath`, …), so the mutated object would differ from the stored one
on *every* reconcile — `CreateOrUpdate` would issue a pointless Update and log "updated" every
5 minutes, forever.

## PriorityClass

The `rafay-headroom` PriorityClass (value **-1000**, `preemptionPolicy: Never`) is ensured on
**every** `Reconcile`, not only from the startup runnable. The startup runnable gets exactly one
attempt, and **without the PriorityClass the pause pods are rejected at admission** — a transient
failure there would silently disable headroom until the next pod restart. The create is idempotent
and cheap (normally a single 409); a failure is tolerated and retried on the next reconcile.

A PriorityClass's `Value` is **immutable**, and the controller holds no `update` verb, so a
pre-existing `rafay-headroom` class with different settings **cannot be repaired** here. It must not
pass silently either, so it is reported with a **loud warning on every reconcile**:

| Drift | Consequence |
|---|---|
| `value` ≠ -1000 (e.g. non-negative) | Headroom pods become un-preemptable — the buffer stops handing capacity back to real workloads, defeating the entire point |
| `preemptionPolicy` ≠ `Never` | Headroom pods can **evict real workloads** |

Fixing either means **deleting the PriorityClass** so the controller recreates it correctly.

## Environment Variables

Read in `cmd/controller/main.go`:

| Variable | Default | Purpose |
|---|---|---|
| `HEADROOM_NAMESPACE` | `karpenter` | Namespace for the headroom Deployments/pods |
| `HEADROOM_CONFIG_NAMESPACE` | value of `HEADROOM_NAMESPACE` | Namespace of the `headroom-policy` ConfigMap |

## RBAC

No kubebuilder markers in this repo — keep the deployed ClusterRole in sync by hand:

- `apps/deployments`: create, update, delete, get, list, watch
- `pods`: delete, get, list, watch (legacy bare-pod GC)
- `configmaps`: get, list, watch
- `nodes`: get, list, watch
- `scheduling.k8s.io/priorityclasses`: create, get

## Key Files

| File | Purpose |
|---|---|
| `pkg/controllers/headroom/controller.go` | Reconciler, Deployment apply, replica math, GC, PriorityClass |
| `pkg/controllers/headroom/config.go` | ConfigMap parsing: percentages, pod sizes, minPods, tolerations |
| `examples/policy_configmap.yaml` | Reference policy ConfigMap |
| `examples/nodepool.yaml` | NodePools with the required `WhenEmpty` consolidation |
