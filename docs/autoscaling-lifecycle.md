# Enabling and Disabling Autoscaling on MKS Clusters

How `karpenter-provider-rafay` gets onto (and off) an MKS cluster. The controller itself is
unaware of any of this — it is deployed and removed by the platform, driven by a single field in
the cluster spec.

Repos involved:

| Repo | Role |
| --- | --- |
| `edge-common` | `spec.config.autoScaling` in the MKS cluster spec, `ExtraConfig.auto_scaling` on the edge object, the two provision-task operations |
| `edgesrv` | reads the spec, decides day0 vs day2, drives the salt orchestration, records the result |
| `infra-states` | the salt states that actually `kubectl apply` / `kubectl delete` on the cluster |
| `karpenter-provider-rafay` | source of the manifests that `infra-states` ships |

---

## 1. The spec field

```yaml
apiVersion: infra.k8smgmt.io/v3
kind: Cluster
metadata:
  name: my-mks-cluster
spec:
  type: mks
  config:
    kubernetesVersion: v1.31.0
    autoScaling: true      # <-- deploys karpenter-provider-rafay
    nodes:
      - hostname: node-1
        roles: [Master, Worker]
```

`autoScaling` is a plain boolean, `omitempty`. **Absent means disabled** — dropping the field from
a spec that previously had `autoScaling: true` is treated as an explicit disable, not as "leave it
alone".

It is stored on the edge object as `ExtraConfig.AutoScaling` (`extra_config` is a JSON column, so
adding it needed no DB migration) and mirrored back into the stored cluster config file after each
successful apply, so the next day2 diff compares against what was really applied.

---

## 2. Day0 — cluster creation

The state is **not** part of `orch.edgeup`. The controller mounts `edge-client-creds` out of
`rafay-system`, and neither exists until `ApplyBootstrap` has run. So creation records intent only,
and the deploy happens once the cluster is up:

```
ClusterCreationExecutor
  └─ e.ExtraConfig.AutoScaling = clusterConfig.Spec.MKSConfig.AutoScaling   (recorded, not applied)

orch.edgeup / orch.edgeup-no-ha  →  processEdgeup
  └─ processControlPlaneProvisionSuccess
       └─ ApplyBootstrap                          (creates rafay-system + edge-client-creds)
       └─ MarkClusterProvisionSuccessful
            └─ deployAutoScalingIfEnabled         (only when AutoScaling is true)
                 └─ salt.EnableAutoScaling → orch.autoscaling
```

`deployAutoScalingIfEnabled` is **best-effort**: a failure is logged and the cluster still comes up
Ready. Recovery is to toggle `autoScaling` on day2.

---

## 3. Day2 — enable and disable

A day2 change arrives on one of two paths. A **v3 cluster-config apply** (rctl / UI / terraform)
goes through the normal MKS diff → task → executor pipeline:

```
ClusterAutoScalingDiffProvider          (diffmanager/mks/diff_cluster_autoscaling.go)
  false → true   ⇒ ClusterAutoScalingEnable
  true  → false  ⇒ ClusterAutoScalingDisable

UpdateClusterAutoScalingExecutor        (executors/clusterautoscaling.go)
  enable  → salt.EnableAutoScaling  → orch.autoscaling
  disable → salt.DisableAutoScaling → orch.autoscaling-disable
```

A **raw v2 edge PUT** (`PUT /edge/v1/.../edges/{id}/` — the mks-oneclick driver's update path)
is caught in the `UpdateEdge` handler (`pkg/els/api/v1/edge.go` → `dayTwoAutoScalingToggle` /
`runDayTwoAutoScaling`): a READY MKS cluster whose incoming `extra_config.auto_scaling` differs
from the stored value triggers the **same** salt orchestration directly (no taskset). The
incoming flag is reverted before the edge is persisted — on both paths the edge object and the
stored cluster config are only ever updated from the salt result
(`processClusterAutoScalingStatus`), so a failed apply never records "autoscaling on" and a
reconciling caller (the driver) re-triggers on its next run. A cluster that is not READY keeps
the day0 contract: the flag persists as sent and `deployAutoScalingIfEnabled` applies it when
provisioning completes.

Two operations rather than one "update" because the two directions run genuinely different
orchestrations. The **operation** is the authority on direction; the metadata's `auto_scaling` flag
only carries the value along for persistence.

Enable on day2 runs exactly the same `orch.autoscaling` as day0 — the state is idempotent, so
re-enabling a cluster that still has the CRDs and RBAC just puts the Deployment back.

### Conflicts

Autoscaling tasks land in the generic "other task" bucket of `ConflictingTasksValidator`, which
means they are rejected alongside a Kubernetes or platform-version upgrade but allowed alongside
ordinary config changes.

---

## 4. What the salt states do

### `autoscaling` (enable)

Runs on **one** master — the state is a set of `kubectl apply`s against the cluster, so running it
on every master would only race the same objects.

1. Write manifests to `/opt/rafay/autoscaling/`
2. Apply the three CRDs (`nodepools.karpenter.sh`, `nodeclaims.karpenter.sh`,
   `rafaynodeclasses.karpenter.rafay.io`)
3. `kubectl wait --for=condition=Established` on them — Karpenter registers a field index on
   `status.providerID` at start-up, which only works once the NodeClaim CRD is established.
   Waiting here avoids a crash-loop-then-restart cycle.
4. Apply the `karpenter` namespace (leader-election Leases only — the Rafay drift webhook rejects
   Lease writes in `rafay-system`)
5. Apply ServiceAccount + ClusterRole/Binding
6. Apply the Deployment into `rafay-system` and wait for the rollout

### `autoscaling-disable`

Deliberately narrow — **only the Deployment is deleted**:

```
kubectl -n rafay-system delete deployment karpenter-provider-rafay --ignore-not-found=true
```

CRDs, ServiceAccount, ClusterRole/Binding, the `karpenter` namespace, and every user-created
`NodePool` / `RafayNodeClass` / `NodeClaim` are left in place. Re-enabling therefore only has to put
the Deployment back and the existing scaling configuration keeps working. `--ignore-not-found`
keeps the state idempotent.

> Nodes that Karpenter already provisioned are **not** removed on disable. Deleting the Deployment
> stops further scale-out and scale-in; the existing fleet stays as-is until someone removes the
> NodePools.

---

## 5. Manifests are copied, not referenced

`infra-states/edge/file/autoscaling/*.yaml` are copies of `karpenter-provider-rafay/config/`:

| infra-states | source |
| --- | --- |
| `karpenter.sh_nodepools.yaml` | `config/crd/karpenter.sh_nodepools.yaml` |
| `karpenter.sh_nodeclaims.yaml` | `config/crd/karpenter.sh_nodeclaims.yaml` |
| `karpenter.rafay.io_rafaynodeclasses.yaml` | `config/crd/karpenter.rafay.io_rafaynodeclasses.yaml` |
| `namespace-karpenter-leader.yaml` | `config/deploy/namespace-karpenter-leader.yaml` |
| `serviceaccount.yaml` | `config/deploy/serviceaccount.yaml` |
| `rbac.yaml` | `config/deploy/rbac.yaml` |
| `deployment.yaml` | `config/deploy/deployment.yaml`, jinja-templated |

**Changing a CRD or the RBAC in this repo means re-copying it into `infra-states`.** There is no
build step that does it for you.

`deployment.yaml` is the one that diverges: it resolves the image registry from `deployment_env`
(the same jinja block every other Rafay state uses) and fills `EDGE_ID` / `RAFAY_CLUSTER_ID` /
`RAFAY_PROJECT_ID` from the salt pillar. The image tag comes from the `KARPENTER_PROVIDER_VERSION`
setting on edgesrv, defaulting to `latest`.

Two further pillars control the broker-driven node pool bootstrap (see
[architecture.md §7.1](architecture.md#71-catalog--rafaynodeclass-bootstrap)):

| Pillar | Default | Env var |
| --- | --- | --- |
| `karpenter_config_bootstrap` | `true` | `KARPENTER_CONFIG_BOOTSTRAP` |
| `karpenter_config_sync_interval` | `10m` | `KARPENTER_CONFIG_SYNC_INTERVAL` |

**This state installs no `NodePool` or `RafayNodeClass` of its own** — it applies only the CRDs,
namespace, ServiceAccount, RBAC and Deployment. Before the bootstrap existed, enabling autoscaling
therefore left Karpenter running with nothing to scale until someone hand-wrote manifests. With
the default `karpenter_config_bootstrap: true` the controller fetches them from edge-broker
instead. Set the pillar to `false` on a cluster whose NodePools are owned by hand or by GitOps —
the resync applies with `force: true` and would otherwise take ownership of them.

Write the pillar as a **quoted string** (`"false"`). A bare YAML `false` renders as `"False"`,
which the controller still parses correctly, but the quoted form is what the template expects.

---

## 6. File map

**edge-common**
- `pkg/clusterconfig/common/mksconfig.go` — `MKSClusterConfig.AutoScaling`
- `pkg/models/edge/edge.proto` — `ExtraConfig.auto_scaling` (field 7)
- `pkg/models/edge/provision_taskset.proto` — `ClusterAutoScalingEnable` (130),
  `ClusterAutoScalingDisable` (131), `UpdatingClusterAutoScalingMetadata`

**edgesrv**
- `pkg/els/clusterctl/executors/clustercreation.go` — day0 capture
- `pkg/els/salt/processor_utils.go` — `deployAutoScalingIfEnabled`, the day0 trigger
- `pkg/els/api/v1/edge.go` — `dayTwoAutoScalingToggle` / `runDayTwoAutoScaling`, the day2
  trigger for the raw v2 edge PUT (mks-oneclick driver path)
- `pkg/els/salt/minion.go` — `EnableAutoScaling` / `DisableAutoScaling`
- `pkg/els/salt/processor.go` — `ProcessAutoScalingOrch`, `processClusterAutoScalingStatus`
- `pkg/els/clusterctl/diffmanager/mks/diff_cluster_autoscaling.go` — day2 diff
- `pkg/els/clusterctl/diffmanager/diff_main.go` — `NewUpdateClusterAutoScalingTask`
- `pkg/els/clusterctl/executors/clusterautoscaling.go` — day2 executor
- `pkg/common/mksgenerate.go` — emits `autoScaling` into the generated spec (diff round-trip)
- `pkg/common/mks_cluster_config_file.go` — `UpdateMKSEdgeWithAutoScaling`
- `pkg/common/events.go` — `AutoScalingEvents` audit events

**infra-states**
- `infra-states/edge/file/autoscaling/` — enable state + manifests
- `infra-states/edge/file/autoscaling-disable/` — disable state
- `infra-states/edge/file/orch/autoscaling.sls`
- `infra-states/edge/file/orch/autoscaling-disable.sls`

---

## 7. Events

| Taskset event | Audit event |
| --- | --- |
| `CLUSTER_AUTOSCALING_ENABLE_SUCCESSFUL` | `cluster.autoscaling.enabled` |
| `CLUSTER_AUTOSCALING_ENABLE_FAILED` | `cluster.autoscaling.enable.failed` |
| `CLUSTER_AUTOSCALING_DISABLE_SUCCESSFUL` | `cluster.autoscaling.disabled` |
| `CLUSTER_AUTOSCALING_DISABLE_FAILED` | `cluster.autoscaling.disable.failed` |

The edge object and the stored cluster config file are updated from the **orchestration result**,
not from the executor, so a failed apply never leaves the recorded state claiming autoscaling is on.
