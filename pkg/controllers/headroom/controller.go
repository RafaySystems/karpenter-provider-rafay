/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package headroom implements proactive scale-out via per-pool overprovisioning
// Deployments (the proven cluster-overprovisioner pattern).
//
// The controller reads a headroom-policy ConfigMap that declares, per Karpenter NodePool,
// what fraction of CPU/memory/GPU to keep pre-reserved and how big each placeholder pod
// is. For every configured pool it maintains one Deployment ("headroom-<pool>") of
// low-priority pause pods with nodeSelector: karpenter.sh/nodepool=<name>. Replicas are
// computed so the pods together hold (total pool allocatable × fraction) of each
// configured resource; `minPods` provides a cold-start floor so an empty pool can be
// warmed up (the Pending pause pods make Karpenter provision the first nodes).
//
// When a real (higher-priority) workload preempts a headroom pod, the ReplicaSet replaces
// it immediately and the replacement becomes Pending. Because its nodeSelector names the
// exact NodePool, Karpenter's provisioner sees an unschedulable pod, matches it to that
// NodePool, and creates a new NodeClaim — scaling out and restoring the buffer.
//
// ConfigMap format (data key "policy"):
//
//	pools:
//	  - name: worker-pool-amd   # must match a Karpenter NodePool .metadata.name
//	    cpu: 30%                # buffer fractions of total pool allocatable
//	    memory: 20%
//	    gpu: 10%                # existing GPU nodes only — see the GPU note below
//	    podCPU: 500m            # absolute per-pod slice (defaults: 500m / 512Mi)
//	    podMemory: 512Mi
//	    podGPU: "1"             # no default; a GPU buffer requires this
//	    minPods: 2              # replica floor even with zero ready nodes (cold start)
//	    tolerations:            # optional; NO blanket Exists toleration is added
//	      - key: example.com/dedicated
//	        operator: Equal
//	        value: headroom
//	        effect: NoSchedule
//
// GPU headroom, and its limitation: a GPU buffer reserves capacity on the pool's EXISTING
// GPU nodes; it CANNOT drive scale-out. The Rafay cloudprovider's instance types advertise
// only cpu/memory/pods in their Capacity (never nvidia.com/gpu or amd.com/gpu), so Karpenter
// cannot satisfy a pending pod that requests a GPU extended resource and will not provision a
// node for it. Consequently the GPU resource name is derived from the GPU nodes the pool
// already has, and on a pool without GPU nodes the podGPU request is dropped instead of
// guessed (a guessed request would only park a pause pod in Pending forever). Use minPods with
// a cpu/memory-sized pod to cold-start a GPU pool; the GPU buffer applies once nodes exist.
//
// Required RBAC (no kubebuilder markers in this repo — keep the ClusterRole in sync):
//   - apps/deployments: create, update, delete, get, list, watch
//   - pods: delete, get, list, watch (legacy bare-pod GC)
//   - configmaps: get, list, watch
//   - nodes: get, list, watch
//   - scheduling.k8s.io/priorityclasses: create, get
package headroom

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	headroomLabel     = "rafay.io/headroom"
	headroomPoolLabel = "rafay.io/headroom-pool"

	// karpenterNodePoolLabel is stamped by Karpenter on every node it manages.
	// Using it as the nodeSelector ties a pending pause pod directly to a named
	// NodePool, so Karpenter's provisioner knows which NodePool to scale out.
	karpenterNodePoolLabel = "karpenter.sh/nodepool"

	pauseImage        = "registry.k8s.io/pause:3.9"
	priorityClassName = "rafay-headroom"
	// priorityValue is intentionally negative so any normal pod (priority 0) can preempt
	// headroom pods, freeing capacity for real workloads without extra configuration.
	priorityValue = int32(-1000)

	configMapName = "headroom-policy"

	// syncRequestName is the synthetic reconcile request that Node/Deployment events map
	// to. Every reconcile performs a full sync of all pools, so one key suffices.
	syncRequestName = "headroom-sync"

	// resyncInterval is the safety resync returned from every successful reconcile,
	// catching anything the watches miss.
	resyncInterval = 5 * time.Minute

	// maxHeadroomReplicas caps the computed replica count of a single headroom Deployment.
	// The replica math divides pool allocatable by the per-pod size, so a typo in either
	// (or a huge minPods) can ask for an absurd number of pause pods; the cap turns that
	// into a loud warning plus a bounded pod count instead of an int32 overflow / pod flood.
	maxHeadroomReplicas = int64(5000)
)

// gpuResourceNames lists the extended resources treated as "GPU" for the buffer math.
// All nodes in a Rafay pool share one SKU, so at most one of these is non-zero per pool.
var gpuResourceNames = []corev1.ResourceName{"nvidia.com/gpu", "amd.com/gpu"}

// Controller maintains one overprovisioning Deployment of pause pods per configured pool.
//
// MaxConcurrentReconciles is 1 and every reconcile is a full sync of all pools (singleton
// semantics) — ConfigMap, Node and Deployment events all funnel into one synthetic request.
type Controller struct {
	kubeClient      client.Client
	podNamespace    string
	configNamespace string
}

// NewController creates the headroom buffer maintainer.
// podNamespace is where headroom Deployments are created (typically "karpenter").
// configNamespace is where the headroom-policy ConfigMap lives.
func NewController(podNamespace, configNamespace string) *Controller {
	return &Controller{
		podNamespace:    podNamespace,
		configNamespace: configNamespace,
	}
}

// Register implements operatorpkg controller.Controller. It wires the kubeClient from the
// manager and registers a controller-runtime controller that performs a full sync on:
//   - changes to the headroom-policy ConfigMap (config changes apply immediately);
//   - Karpenter pool node events (allocatable totals changed);
//   - headroom Deployment events (drift repair / self-heal).
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	c.kubeClient = m.GetClient()

	// Create the PriorityClass at startup so it exists before the first Deployment's pods are
	// admitted. This is only a head start: Reconcile re-ensures it on every sync, which is
	// what actually retries a failure here (this runnable gets exactly one attempt).
	if err := m.Add(manager.RunnableFunc(func(ctx context.Context) error {
		if err := c.ensurePriorityClass(ctx); err != nil {
			klog.Warningf("headroom: could not ensure PriorityClass %q: %v (continuing)", priorityClassName, err)
		}
		return nil
	})); err != nil {
		return err
	}

	// Only the headroom-policy ConfigMap in the config namespace is interesting.
	configMapPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetName() == configMapName && obj.GetNamespace() == c.configNamespace
	})
	// Only nodes managed by a Karpenter NodePool affect the buffer math.
	poolNodePredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[karpenterNodePoolLabel] != ""
	})
	// Only headroom-owned Deployments in the pod namespace.
	headroomDeploymentPredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetNamespace() == c.podNamespace && obj.GetLabels()[headroomLabel] == "true"
	})

	return controllerruntime.NewControllerManagedBy(m).
		Named("headroom").
		For(&corev1.ConfigMap{}, builder.WithPredicates(configMapPredicate)).
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(mapToSyncRequest),
			builder.WithPredicates(poolNodePredicate),
		).
		Watches(&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(mapToSyncRequest),
			builder.WithPredicates(headroomDeploymentPredicate),
		).
		WithOptions(crcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(c)
}

// mapToSyncRequest collapses any watched event into the single synthetic request. The
// reconciler always does a full sync, so per-object requests would only add queue churn.
func mapToSyncRequest(context.Context, client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: syncRequestName}}}
}

// Reconcile performs a full sync: for every configured pool it computes the desired
// replica count and applies the headroom Deployment, then garbage-collects legacy bare
// pause pods and Deployments of pools no longer in the config. The request key is
// ignored — all triggers funnel into the same full sync.
func (c *Controller) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	// Idempotent and cheap (one Create that normally 409s), so run it on every sync: the
	// startup runnable gets exactly one attempt, and without the PriorityClass the pause pods
	// are rejected at admission — a transient failure there would disable headroom silently
	// until the next restart. Tolerated on failure: retried on the next reconcile.
	if err := c.ensurePriorityClass(ctx); err != nil {
		klog.Warningf("headroom: could not ensure PriorityClass %q: %v (continuing)", priorityClassName, err)
	}

	// A broken policy (any parse error with no usable pools) MUST abort here: falling through
	// with an empty pool list would make cleanupStaleDeployments tear down every headroom
	// Deployment in the cluster over a YAML typo.
	pools, err := c.loadPools(ctx)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("load config: %w", err)
	}

	var errs []error
	for _, pool := range pools {
		if err := c.reconcilePool(ctx, pool); err != nil {
			klog.Warningf("headroom: reconcile pool %q: %v", pool.Name, err)
			errs = append(errs, fmt.Errorf("pool %q: %w", pool.Name, err))
		}
	}
	if err := c.cleanupLegacyPods(ctx); err != nil {
		klog.Warningf("headroom: cleanup legacy pods: %v", err)
		errs = append(errs, err)
	}
	if err := c.cleanupStaleDeployments(ctx, pools); err != nil {
		klog.Warningf("headroom: cleanup stale deployments: %v", err)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return reconcile.Result{}, errors.Join(errs...)
	}
	return reconcile.Result{RequeueAfter: resyncInterval}, nil
}

func (c *Controller) loadPools(ctx context.Context) ([]parsedPoolPolicy, error) {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Name: configMapName, Namespace: c.configNamespace}
	if err := c.kubeClient.Get(ctx, key, &cm); err != nil {
		if kerrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return parseHeadroomConfig(&cm)
}

// reconcilePool applies the headroom Deployment for one pool.
//
// Buffer model: replicas = max over configured resources of
// ceil(fraction × Σ ready-node allocatable / per-pod size), floored at minPods.
// Pod requests are exactly the per-pod size (NOT derived from node size) — the buffer is
// denominated absolutely, so preempting one pod frees a predictable slice of capacity.
func (c *Controller) reconcilePool(ctx context.Context, pool parsedPoolPolicy) error {
	var nodeList corev1.NodeList
	if err := c.kubeClient.List(ctx, &nodeList, client.MatchingLabels{karpenterNodePoolLabel: pool.Name}); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	readyNodes := filterReady(nodeList.Items)

	total := sumAllocatable(readyNodes)
	replicas := desiredReplicas(pool, total, len(readyNodes))
	requests := perPodRequests(pool, total.gpuKey)
	if !pool.PodGPU.IsZero() && total.gpuKey == "" {
		klog.Warningf("headroom: pool %q configures podGPU but none of its %d ready nodes advertise a GPU resource; "+
			"omitting the GPU request (GPU headroom cannot provision GPU nodes, only reserve capacity on existing ones)",
			pool.Name, len(readyNodes))
	}

	podLabels := map[string]string{
		headroomLabel:     "true",
		headroomPoolLabel: pool.Name,
	}
	grace := int64(0)
	never := corev1.PreemptNever

	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name:      deploymentName(pool.Name),
		Namespace: c.podNamespace,
	}}
	op, err := controllerutil.CreateOrUpdate(ctx, c.kubeClient, dep, func() error {
		dep.Labels = podLabels
		// Selector is immutable after creation; only set it on create.
		if dep.Spec.Selector == nil {
			dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: podLabels}
		}
		dep.Spec.Replicas = lo.ToPtr(replicas)

		// Mutate only the fields this controller owns, in place on the FETCHED template.
		// Assigning a whole PodTemplateSpec here would drop everything the API server
		// defaults (restartPolicy, dnsPolicy, schedulerName, securityContext, per-container
		// terminationMessagePath, …), so the mutated object would differ from the stored one
		// on EVERY reconcile — CreateOrUpdate would issue a pointless Update and log
		// "updated" every 5 minutes, forever.
		tmpl := &dep.Spec.Template
		tmpl.Labels = podLabels
		tmpl.Spec.PriorityClassName = priorityClassName
		tmpl.Spec.PreemptionPolicy = &never
		tmpl.Spec.TerminationGracePeriodSeconds = &grace
		tmpl.Spec.NodeSelector = map[string]string{
			karpenterNodePoolLabel: pool.Name,
		}
		// Only tolerations from the pool config — deliberately NO blanket Exists
		// toleration, so headroom pods respect taints that real workloads cannot cross.
		tmpl.Spec.Tolerations = pool.Tolerations
		// Spread placeholder pods across nodes so preempting one frees capacity on
		// the node where the real workload wants to land.
		tmpl.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew:           1,
			TopologyKey:       corev1.LabelHostname,
			WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{headroomPoolLabel: pool.Name}},
		}}
		if len(tmpl.Spec.Containers) != 1 {
			tmpl.Spec.Containers = make([]corev1.Container, 1)
		}
		ctr := &tmpl.Spec.Containers[0]
		ctr.Name = "pause"
		ctr.Image = pauseImage
		ctr.ImagePullPolicy = corev1.PullIfNotPresent
		// Requests == Limits → Guaranteed QoS; each pod holds exactly its slice.
		ctr.Resources = corev1.ResourceRequirements{
			Requests: requests,
			Limits:   requests.DeepCopy(),
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("apply deployment %s/%s: %w", c.podNamespace, dep.Name, err)
	}
	if op != controllerutil.OperationResultNone {
		klog.Infof("headroom: %s deployment %s/%s pool=%s replicas=%d perPod=%v readyNodes=%d",
			op, dep.Namespace, dep.Name, pool.Name, replicas, requests, len(readyNodes))
	}
	return nil
}

// deploymentName returns the per-pool headroom Deployment name.
func deploymentName(poolName string) string {
	return "headroom-" + poolName
}

// poolAllocatable aggregates allocatable resources across the ready nodes of one pool.
type poolAllocatable struct {
	cpuMilli int64
	memBytes int64
	gpu      int64
	// gpuKey is the GPU extended resource name observed on the pool's ready nodes, and is
	// EMPTY when none of them advertise a GPU. It is deliberately not defaulted: guessing
	// nvidia.com/gpu for a pool with no GPU nodes yet would stamp a GPU request that nothing
	// can satisfy (the cloudprovider's instance types never advertise GPU capacity, so
	// Karpenter cannot provision for it) and park the pause pod in Pending forever — and on
	// an amd.com/gpu pool the guess would be wrong outright.
	gpuKey corev1.ResourceName
}

func sumAllocatable(nodes []corev1.Node) poolAllocatable {
	var total poolAllocatable
	for i := range nodes {
		alloc := nodes[i].Status.Allocatable
		total.cpuMilli += alloc.Cpu().MilliValue()
		total.memBytes += alloc.Memory().Value()
		for _, key := range gpuResourceNames {
			if q, ok := alloc[key]; ok && !q.IsZero() {
				total.gpu += q.Value()
				total.gpuKey = key
			}
		}
	}
	return total
}

// desiredReplicas computes the replica count for one pool's headroom Deployment: the max
// over configured resources of ceil(buffer / perPod), floored at minPods. With no ready
// nodes the buffer is zero, so the result is exactly minPods (cold start; 0 if unset).
//
// The result is clamped into [0, maxHeadroomReplicas] before the int32 cast: parse-time
// validation makes an absurd count unlikely, but an unclamped int64→int32 cast would turn
// one bad number into a negative replica count (rejected by the API server) or a silently
// truncated one, and even a valid-looking count must not be allowed to flood the cluster.
func desiredReplicas(pool parsedPoolPolicy, total poolAllocatable, readyNodes int) int32 {
	var n int64
	if readyNodes > 0 {
		if pool.CPU > 0 {
			n = max(n, ceilDiv(float64(total.cpuMilli)*pool.CPU, pool.PodCPU.MilliValue()))
		}
		if pool.Memory > 0 {
			n = max(n, ceilDiv(float64(total.memBytes)*pool.Memory, pool.PodMemory.Value()))
		}
		// A GPU buffer needs an explicit per-pod size; there is no sane default.
		if pool.GPU > 0 && !pool.PodGPU.IsZero() {
			n = max(n, ceilDiv(float64(total.gpu)*pool.GPU, pool.PodGPU.Value()))
		}
	}
	n = max(n, int64(pool.MinPods))
	if n < 0 {
		klog.Warningf("headroom: pool %q computed a negative replica count (%d); clamping to 0 — check podCPU/podMemory/minPods", pool.Name, n)
		n = 0
	}
	if n > maxHeadroomReplicas {
		klog.Warningf("headroom: pool %q computed %d replicas, above the cap of %d; clamping — check the buffer fractions and per-pod sizes",
			pool.Name, n, maxHeadroomReplicas)
		n = maxHeadroomReplicas
	}
	return int32(n)
}

// ceilDiv returns ceil(buffer / perPod), guarding against non-positive operands and capping
// the result at maxHeadroomReplicas — the cap also keeps the float64→int64 conversion in
// range (converting a +Inf or an out-of-range float is undefined behaviour in Go).
func ceilDiv(buffer float64, perPod int64) int64 {
	if perPod <= 0 || buffer <= 0 {
		return 0
	}
	n := math.Ceil(buffer / float64(perPod))
	if n > float64(maxHeadroomReplicas) {
		return maxHeadroomReplicas
	}
	return int64(n)
}

// perPodRequests builds the per-pod resource list. CPU and memory always carry the
// (defaulted) per-pod size. GPU is added only when podGPU is configured AND the pool's ready
// nodes actually advertise a GPU extended resource, whose name gpuKey carries: with no GPU
// node there is no key to request, and requesting a guessed one would only strand the pause
// pod in Pending (Karpenter cannot provision GPU capacity for it — see the package doc).
func perPodRequests(pool parsedPoolPolicy, gpuKey corev1.ResourceName) corev1.ResourceList {
	reqs := corev1.ResourceList{
		corev1.ResourceCPU:    pool.PodCPU,
		corev1.ResourceMemory: pool.PodMemory,
	}
	if !pool.PodGPU.IsZero() && gpuKey != "" {
		reqs[gpuKey] = pool.PodGPU
	}
	return reqs
}

// cleanupLegacyPods deletes bare pause pods created directly by the pre-Deployment version
// of this controller. Deployment-managed pods carry a ReplicaSet ownerReference, so only
// ownerless pods with the headroom label are legacy and safe to remove.
func (c *Controller) cleanupLegacyPods(ctx context.Context) error {
	var podList corev1.PodList
	if err := c.kubeClient.List(ctx, &podList,
		client.InNamespace(c.podNamespace),
		client.MatchingLabels{headroomLabel: "true"},
	); err != nil {
		return fmt.Errorf("list headroom pods: %w", err)
	}
	for i := range podList.Items {
		p := &podList.Items[i]
		if len(p.OwnerReferences) > 0 {
			continue
		}
		klog.Infof("headroom: deleting legacy bare pod %s/%s pool=%s", p.Namespace, p.Name, p.Labels[headroomPoolLabel])
		if err := c.kubeClient.Delete(ctx, p); client.IgnoreNotFound(err) != nil {
			klog.Warningf("headroom: delete legacy pod %s/%s: %v", p.Namespace, p.Name, err)
		}
	}
	return nil
}

// cleanupStaleDeployments deletes headroom Deployments whose pool is no longer configured.
func (c *Controller) cleanupStaleDeployments(ctx context.Context, pools []parsedPoolPolicy) error {
	configured := make(map[string]bool, len(pools))
	for _, pool := range pools {
		configured[pool.Name] = true
	}
	var depList appsv1.DeploymentList
	if err := c.kubeClient.List(ctx, &depList,
		client.InNamespace(c.podNamespace),
		client.MatchingLabels{headroomLabel: "true"},
	); err != nil {
		return fmt.Errorf("list headroom deployments: %w", err)
	}
	for i := range depList.Items {
		d := &depList.Items[i]
		if configured[d.Labels[headroomPoolLabel]] {
			continue
		}
		klog.Infof("headroom: deleting deployment %s/%s for removed pool %q", d.Namespace, d.Name, d.Labels[headroomPoolLabel])
		if err := c.kubeClient.Delete(ctx, d); client.IgnoreNotFound(err) != nil {
			klog.Warningf("headroom: delete deployment %s/%s: %v", d.Namespace, d.Name, err)
		}
	}
	return nil
}

// ensurePriorityClass idempotently creates the rafay-headroom PriorityClass.
// priority=-1000 means every normal pod (priority≥0) can preempt headroom pods.
// PreemptionPolicy=Never prevents headroom pods from evicting other pods.
//
// A PriorityClass's Value is immutable and we hold no update verb, so a pre-existing class
// with different settings cannot be repaired here — but it must not pass silently either: a
// non-negative Value makes headroom pods un-preemptable (headroom stops handing capacity
// back to real workloads) and PreemptionPolicy!=Never lets them evict real ones. Both are
// reported loudly on every reconcile; fixing them means deleting the PriorityClass.
func (c *Controller) ensurePriorityClass(ctx context.Context) error {
	never := corev1.PreemptNever
	pv := priorityValue
	pc := &schedulingv1.PriorityClass{
		ObjectMeta:       metav1.ObjectMeta{Name: priorityClassName},
		Value:            pv,
		GlobalDefault:    false,
		PreemptionPolicy: &never,
		Description:      "Headroom placeholder pods — preemptable by all real workloads.",
	}
	if err := c.kubeClient.Create(ctx, pc); err != nil {
		if kerrors.IsAlreadyExists(err) {
			return c.checkPriorityClassDrift(ctx)
		}
		return err
	}
	klog.Infof("headroom: created PriorityClass %q priority=%d", priorityClassName, pv)
	return nil
}

// checkPriorityClassDrift warns when the existing rafay-headroom PriorityClass does not carry
// the settings headroom preemption depends on. It never mutates the class (no update verb).
func (c *Controller) checkPriorityClassDrift(ctx context.Context) error {
	var existing schedulingv1.PriorityClass
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: priorityClassName}, &existing); err != nil {
		return fmt.Errorf("get existing PriorityClass %q: %w", priorityClassName, err)
	}
	if existing.Value != priorityValue {
		klog.Warningf("headroom: PriorityClass %q has value=%d, expected %d — headroom pods may not be preemptable by real workloads; "+
			"delete the PriorityClass to have it recreated correctly", priorityClassName, existing.Value, priorityValue)
	}
	if existing.PreemptionPolicy == nil || *existing.PreemptionPolicy != corev1.PreemptNever {
		klog.Warningf("headroom: PriorityClass %q has preemptionPolicy=%q, expected %q — headroom pods may preempt real workloads; "+
			"delete the PriorityClass to have it recreated correctly",
			priorityClassName, lo.FromPtr(existing.PreemptionPolicy), corev1.PreemptNever)
	}
	return nil
}

func filterReady(nodes []corev1.Node) []corev1.Node {
	out := nodes[:0:0]
	for i := range nodes {
		if isNodeReady(&nodes[i]) {
			out = append(out, nodes[i])
		}
	}
	return out
}

func isNodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
