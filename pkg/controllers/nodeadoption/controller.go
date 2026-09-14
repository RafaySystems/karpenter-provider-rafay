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

// Package nodeadoption gives every worker node that already exists in the cluster a NodeClaim, so
// Karpenter manages the whole node pool rather than only the nodes it provisioned itself.
//
// A Rafay cluster is created with worker nodes already in it: the compute instance's "Worker Node
// Pool" catalog says pool1 has 3 nodes, and the platform builds them before Karpenter ever runs.
// Those nodes carry the pool's identity as labels — nodepoolname=pool1, sku_name=oci-inst — but no
// NodeClaim, and a node without a NodeClaim is invisible to Karpenter:
//
//   - NodePool.status.resources and .status.nodes count only NodeClaims, so a pool that already has
//     3 nodes reports zero and its limits are computed as if the cluster were empty.
//   - the scheduler sees the nodes (they are in cluster state) but cannot disrupt or consolidate
//     them (StateNode.ValidateNodeDisruptable requires a NodeClaim), so capacity the catalog
//     provisioned can never be reclaimed — only capacity Karpenter itself added.
//
// So on NodePool creation (and whenever a matching node appears) this controller counts the pool's
// nodes, works out which of them no NodeClaim owns, and creates one per node — bound to the node by
// spec.providerID, which it also fills in when the platform left it empty.
//
// The NodeClaim is created carrying cloudprovider.AdoptedProviderIDAnnotationKey. That marker is
// what keeps adoption from provisioning machines: Karpenter's lifecycle controller calls
// CloudProvider.Create() for any NodeClaim that is not yet Launched, and Create() returns the
// annotated ProviderID instead of asking the broker for a node. Karpenter then registers and
// initializes the NodeClaim against the existing node through its normal path.
//
// Deliberately conservative, because the nodes involved are already running workloads:
//
//   - a pool whose NodePool is annotated karpenter.rafay.io/auto-scaling=false — edge-broker's
//     inert rendering of a catalog row that did not opt into autoscaling — is skipped entirely,
//     so its nodes never get NodeClaims and stay outside Karpenter's control;
//   - only a node the kubelet reports Ready is adopted (see isReady), so the pool is never charged
//     for capacity nothing can schedule on, and a machine that never joined properly — or is being
//     torn down — is not handed an immutable provider ID on its way out;
//   - a node a pending NodeClaim may still be waiting for is left alone (see claimedByPendingClaim),
//     so adoption never steals the node a scale-out just asked the broker for;
//   - a node whose sku_name is not in the pool's RafayNodeClass, or whose labels do not satisfy the
//     pool's requirements, is skipped with a warning rather than adopted into a NodeClaim that
//     Karpenter would immediately consider drifted and replace;
//   - the pool's taints are not copied onto the NodeClaim (Registration would push them onto a node
//     already running pods, and a NoExecute taint would evict them);
//   - no karpenter.sh/nodepool-hash annotation is set, so a later edit to the NodePool template
//     does not mark every adopted node drifted and roll the pool.
//
// Adoption does mean Karpenter may consolidate a pre-existing node once it is empty, which is the
// point — the pool becomes Karpenter's to size. KARPENTER_ADOPT_EXISTING_NODES=false turns it off.
package nodeadoption

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/object"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

const (
	// nodepoolNameLabel / skuNameLabel are the labels the Rafay platform stamps on every worker
	// node: the catalog pool it belongs to and the node SKU it was built from. nodepoolname is
	// also the Karpenter NodePool name (edge-broker renders one NodePool per catalog row, named
	// after the pool), which is what lets a node be traced back to its pool.
	nodepoolNameLabel = "nodepoolname"
	skuNameLabel      = "sku_name"

	// adoptedNodeAnnotationKey records which node an adopted NodeClaim was created for. It is
	// informational (`kubectl get nodeclaim -o yaml` reads better than diffing provider IDs); the
	// load-bearing marker is cprovider.AdoptedProviderIDAnnotationKey.
	adoptedNodeAnnotationKey = "karpenter.rafay.io/adopted-node"

	// autoScalingAnnotationKey is stamped by edge-broker on every NodePool it renders: "true" for
	// a catalog row that opted into autoscaling, "false" for a row rendered only so the pool is
	// visible in-cluster. The spelling is a contract with the broker's render
	// (edge-broker/pkg/context/karpenter_config_render.go). Only the explicit "false" is acted on
	// here — a hand-written NodePool carries no annotation and adopts as it always has.
	autoScalingAnnotationKey = "karpenter.rafay.io/auto-scaling"

	// resyncInterval re-examines each pool periodically. The Node and NodePool watches already
	// cover the normal triggers; this only catches an adoption that failed transiently (a node
	// patch conflict, a NodeClaim create rejected while the API server was busy) without needing
	// its own retry state.
	resyncInterval = 5 * time.Minute
)

// InstanceTypeProvider resolves a NodePool's instance types. *cloudprovider.CloudProvider
// implements it; the interface keeps this controller testable without a broker connection.
type InstanceTypeProvider interface {
	GetInstanceTypes(ctx context.Context, nodePool *karpv1.NodePool) ([]*cloudprovider.InstanceType, error)
}

// Controller adopts pre-existing worker nodes into the NodePool they are labelled with.
//
// MaxConcurrentReconciles is 1 so the count-decide-create cycle never runs concurrently for two
// pools: the decision reads every NodeClaim in the cluster, and two overlapping passes could both
// conclude the same node is unowned.
//
// apiReader is an uncached client reading straight from the API server. The NodeClaim list goes
// through it because a freshly created NodeClaim is not in the informer cache yet, and a stale read
// there means a second NodeClaim for a node that already has one — two NodeClaims for one machine,
// the second of which Karpenter eventually terminates, taking the node with it.
type Controller struct {
	kubeClient    client.Client
	apiReader     client.Reader
	instanceTypes InstanceTypeProvider
}

// NewController returns a controller that adopts existing worker nodes into their NodePool.
func NewController(kubeClient client.Client, instanceTypes InstanceTypeProvider) *Controller {
	return &Controller{kubeClient: kubeClient, instanceTypes: instanceTypes}
}

// EnabledFromEnv reports whether adoption should run. It defaults to on;
// KARPENTER_ADOPT_EXISTING_NODES=false is the escape hatch for a cluster whose pre-existing nodes
// must stay outside Karpenter's control (adopted nodes become eligible for consolidation).
func EnabledFromEnv() bool {
	raw := strings.TrimSpace(os.Getenv("KARPENTER_ADOPT_EXISTING_NODES"))
	if raw == "" {
		return true
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		klog.Warningf("nodeadoption: invalid KARPENTER_ADOPT_EXISTING_NODES=%q, defaulting to enabled", raw)
		return true
	}
	return enabled
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	// Store the uncached API reader; must be set before any reconcile runs.
	c.apiReader = m.GetAPIReader()

	// rafayNodePredicate passes only nodes that carry the platform's pool identity. Unlike the
	// NodeProviderIDController's predicate it does NOT require spec.providerID to be set — a node
	// with an empty provider ID is exactly the case this controller fixes.
	rafayNodePredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return false
		}
		return node.Labels[nodepoolNameLabel] != "" && node.Labels[skuNameLabel] != ""
	})

	return controllerruntime.NewControllerManagedBy(m).
		Named("nodepool.nodeadoption").
		// Primary watch: NodePools. A pool applied by the nodeconfig bootstrap (or by hand) is
		// reconciled the moment it is created, which is when its pre-existing nodes need claims.
		For(&karpv1.NodePool{}).
		// Secondary watch: a worker node appearing (or being relabelled) enqueues its pool, so a
		// node that joins after the pool exists is adopted without waiting for the resync.
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(nodeToNodePool),
			builder.WithPredicates(rafayNodePredicate),
		).
		WithOptions(crcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

// nodeToNodePool maps a worker node to the NodePool named by its nodepoolname label. A node whose
// pool has no NodePool object resolves to nothing: reconcile.AsReconciler drops the request when the
// NodePool does not exist.
func nodeToNodePool(_ context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	pool := node.Labels[nodepoolNameLabel]
	if pool == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: pool}}}
}

//nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context, nodePool *karpv1.NodePool) (reconcile.Result, error) {
	if !nodePool.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	// Only pools backed by a RafayNodeClass are ours to adopt into. A NodePool for another
	// provider in the same cluster keeps its own nodes.
	ref := nodePool.Spec.Template.Spec.NodeClassRef
	if ref == nil || ref.Group != v1alpha1.Group || ref.Kind != "RafayNodeClass" {
		return reconcile.Result{}, nil
	}
	// A pool rendered for visibility only (its catalog row did not opt into autoscaling) keeps
	// its nodes unadopted. The inert NodePool already blocks scale-out and disruption, but a
	// NodeClaim would still hand Karpenter ownership it must not have — counting the node
	// against limits, and terminating it (draining the machine) on a manual NodeClaim delete or
	// a stale stamped expiry. No claims means Karpenter cannot touch the pool at all.
	if nodePool.Annotations[autoScalingAnnotationKey] == "false" {
		klog.V(2).Infof("nodeadoption: pool %q has autoscaling disabled (%s=false); leaving its nodes unadopted",
			nodePool.Name, autoScalingAnnotationKey)
		return reconcile.Result{}, nil
	}

	// Nodes come from the informer cache: a stale read here costs at most a delayed adoption, which
	// the next node event or resync corrects.
	var poolNodes corev1.NodeList
	if err := c.kubeClient.List(ctx, &poolNodes, client.MatchingLabels{nodepoolNameLabel: nodePool.Name}); err != nil {
		return reconcile.Result{}, fmt.Errorf("list nodes in pool %q: %w", nodePool.Name, err)
	}
	var allNodes corev1.NodeList
	if err := c.kubeClient.List(ctx, &allNodes); err != nil {
		return reconcile.Result{}, fmt.Errorf("list nodes: %w", err)
	}

	// NodeClaims come from the API server — see the Controller doc for why the cache is unsafe here.
	var claimList karpv1.NodeClaimList
	if err := c.apiReader.List(ctx, &claimList); err != nil {
		return reconcile.Result{}, fmt.Errorf("list nodeclaims: %w", err)
	}

	// claimedIDs is every provider ID a NodeClaim already speaks for. The adoption annotation is
	// read alongside status.providerID because an adopted NodeClaim has no status until Karpenter's
	// Launch reconciler calls Create() — until then the annotation is the only record that the node
	// is taken, and without it a second pass would adopt the same node twice.
	claimedIDs := make(map[string]bool, len(claimList.Items))
	var pendingClaims []*karpv1.NodeClaim
	poolClaims := 0
	for i := range claimList.Items {
		nc := &claimList.Items[i]
		if pid := nc.Status.ProviderID; pid != "" && !strings.HasPrefix(pid, cprovider.PendingProviderIDPrefix) {
			claimedIDs[pid] = true
		}
		if pid := nc.Annotations[cprovider.AdoptedProviderIDAnnotationKey]; pid != "" {
			claimedIDs[pid] = true
		}
		if nc.Labels[karpv1.NodePoolLabelKey] != nodePool.Name {
			continue
		}
		poolClaims++
		if strings.HasPrefix(nc.Status.ProviderID, cprovider.PendingProviderIDPrefix) && nc.DeletionTimestamp.IsZero() {
			pendingClaims = append(pendingClaims, nc)
		}
	}

	workerNodes := lo.CountBy(allNodes.Items, func(n corev1.Node) bool {
		return n.Labels[nodepoolNameLabel] != "" && !isControlPlane(&n)
	})

	// instanceTypes is resolved lazily: most reconciles find nothing to adopt, and this reads the
	// pool's RafayNodeClass.
	var instanceTypes []*cloudprovider.InstanceType
	adopted, skipped, notReady := 0, 0, 0
	var errs error

	for i := range poolNodes.Items {
		node := &poolNodes.Items[i]
		if !node.DeletionTimestamp.IsZero() {
			continue
		}
		// A control-plane node must never be adopted: a NodeClaim would let Karpenter drain and
		// remove the cluster's own control plane.
		if isControlPlane(node) {
			klog.Warningf("nodeadoption: node %s is labelled %s=%s but is a control-plane node; not adopting it",
				node.Name, nodepoolNameLabel, nodePool.Name)
			skipped++
			continue
		}
		// Only adopt a node the kubelet reports Ready. See isReady for why.
		//
		// This sits above the ownership checks below on purpose: those need an instance type resolved
		// (which reads the RafayNodeClass and hard-errors the reconcile if it is unusable) and they
		// warn about labels the platform may not have stamped yet, so a node that is merely still
		// coming up would draw recurring warnings instead of a quiet deferral. The cost is that an
		// already-adopted node that goes NotReady reaches here too — which is why the counter, whose
		// only job is to report what adoption is still waiting on, excludes nodes already spoken for.
		if !isReady(node) {
			if pid := node.Spec.ProviderID; pid == "" || !claimedIDs[pid] {
				klog.V(2).Infof("nodeadoption: node %s is not Ready; leaving it unadopted until it is", node.Name)
				notReady++
			}
			continue
		}
		sku := node.Labels[skuNameLabel]
		if sku == "" {
			klog.Warningf("nodeadoption: node %s has no %s label; cannot tell which SKU it is, not adopting it", node.Name, skuNameLabel)
			skipped++
			continue
		}

		providerID := node.Spec.ProviderID
		if providerID != "" && !strings.HasPrefix(providerID, rafay.ProviderIDPrefix) {
			// Some other provider owns this node despite the Rafay pool labels.
			continue
		}
		if providerID != "" && claimedIDs[providerID] {
			continue
		}
		if claimedByPendingClaim(pendingClaims, node, sku) {
			// A scale-out is still waiting for its node; the NodeProviderIDController will bind
			// this one to that NodeClaim. Adopting it here would leave that claim unresolved until
			// its registration timeout.
			continue
		}

		if instanceTypes == nil {
			its, err := c.instanceTypes.GetInstanceTypes(ctx, nodePool)
			if err != nil {
				return reconcile.Result{}, fmt.Errorf("get instance types for pool %q: %w", nodePool.Name, err)
			}
			instanceTypes = its
		}
		instanceType, ok := lo.Find(instanceTypes, func(it *cloudprovider.InstanceType) bool { return it.Name == sku })
		if !ok {
			klog.Warningf("nodeadoption: node %s has %s=%q, which the RafayNodeClass %q of pool %q does not list as an instance type; "+
				"not adopting it (a NodeClaim for an unknown instance type is marked drifted and replaced)",
				node.Name, skuNameLabel, sku, ref.Name, nodePool.Name)
			skipped++
			continue
		}

		nodeClaim := adoptedNodeClaim(nodePool, node, sku, instanceType)
		if err := validateAdoptable(nodePool, instanceType, nodeClaim.Labels); err != nil {
			klog.Warningf("nodeadoption: not adopting node %s into pool %q: %v", node.Name, nodePool.Name, err)
			skipped++
			continue
		}

		// Fill in the provider ID (and pre-set the registered label) before the NodeClaim exists:
		// the NodeClaim is bound to the node by provider ID, so a claim created first would have
		// nothing to match.
		resolvedID, err := c.prepareNode(ctx, node, nodePool.Name, sku)
		if err != nil {
			errs = multierr.Append(errs, fmt.Errorf("prepare node %s: %w", node.Name, err))
			continue
		}
		nodeClaim.Annotations[cprovider.AdoptedProviderIDAnnotationKey] = resolvedID

		if err := c.kubeClient.Create(ctx, nodeClaim); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("create nodeclaim for node %s: %w", node.Name, err))
			continue
		}
		claimedIDs[resolvedID] = true
		adopted++
		klog.Infof("nodeadoption: adopted node %s into pool %q as nodeclaim %s (providerID=%s, instanceType=%s)",
			node.Name, nodePool.Name, nodeClaim.Name, resolvedID, sku)
	}

	// The counts are the reason to read this log line: how big the pool actually is, how much of it
	// Karpenter already accounted for, and what this pass changed.
	msg := fmt.Sprintf("nodeadoption: pool %q has %d node(s) in the cluster and %d nodeclaim(s) (%d worker node(s) across all pools); adopted %d, skipped %d",
		nodePool.Name, len(poolNodes.Items), poolClaims, workerNodes, adopted, skipped)
	if notReady > 0 {
		msg += fmt.Sprintf(", waiting for %d node(s) to become Ready", notReady)
	}
	// notReady counts towards logging at Info deliberately, even though it repeats every resync
	// while a node stays down. "Why does my pool have fewer NodeClaims than nodes?" needs an
	// answer in the log; a node stuck NotReady is the whole answer, and silence about it is the
	// failure mode worth avoiding more than the repetition.
	if adopted > 0 || skipped > 0 || notReady > 0 {
		klog.Info(msg)
	} else {
		klog.V(2).Info(msg)
	}

	if errs != nil {
		return reconcile.Result{}, errs
	}
	return reconcile.Result{RequeueAfter: resyncInterval}, nil
}

// prepareNode makes the node ready to be owned by a NodeClaim and returns the provider ID the
// NodeClaim must bind to.
//
// Two fields are set, in one patch:
//
//   - spec.providerID, when the platform left it empty. Kubernetes allows setting it only while it
//     is empty (it is immutable afterwards), which is also why a wrong value here is unrecoverable
//     — hence rafay.BuildProviderID rendering exactly the format the platform itself writes.
//   - karpenter.sh/registered, because the Registration reconciler logs a registration error and
//     emits an event for any node it adopts that carries neither that label nor the
//     karpenter.sh/unregistered startup taint. An already-joined node genuinely is registered, and
//     the taint is not an option: it is NoExecute and would evict the pods already running here.
func (c *Controller) prepareNode(ctx context.Context, node *corev1.Node, nodePoolName, sku string) (string, error) {
	providerID := node.Spec.ProviderID
	stored := node.DeepCopy()
	patched := node.DeepCopy()

	if providerID == "" {
		providerID = rafay.BuildProviderID(nodePoolName, sku, node.Name)
		if providerID == "" {
			return "", fmt.Errorf("cannot build a provider ID from pool %q, sku %q, node %q", nodePoolName, sku, node.Name)
		}
		patched.Spec.ProviderID = providerID
	}
	if patched.Labels[karpv1.NodeRegisteredLabelKey] != "true" {
		patched.Labels = lo.Assign(patched.Labels, map[string]string{karpv1.NodeRegisteredLabelKey: "true"})
	}
	if equalNodes(stored, patched) {
		return providerID, nil
	}
	// Optimistic locking: spec.providerID is immutable once set, so a concurrent writer must make
	// this patch fail rather than have it silently lose.
	if err := c.kubeClient.Patch(ctx, patched,
		client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		return "", err
	}
	klog.Infof("nodeadoption: prepared node %s for adoption (providerID=%s)", node.Name, providerID)
	return providerID, nil
}

func equalNodes(a, b *corev1.Node) bool {
	if a.Spec.ProviderID != b.Spec.ProviderID {
		return false
	}
	return a.Labels[karpv1.NodeRegisteredLabelKey] == b.Labels[karpv1.NodeRegisteredLabelKey]
}

// adoptedNodeClaim builds the NodeClaim for one existing node.
//
// It starts from the pool's node template — so an adopted node carries the same pool metadata,
// nodeClassRef and expiry as one Karpenter provisions — and then differs in four ways, each of
// which exists because the machine is already running and holding pods:
//
//   - taints and startupTaints are dropped. Registration merges a NodeClaim's taints onto its node,
//     and a NoExecute taint the node does not already have would evict the pods on it. The platform
//     has already applied the pool's catalog taints to the nodes it built.
//   - no resource requests are set, so Initialization does not wait for an extended resource
//     (a GPU device plugin, say) that this node was never asked to advertise.
//   - the karpenter.sh/nodepool-hash annotations are omitted. Static drift is only evaluated when
//     both NodePool and NodeClaim carry the hash, so leaving it off means a later edit to the pool
//     template does not mark every adopted node drifted and roll nodes the operator did not create.
//   - well-known labels come from the node, not from the SKU (see wellKnownNodeLabels).
func adoptedNodeClaim(nodePool *karpv1.NodePool, node *corev1.Node, sku string, instanceType *cloudprovider.InstanceType) *karpv1.NodeClaim {
	nodeClaim := nodePool.Spec.Template.ToNodeClaim()
	nodeClaim.GenerateName = nodePool.Name + "-"
	nodeClaim.OwnerReferences = []metav1.OwnerReference{{
		APIVersion:         object.GVK(&karpv1.NodePool{}).GroupVersion().String(),
		Kind:               object.GVK(&karpv1.NodePool{}).Kind,
		Name:               nodePool.Name,
		UID:                nodePool.UID,
		BlockOwnerDeletion: lo.ToPtr(true),
	}}
	nodeClaim.Spec.Taints = nil
	nodeClaim.Spec.StartupTaints = nil
	nodeClaim.Spec.Requirements = pinInstanceType(nodePool.Spec.Template.Spec.Requirements, sku)

	nodeClaim.Labels = lo.Assign(
		nodePool.Spec.Template.Labels,
		wellKnownNodeLabels(node, sku, instanceType),
		map[string]string{
			karpv1.NodePoolLabelKey: nodePool.Name,
			karpv1.NodeClassLabelKey(nodePool.Spec.Template.Spec.NodeClassRef.GroupKind()): nodePool.Spec.Template.Spec.NodeClassRef.Name,
		},
	)
	nodeClaim.Annotations = lo.Assign(nodePool.Spec.Template.Annotations, map[string]string{
		adoptedNodeAnnotationKey: node.Name,
	})
	return nodeClaim
}

// wellKnownNodeLabels returns the well-known labels an adopted NodeClaim carries.
//
// They are read off the node wherever the node has them, because Registration copies a NodeClaim's
// labels onto its node: a value inferred from the SKU that disagrees with the node would overwrite
// what the kubelet reported. Only the two the node cannot carry are supplied from the pool side —
// sku_name is the platform's spelling of the instance type, and capacity type is a Karpenter
// concept that the Rafay platform has no equivalent of.
//
// Topology labels are deliberately absent when the node has none. A RafayNodeClass with no zone
// yields the synthetic zone "default", and stamping that onto an adopted node would replace real
// topology information (or invent it). Omitting the key is safe in both directions: zone is a
// well-known label, so Karpenter treats a NodeClaim that lacks it as unconstrained when matching
// the pool's requirements and the SKU's offerings, rather than as a mismatch.
func wellKnownNodeLabels(node *corev1.Node, sku string, instanceType *cloudprovider.InstanceType) map[string]string {
	labels := map[string]string{
		corev1.LabelInstanceTypeStable: sku,
		karpv1.CapacityTypeLabelKey:    capacityTypeOf(instanceType),
	}
	for _, key := range []string{
		corev1.LabelArchStable,
		corev1.LabelOSStable,
		corev1.LabelTopologyZone,
		corev1.LabelTopologyRegion,
	} {
		if v := node.Labels[key]; v != "" {
			labels[key] = v
		}
	}
	return labels
}

// capacityTypeOf reads the capacity type off the instance type's offering. Every Rafay instance
// type has exactly one offering (see rafayInstanceTypesToKarpenter), and it is on-demand; reading
// it rather than hard-coding it keeps this correct if that ever stops being true.
func capacityTypeOf(instanceType *cloudprovider.InstanceType) string {
	if len(instanceType.Offerings) > 0 {
		if ct := instanceType.Offerings[0].CapacityType(); ct != "" {
			return ct
		}
	}
	return karpv1.CapacityTypeOnDemand
}

// pinInstanceType returns the pool's requirements with the instance-type requirement replaced by
// the one SKU this node actually is. The pool template may allow several (or, for a NodeClass with
// multiple instanceTypes, leave it open); the NodeClaim describes a machine that already exists, so
// it states which.
func pinInstanceType(reqs []karpv1.NodeSelectorRequirementWithMinValues, sku string) []karpv1.NodeSelectorRequirementWithMinValues {
	out := make([]karpv1.NodeSelectorRequirementWithMinValues, 0, len(reqs)+1)
	for _, r := range reqs {
		if r.Key == corev1.LabelInstanceTypeStable {
			continue
		}
		out = append(out, r)
	}
	return append(out, karpv1.NodeSelectorRequirementWithMinValues{
		Key:      corev1.LabelInstanceTypeStable,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{sku},
	})
}

// validateAdoptable rejects a NodeClaim that Karpenter would immediately consider drifted, which
// for an existing node means draining and removing it. Both checks mirror the drift sub-controller
// exactly (see nodeclaim/disruption/drift.go):
//
//   - areRequirementsDrifted compares the pool's requirements against the NodeClaim's labels;
//   - instanceTypeNotFound requires a SKU offering compatible with those labels.
//
// Failing either is a real mismatch — an arm64 node labelled into an amd64-only pool, a catalog
// label that collides with a well-known one — so the node is left unmanaged and named in a warning.
func validateAdoptable(nodePool *karpv1.NodePool, instanceType *cloudprovider.InstanceType, labels map[string]string) error {
	labelReqs := scheduling.NewLabelRequirements(labels)
	poolReqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nodePool.Spec.Template.Spec.Requirements...)
	if err := labelReqs.Compatible(poolReqs); err != nil {
		return fmt.Errorf("the node's labels do not satisfy the pool's requirements: %w", err)
	}
	if !instanceType.Offerings.HasCompatible(labelReqs) {
		return fmt.Errorf("instance type %q has no offering compatible with the node's labels", instanceType.Name)
	}
	return nil
}

// claimedByPendingClaim reports whether a scale-out that has not yet found its node could be
// waiting for this one.
//
// It is the mirror of the NodeProviderIDController's rule: that controller binds a node to a
// pending NodeClaim only when the node was created after the claim, so a node newer than any
// pending claim for the same pool and SKU is that claim's to take. Adopting it instead would leave
// the claim unresolved until its 60-minute registration timeout deleted it.
func claimedByPendingClaim(pendingClaims []*karpv1.NodeClaim, node *corev1.Node, sku string) bool {
	for _, nc := range pendingClaims {
		if !cprovider.NodeClaimSKUs(nc)[sku] {
			continue
		}
		if node.CreationTimestamp.After(nc.CreationTimestamp.Time) {
			return true
		}
	}
	return false
}

// isReady reports whether the kubelet says this node is Ready, using the same test Karpenter's own
// Initialization reconciler applies (`nodeutils.GetCondition(node, NodeReady).Status == True`).
//
// Adoption waits for Ready because a NotReady node is not yet — or no longer — a working member of
// the pool, and a NodeClaim for one is worse than no NodeClaim at all:
//
//   - It would be counted. NodePool.status.resources and .status.nodes include the claim, and while
//     it is uninitialized StateNode.Capacity() fills any resource the node does not report from the
//     NodeClaim's — i.e. the SKU's declared figures. For a node whose kubelet never reported at all,
//     or whose Node object is later deleted while the claim lives (Cluster.cleanupNode keeps the
//     StateNode and nils out its Node), that is the *whole* capacity: Karpenter bin-packs against,
//     and charges the pool's limits for, a machine that is not there.
//   - It would be stuck. Initialization requires NodeReady, so the claim would sit at
//     Registered=True / Initialized=Unknown("NodeNotReady") — and Liveness only deletes claims that
//     fail to *register*, which this one did not. Nothing else reaps it either, short of
//     spec.expireAfter (720h from the pool template), so a node that never comes up leaves a phantom
//     NodeClaim for 30 days.
//   - It may not be a node at all. A machine mid-teardown, or one whose kubelet never joined
//     properly, presents exactly as NotReady; adopting it writes an immutable spec.providerID onto
//     an object that is on its way out.
//
// Waiting costs nothing: the controller's Node watch fires on the status update that flips the
// condition to True, so the node is adopted seconds after it becomes Ready rather than at the next
// resync. A node that is Ready and later goes NotReady keeps its NodeClaim — this gate is only about
// when to take a node on, and from then on Karpenter's disruption path owns it. (Not its node-repair
// path: that controller is only registered when CloudProvider.RepairPolicies() is non-empty, and
// this provider returns nil.)
func isReady(node *corev1.Node) bool {
	return nodeutils.GetCondition(node, corev1.NodeReady).Status == corev1.ConditionTrue
}

// isControlPlane reports whether the node is a control-plane member, by either the current or the
// legacy role label.
func isControlPlane(node *corev1.Node) bool {
	if _, ok := node.Labels["node-role.kubernetes.io/control-plane"]; ok {
		return true
	}
	_, ok := node.Labels["node-role.kubernetes.io/master"]
	return ok
}
