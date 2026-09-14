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

// Package nodeproviderid provides a controller that resolves the real Kubernetes node ProviderID
// for NodeClaims that were created with a synthetic pending ProviderID.
//
// Flow:
//  1. CloudProvider.Create() returns immediately after broker ACK with ProviderID = "rafay://pending/<uid>".
//  2. This controller watches NodeClaims and Nodes.
//  3. When a node with matching nodepoolname+sku_name labels appears, it patches the NodeClaim's
//     Status.ProviderID to the real node ProviderID (see cprovider.NodeClaimSKUs for how sku_name is matched).
//  4. Karpenter's Registration reconciler then finds the matching node and sets Registered=True.
//
// On pod restart, the NodeClaim watch ensures all existing pending NodeClaims are re-enqueued,
// so no state is kept in memory — NodeClaims are the source of truth.
package nodeproviderid

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
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

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
)

const (
	nodepoolNameLabel   = "nodepoolname"
	skuNameLabel        = "sku_name"
	nodeWaitRequeueTime = 30 * time.Second
)

// Controller watches NodeClaims with synthetic pending ProviderIDs and patches them with
// the real node ProviderID once the node joins the cluster.
//
// MaxConcurrentReconciles is 1 to ensure the read-decide-patch cycle for node assignment
// is never executed concurrently, preventing two NodeClaims from claiming the same node.
//
// apiReader is an uncached client that reads directly from the API server. It is used for
// the NodeClaim list that builds usedIDs, bypassing the informer cache. This prevents a
// stale-cache race where NodeClaim A's freshly-patched ProviderID is not yet visible in the
// cache when NodeClaim B's reconcile runs, which would allow both to claim the same node.
type Controller struct {
	kubeClient client.Client
	apiReader  client.Reader
}

func NewController(kubeClient client.Client) *Controller {
	return &Controller{kubeClient: kubeClient}
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	// Store the uncached API reader; must be set before any reconcile runs.
	c.apiReader = m.GetAPIReader()

	// rafayNodePredicate filters node events to only those where the node already has a real
	// spec.providerID and the Rafay labels. This avoids triggering reconciles for:
	//   - node updates that don't affect ProviderID or labels (e.g. status heartbeats)
	//   - node deletes
	//   - nodes from other providers
	rafayNodePredicate := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return false
		}
		return node.Spec.ProviderID != "" &&
			node.Labels[nodepoolNameLabel] != "" &&
			node.Labels[skuNameLabel] != ""
	})

	return controllerruntime.NewControllerManagedBy(m).
		Named("nodeclaim.nodeproviderid").
		// Primary watch: reconcile NodeClaims. On pod restart, all existing NodeClaims
		// with pending ProviderIDs are automatically re-enqueued by controller-runtime.
		For(&karpv1.NodeClaim{}).
		// Secondary watch: when a real Rafay node appears, immediately trigger reconciliation
		// of matching pending NodeClaims without waiting for the 30s requeue timer.
		Watches(&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(c.nodesToNodeClaims),
			builder.WithPredicates(rafayNodePredicate),
		).
		WithOptions(crcontroller.Options{MaxConcurrentReconciles: 1}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func (c *Controller) Reconcile(ctx context.Context, nodeClaim *karpv1.NodeClaim) (reconcile.Result, error) {
	// Skip NodeClaims that are not in the pending state or are being deleted.
	if !strings.HasPrefix(nodeClaim.Status.ProviderID, cprovider.PendingProviderIDPrefix) {
		return reconcile.Result{}, nil
	}
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}

	nodePoolName := nodeClaim.Labels[karpv1.NodePoolLabelKey]
	skus := cprovider.NodeClaimSKUs(nodeClaim)
	if nodePoolName == "" || len(skus) == 0 {
		klog.Warningf("nodeproviderid: nodeclaim %s missing nodepool/sku labels, skipping", nodeClaim.Name)
		return reconcile.Result{}, nil
	}

	// Read the current NodeClaim list directly from the API server (not the informer cache).
	// This is critical: after patching NodeClaim A's ProviderID, the informer cache may not
	// yet reflect the change when NodeClaim B's reconcile starts. Reading from the API server
	// guarantees we see A's real ProviderID in usedIDs, preventing double-assignment.
	var claimList karpv1.NodeClaimList
	if err := c.apiReader.List(ctx, &claimList); err != nil {
		return reconcile.Result{}, err
	}
	usedIDs := make(map[string]bool, len(claimList.Items))
	for i := range claimList.Items {
		pid := claimList.Items[i].Status.ProviderID
		if pid != "" && !strings.HasPrefix(pid, cprovider.PendingProviderIDPrefix) {
			usedIDs[pid] = true
		}
	}

	// List nodes in this nodepool. sku_name cannot be a server-side exact-match selector because
	// a NodeClaim may legitimately match more than one sku_name value (see cprovider.NodeClaimSKUs), so it
	// is filtered in code below.
	var nodeList corev1.NodeList
	if err := c.kubeClient.List(ctx, &nodeList, client.MatchingLabels{
		nodepoolNameLabel: nodePoolName,
	}); err != nil {
		return reconcile.Result{}, err
	}

	// Find the first unclaimed node with a matching sku that was created after this NodeClaim.
	// The creation-time guard ensures we don't claim a pre-existing node from a different workload.
	var found string
	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		if !skus[n.Labels[skuNameLabel]] {
			continue
		}
		pid := n.Spec.ProviderID
		if pid == "" || usedIDs[pid] {
			continue
		}
		if !n.CreationTimestamp.After(nodeClaim.CreationTimestamp.Time) {
			continue
		}
		found = pid
		break
	}

	if found == "" {
		// Node not yet visible; requeue and check again.
		return reconcile.Result{RequeueAfter: nodeWaitRequeueTime}, nil
	}

	// Patch the NodeClaim's ProviderID with the real value. Optimistic locking ensures
	// that if two reconciles race (shouldn't happen at concurrency=1, but defensive), only
	// one succeeds and the other retries with fresh data.
	stored := nodeClaim.DeepCopy()
	nodeClaim.Status.ProviderID = found
	if err := c.kubeClient.Status().Patch(ctx, nodeClaim,
		client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
		if kerrors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		// NotFound means the NodeClaim was deleted between the DeletionTimestamp check above
		// and the patch. This is a normal race during disruption; ignore it cleanly.
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	klog.Infof("nodeproviderid: assigned providerID=%s to nodeclaim=%s", found, nodeClaim.Name)
	return reconcile.Result{}, nil
}

// nodesToNodeClaims maps Node events to NodeClaim reconcile requests.
// Called only for nodes that passed rafayNodePredicate (real ProviderID + Rafay labels).
func (c *Controller) nodesToNodeClaims(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}
	nodePoolName := node.Labels[nodepoolNameLabel]
	skuName := node.Labels[skuNameLabel]
	if nodePoolName == "" || skuName == "" {
		return nil
	}

	var claimList karpv1.NodeClaimList
	if err := c.kubeClient.List(ctx, &claimList); err != nil {
		klog.Warningf("nodeproviderid: list nodeclaims for node %s: %v", node.Name, err)
		return nil
	}

	var requests []reconcile.Request
	for i := range claimList.Items {
		nc := &claimList.Items[i]
		if !strings.HasPrefix(nc.Status.ProviderID, cprovider.PendingProviderIDPrefix) {
			continue
		}
		if nc.Labels[karpv1.NodePoolLabelKey] != nodePoolName {
			continue
		}
		if !cprovider.NodeClaimSKUs(nc)[skuName] {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: nc.Name},
		})
	}
	return requests
}
