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

package nodeproviderid

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
)

// adNewInterceptedClient is newTestClient with interceptor funcs, for driving the error branches
// of the status patch.
func adNewInterceptedClient(funcs interceptor.Funcs, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithStatusSubresource(&karpv1.NodeClaim{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
}

// adReconcile runs one reconcile of the named NodeClaim read fresh from the client.
func adReconcile(t *testing.T, ctrl *Controller, c client.Client, name string) (reconcile.Result, error) {
	t.Helper()
	return ctrl.Reconcile(context.Background(), getNodeClaim(t, c, name))
}

// TestAdReconcileSkipsNodeReservedByAdoptionAnnotation: an adopted NodeClaim has an empty status
// until Karpenter's Launch reconciler calls Create(); in that window
// karpenter.rafay.io/adopted-provider-id is the only record that the node is taken (the adoption
// controller reads it into claimedIDs for exactly this reason). A pending claim reconciled in that
// window must not bind the reserved node, or two NodeClaims end up resolving to one machine.
func TestAdReconcileSkipsNodeReservedByAdoptionAnnotation(t *testing.T) {

	pendingID := cprovider.PendingProviderIDPrefix + "uid-p"
	nodeID := "rafay://pool-1/sku-1/node-n"

	pending := newNodeClaim("claim-p", "pool-1", "sku-1", pendingID, baseTime)
	adopted := newNodeClaim("claim-a", "pool-1", "sku-1", "", baseTime.Add(2*time.Minute))
	adopted.Annotations = map[string]string{cprovider.AdoptedProviderIDAnnotationKey: nodeID}
	node := newNode("node-n", "pool-1", "sku-1", nodeID, baseTime.Add(time.Minute))

	c := newTestClient(pending, adopted, node)
	ctrl := newTestController(c)

	res, err := adReconcile(t, ctrl, c, "claim-p")
	if err != nil {
		t.Fatalf("reconcile: unexpected error: %v", err)
	}
	if res.RequeueAfter != nodeWaitRequeueTime {
		t.Fatalf("expected requeue after %v (node is reserved by the adopted claim), got %+v", nodeWaitRequeueTime, res)
	}
	if got := getNodeClaim(t, c, "claim-p").Status.ProviderID; got != pendingID {
		t.Fatalf("pending claim was bound to the adopted node: ProviderID = %q, want %q", got, pendingID)
	}
}

// TestAdReconcileTwoPendingClaimsOneNode is the invariant the uncached apiReader read exists for:
// once the first claim is bound, the second reconcile must see the node in usedIDs and wait for
// its own node instead of double-binding.
func TestAdReconcileTwoPendingClaimsOneNode(t *testing.T) {
	nodeID := "rafay://pool-1/sku-1/node-1"
	p1 := newNodeClaim("claim-1", "pool-1", "sku-1", cprovider.PendingProviderIDPrefix+"uid-1", baseTime)
	p2 := newNodeClaim("claim-2", "pool-1", "sku-1", cprovider.PendingProviderIDPrefix+"uid-2", baseTime)
	node := newNode("node-1", "pool-1", "sku-1", nodeID, baseTime.Add(time.Minute))

	c := newTestClient(p1, p2, node)
	ctrl := newTestController(c)

	res, err := adReconcile(t, ctrl, c, "claim-1")
	if err != nil {
		t.Fatalf("reconcile claim-1: %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Fatalf("reconcile claim-1: expected zero result, got %+v", res)
	}
	if got := getNodeClaim(t, c, "claim-1").Status.ProviderID; got != nodeID {
		t.Fatalf("claim-1 ProviderID = %q, want %q", got, nodeID)
	}

	res, err = adReconcile(t, ctrl, c, "claim-2")
	if err != nil {
		t.Fatalf("reconcile claim-2: %v", err)
	}
	if res.RequeueAfter != nodeWaitRequeueTime {
		t.Fatalf("reconcile claim-2: expected requeue after %v, got %+v", nodeWaitRequeueTime, res)
	}
	if got := getNodeClaim(t, c, "claim-2").Status.ProviderID; got != cprovider.PendingProviderIDPrefix+"uid-2" {
		t.Fatalf("claim-2 was double-bound: ProviderID = %q", got)
	}
}

// TestAdReconcileSkipsNodeWithEmptyProviderID pins the current decision at the pid == "" guard: a
// Ready, labelled node newer than the claim but with no spec.providerID is not bound (there is
// nothing to bind to), so the claim keeps waiting. Filling in the ID is the adoption controller's
// job, and today it defers to the pending claim for such a node — see
// R1-karpenter-core-interaction-6 for the resulting stall.
func TestAdReconcileSkipsNodeWithEmptyProviderID(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
	node := newNode("node-1", "pool-1", "sku-1", "", baseTime.Add(time.Minute))
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}

	c := newTestClient(claim, node)
	ctrl := newTestController(c)

	res, err := adReconcile(t, ctrl, c, "claim-a")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != nodeWaitRequeueTime {
		t.Fatalf("expected requeue after %v, got %+v", nodeWaitRequeueTime, res)
	}
	if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != pendingID {
		t.Fatalf("ProviderID = %q, want it to remain pending %q", got, pendingID)
	}
	if got := getNode(t, c, "node-1").Spec.ProviderID; got != "" {
		t.Fatalf("node providerID = %q, want it left empty (this controller never writes nodes)", got)
	}
}

// TestAdReconcileStatusPatchErrors covers the branches after Status().Patch: an optimistic-lock
// Conflict is a retry, a NotFound (claim deleted mid-reconcile during disruption) is ignored, and
// anything else surfaces as an error so the workqueue backs off.
func TestAdReconcileStatusPatchErrors(t *testing.T) {
	gr := schema.GroupResource{Group: "karpenter.sh", Resource: "nodeclaims"}
	other := errors.New("boom")
	tests := []struct {
		name    string
		err     error
		wantRes reconcile.Result
		wantErr error
	}{
		{name: "conflict requeues", err: kerrors.NewConflict(gr, "claim-a", errors.New("stale")), wantRes: reconcile.Result{Requeue: true}},
		{name: "not found is ignored", err: kerrors.NewNotFound(gr, "claim-a"), wantRes: reconcile.Result{}},
		{name: "other error is returned", err: other, wantRes: reconcile.Result{}, wantErr: other},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
			claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
			node := newNode("node-1", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-1", baseTime.Add(time.Minute))

			patches := 0
			c := adNewInterceptedClient(interceptor.Funcs{
				SubResourcePatch: func(context.Context, client.Client, string, client.Object, client.Patch, ...client.SubResourcePatchOption) error {
					patches++
					return tt.err
				},
			}, claim, node)
			ctrl := newTestController(c)

			res, err := adReconcile(t, ctrl, c, "claim-a")
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if res != tt.wantRes {
				t.Fatalf("result = %+v, want %+v", res, tt.wantRes)
			}
			if patches != 1 {
				t.Fatalf("status patched %d times, want 1", patches)
			}
			if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != pendingID {
				t.Fatalf("ProviderID = %q, want it still pending %q (patch failed)", got, pendingID)
			}
		})
	}
}

// TestAdReconcileListErrorsAreReturned: a failed NodeClaim (uncached) or Node list must abort the
// reconcile with the error — deciding on a partial view is how two claims end up on one node.
func TestAdReconcileListErrorsAreReturned(t *testing.T) {
	boom := errors.New("apiserver unavailable")
	for _, tt := range []struct {
		name string
		fail func(list client.ObjectList) bool
	}{
		{name: "nodeclaim list", fail: func(l client.ObjectList) bool { _, ok := l.(*karpv1.NodeClaimList); return ok }},
		{name: "node list", fail: func(l client.ObjectList) bool { _, ok := l.(*corev1.NodeList); return ok }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
			claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
			node := newNode("node-1", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-1", baseTime.Add(time.Minute))
			// The claim is fetched with Get before the reconcile, so only List is intercepted.
			c := adNewInterceptedClient(interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if tt.fail(list) {
						return boom
					}
					return cl.List(ctx, list, opts...)
				},
			}, claim, node)
			ctrl := newTestController(c)

			_, err := adReconcile(t, ctrl, c, "claim-a")
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want %v", err, boom)
			}
			if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != pendingID {
				t.Fatalf("ProviderID = %q, want it still pending %q", got, pendingID)
			}
		})
	}
}

// TestAdReconcileSkipsClaimMissingPoolOrSKU: a pending claim with no nodepool label, or with no
// way to tell its SKU, cannot be matched to any node and is dropped without a requeue.
func TestAdReconcileSkipsClaimMissingPoolOrSKU(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	for _, tt := range []struct {
		name   string
		mutate func(*karpv1.NodeClaim)
	}{
		{name: "no nodepool label", mutate: func(nc *karpv1.NodeClaim) { delete(nc.Labels, karpv1.NodePoolLabelKey) }},
		{name: "no sku", mutate: func(nc *karpv1.NodeClaim) { nc.Spec.NodeClassRef = nil }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
			tt.mutate(claim)
			node := newNode("node-1", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-1", baseTime.Add(time.Minute))
			c := newTestClient(claim, node)
			ctrl := newTestController(c)

			res, err := adReconcile(t, ctrl, c, "claim-a")
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if res != (reconcile.Result{}) {
				t.Fatalf("expected zero result, got %+v", res)
			}
			if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != pendingID {
				t.Fatalf("ProviderID = %q, want it still pending %q", got, pendingID)
			}
		})
	}
}

// TestAdReconcileBindsFirstEligibleNodeOnly: with several candidate nodes only one is bound per
// reconcile, and it is one that is newer than the claim, unclaimed and of the right SKU.
func TestAdReconcileBindsFirstEligibleNodeOnly(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
	tooOld := newNode("node-old", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-old", baseTime.Add(-time.Minute))
	wrongSKU := newNode("node-gpu", "pool-1", "gpu-1", "rafay://pool-1/gpu-1/node-gpu", baseTime.Add(time.Minute))
	owned := newNode("node-owned", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-owned", baseTime.Add(time.Minute))
	owner := newNodeClaim("claim-owner", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-owned", baseTime.Add(-time.Hour))
	eligible := newNode("node-new", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-new", baseTime.Add(time.Minute))

	c := newTestClient(claim, owner, tooOld, wrongSKU, owned, eligible)
	ctrl := newTestController(c)

	if _, err := adReconcile(t, ctrl, c, "claim-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != "rafay://pool-1/sku-1/node-new" {
		t.Fatalf("ProviderID = %q, want the only eligible node", got)
	}
}

// TestAdBoundNodeGetsRegisteredLabel: Karpenter's Registration reconciler logs an error and emits
// UnregisteredTaintMissing for a node that carries neither karpenter.sh/registered nor the
// karpenter.sh/unregistered startup taint. Platform-built nodes never have the taint, so binding
// should pre-set the label the way nodeadoption.prepareNode does for adopted nodes.
func TestAdBoundNodeGetsRegisteredLabel(t *testing.T) {

	nodeID := "rafay://pool-1/sku-1/node-1"
	claim := newNodeClaim("claim-a", "pool-1", "sku-1", cprovider.PendingProviderIDPrefix+"uid-a", baseTime)
	node := newNode("node-1", "pool-1", "sku-1", nodeID, baseTime.Add(time.Minute))
	c := newTestClient(claim, node)
	ctrl := newTestController(c)

	if _, err := adReconcile(t, ctrl, c, "claim-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != nodeID {
		t.Fatalf("ProviderID = %q, want %q", got, nodeID)
	}
	if got := getNode(t, c, "node-1").Labels[karpv1.NodeRegisteredLabelKey]; got != "true" {
		t.Fatalf("node %s = %q, want \"true\" so Registration does not report a missing startup taint", karpv1.NodeRegisteredLabelKey, got)
	}
}

// TestAdNodePredicate pins the Node watch filter: only a node with a real spec.providerID and both
// Rafay labels reaches nodesToNodeClaims, and the Update that matters — the platform filling in
// spec.providerID — passes on the new object.
func TestAdNodePredicate(t *testing.T) {
	ready := func(n *corev1.Node) *corev1.Node {
		n.Labels = map[string]string{nodepoolNameLabel: "pool-1", skuNameLabel: "sku-1"}
		return n
	}
	full := ready(newNode("node-1", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-1", baseTime))
	noID := ready(newNode("node-1", "pool-1", "sku-1", "", baseTime))
	noPool := newNode("node-1", "", "sku-1", "rafay://pool-1/sku-1/node-1", baseTime)
	delete(noPool.Labels, nodepoolNameLabel)
	noSKU := newNode("node-1", "pool-1", "", "rafay://pool-1/sku-1/node-1", baseTime)
	delete(noSKU.Labels, skuNameLabel)

	t.Run("create", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			obj  client.Object
			want bool
		}{
			{name: "labelled node with providerID", obj: full, want: true},
			{name: "labelled node without providerID", obj: noID, want: false},
			{name: "node without pool label", obj: noPool, want: false},
			{name: "node without sku label", obj: noSKU, want: false},
			{name: "non-node object", obj: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}}, want: false},
		} {
			if got := rafayNodePredicate.Create(event.CreateEvent{Object: tt.obj}); got != tt.want {
				t.Errorf("%s: Create = %t, want %t", tt.name, got, tt.want)
			}
		}
	})

	t.Run("update", func(t *testing.T) {
		// The platform stamping spec.providerID onto a node that joined without one is the event
		// this watch exists for.
		if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: noID, ObjectNew: full}) {
			t.Error("Update(no providerID -> providerID set) = false, want true")
		}
		// The reverse cannot happen (providerID is immutable) but the filter looks at the new object.
		if rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: full, ObjectNew: noID}) {
			t.Error("Update(providerID -> empty) = true, want false")
		}
	})

	t.Run("generic", func(t *testing.T) {
		if !rafayNodePredicate.Generic(event.GenericEvent{Object: full}) {
			t.Error("Generic(full node) = false, want true")
		}
		if rafayNodePredicate.Generic(event.GenericEvent{Object: noID}) {
			t.Error("Generic(node without providerID) = true, want false")
		}
	})
}

func getNode(t *testing.T, c client.Client, name string) *corev1.Node {
	t.Helper()
	n := &corev1.Node{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, n); err != nil {
		t.Fatalf("get node %s: %v", name, err)
	}
	return n
}

// TestAdBoundNodeAlreadyRegisteredIsNotPatched: a node that already carries
// karpenter.sh/registered=true gets no node patch; only the NodeClaim status is written.
func TestAdBoundNodeAlreadyRegisteredIsNotPatched(t *testing.T) {
	nodeID := "rafay://pool-1/sku-1/node-1"
	claim := newNodeClaim("claim-a", "pool-1", "sku-1", cprovider.PendingProviderIDPrefix+"uid-a", baseTime)
	node := newNode("node-1", "pool-1", "sku-1", nodeID, baseTime.Add(time.Minute))
	node.Labels[karpv1.NodeRegisteredLabelKey] = "true"
	nodePatches := 0
	c := adNewInterceptedClient(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				nodePatches++
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, claim, node)
	ctrl := newTestController(c)

	if _, err := adReconcile(t, ctrl, c, "claim-a"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if nodePatches != 0 {
		t.Errorf("node patches = %d, want 0", nodePatches)
	}
	if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != nodeID {
		t.Fatalf("ProviderID = %q, want %q", got, nodeID)
	}
}

// TestAdNodeLabelPatchFailureLeavesClaimPending: the node is labelled before the claim is bound,
// so a failed label patch returns the error (controller-runtime retries) and the claim stays
// pending — nothing is half-bound.
func TestAdNodeLabelPatchFailureLeavesClaimPending(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
	node := newNode("node-1", "pool-1", "sku-1", "rafay://pool-1/sku-1/node-1", baseTime.Add(time.Minute))
	boom := errors.New("node patch rejected")
	c := adNewInterceptedClient(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				return boom
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, claim, node)
	ctrl := newTestController(c)

	_, err := adReconcile(t, ctrl, c, "claim-a")
	if !errors.Is(err, boom) {
		t.Fatalf("reconcile err = %v, want it to wrap %v", err, boom)
	}
	if got := getNodeClaim(t, c, "claim-a").Status.ProviderID; got != pendingID {
		t.Fatalf("claim was bound despite the failed node patch: ProviderID = %q", got)
	}
	if got := getNode(t, c, "node-1").Labels[karpv1.NodeRegisteredLabelKey]; got != "" {
		t.Errorf("node registered label = %q, want unset", got)
	}
}
