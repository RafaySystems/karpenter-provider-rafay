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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
)

// baseTime is a fixed, second-truncated reference time so CreationTimestamp
// comparisons are deterministic.
var baseTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

func newTestClient(objs ...client.Object) client.Client {
	// karpv1's package init registers NodeClaim/NodeClaimList into the default
	// client-go scheme, which also contains corev1.
	return fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithStatusSubresource(&karpv1.NodeClaim{}).
		WithObjects(objs...).
		Build()
}

func newTestController(c client.Client) *Controller {
	// Register() normally sets apiReader from the manager; in tests we set both
	// kubeClient and apiReader to the same fake client directly.
	ctrl := NewController(c)
	ctrl.apiReader = c
	return ctrl
}

func newNodeClaim(name, pool, sku, providerID string, created time.Time) *karpv1.NodeClaim {
	nc := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{
				karpv1.NodePoolLabelKey: pool,
			},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{
				Kind:  "RafayNodeClass",
				Name:  sku,
				Group: "karpenter.rafay.dev",
			},
		},
		Status: karpv1.NodeClaimStatus{
			ProviderID: providerID,
		},
	}
	return nc
}

// withInstanceType stamps the node.kubernetes.io/instance-type label that
// CloudProvider.Create() applies from the SELECTED instance type (via requirementsToLabels).
// For a multi-instanceType RafayNodeClass this value differs from the NodeClass name and is
// what the platform writes into the node's sku_name label.
func withInstanceType(nc *karpv1.NodeClaim, instanceType string) *karpv1.NodeClaim {
	nc.Labels[corev1.LabelInstanceTypeStable] = instanceType
	return nc
}

func newNode(name, pool, sku, providerID string, created time.Time) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{
				nodepoolNameLabel: pool,
				skuNameLabel:      sku,
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: providerID,
		},
	}
}

func getNodeClaim(t *testing.T, c client.Client, name string) *karpv1.NodeClaim {
	t.Helper()
	nc := &karpv1.NodeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, nc); err != nil {
		t.Fatalf("get nodeclaim %s: %v", name, err)
	}
	return nc
}

// 1. Happy path: a pending NodeClaim and a matching node created after the claim
// results in the NodeClaim's Status.ProviderID being patched to the node's ProviderID.
func TestReconcileHappyPath(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	realID := "rafay://cluster-1/node-1"

	claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
	node := newNode("node-1", "pool-1", "sku-1", realID, baseTime.Add(time.Minute))

	c := newTestClient(claim, node)
	ctrl := newTestController(c)

	got := getNodeClaim(t, c, "claim-a")
	res, err := ctrl.Reconcile(context.Background(), got)
	if err != nil {
		t.Fatalf("reconcile: unexpected error: %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Fatalf("reconcile: expected zero result, got %+v", res)
	}

	updated := getNodeClaim(t, c, "claim-a")
	if updated.Status.ProviderID != realID {
		t.Fatalf("expected ProviderID %q, got %q", realID, updated.Status.ProviderID)
	}
}

// 2. usedIDs exclusion: a node whose ProviderID is already owned by another
// non-pending NodeClaim must not be assigned to a pending claim.
func TestReconcileSkipsNodeOwnedByAnotherClaim(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	ownedID := "rafay://cluster-1/node-1"

	pendingClaim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
	ownerClaim := newNodeClaim("claim-b", "pool-1", "sku-1", ownedID, baseTime.Add(-time.Hour))
	node := newNode("node-1", "pool-1", "sku-1", ownedID, baseTime.Add(time.Minute))

	c := newTestClient(pendingClaim, ownerClaim, node)
	ctrl := newTestController(c)

	got := getNodeClaim(t, c, "claim-a")
	res, err := ctrl.Reconcile(context.Background(), got)
	if err != nil {
		t.Fatalf("reconcile: unexpected error: %v", err)
	}
	if res.RequeueAfter != nodeWaitRequeueTime {
		t.Fatalf("expected requeue after %v, got %+v", nodeWaitRequeueTime, res)
	}

	updated := getNodeClaim(t, c, "claim-a")
	if updated.Status.ProviderID != pendingID {
		t.Fatalf("expected ProviderID to remain pending %q, got %q", pendingID, updated.Status.ProviderID)
	}
}

// 3. CreationTimestamp guard: nodes created before, or at the same instant as,
// the NodeClaim must not be matched; the reconcile requeues instead.
func TestReconcileIgnoresNodesCreatedBeforeOrAtClaim(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"

	cases := []struct {
		name        string
		nodeCreated time.Time
	}{
		{name: "node created before claim", nodeCreated: baseTime.Add(-time.Minute)},
		{name: "node created at same time as claim", nodeCreated: baseTime},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
			node := newNode("node-1", "pool-1", "sku-1", "rafay://cluster-1/node-1", tc.nodeCreated)

			c := newTestClient(claim, node)
			ctrl := newTestController(c)

			got := getNodeClaim(t, c, "claim-a")
			res, err := ctrl.Reconcile(context.Background(), got)
			if err != nil {
				t.Fatalf("reconcile: unexpected error: %v", err)
			}
			if res.RequeueAfter != nodeWaitRequeueTime {
				t.Fatalf("expected requeue after %v, got %+v", nodeWaitRequeueTime, res)
			}

			updated := getNodeClaim(t, c, "claim-a")
			if updated.Status.ProviderID != pendingID {
				t.Fatalf("expected ProviderID to remain pending %q, got %q", pendingID, updated.Status.ProviderID)
			}
		})
	}
}

// 4. Non-pending or deleting NodeClaims are skipped without touching the node assignment.
func TestReconcileSkipsNonPendingAndDeletingClaims(t *testing.T) {
	t.Run("non-pending claim", func(t *testing.T) {
		realID := "rafay://cluster-1/node-0"
		claim := newNodeClaim("claim-a", "pool-1", "sku-1", realID, baseTime)
		// A matching, unclaimed node exists; it must NOT be assigned since the
		// claim already has a real ProviderID.
		node := newNode("node-1", "pool-1", "sku-1", "rafay://cluster-1/node-1", baseTime.Add(time.Minute))

		c := newTestClient(claim, node)
		ctrl := newTestController(c)

		got := getNodeClaim(t, c, "claim-a")
		res, err := ctrl.Reconcile(context.Background(), got)
		if err != nil {
			t.Fatalf("reconcile: unexpected error: %v", err)
		}
		if res != (reconcile.Result{}) {
			t.Fatalf("expected zero result, got %+v", res)
		}

		updated := getNodeClaim(t, c, "claim-a")
		if updated.Status.ProviderID != realID {
			t.Fatalf("expected ProviderID to remain %q, got %q", realID, updated.Status.ProviderID)
		}
	})

	t.Run("deleting claim", func(t *testing.T) {
		pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
		claim := newNodeClaim("claim-a", "pool-1", "sku-1", pendingID, baseTime)
		// A finalizer is required so the fake client retains the object while
		// its DeletionTimestamp is set.
		claim.Finalizers = []string{"karpenter.sh/termination"}
		now := metav1.NewTime(baseTime.Add(2 * time.Minute))
		claim.DeletionTimestamp = &now
		node := newNode("node-1", "pool-1", "sku-1", "rafay://cluster-1/node-1", baseTime.Add(time.Minute))

		c := newTestClient(claim, node)
		ctrl := newTestController(c)

		got := getNodeClaim(t, c, "claim-a")
		res, err := ctrl.Reconcile(context.Background(), got)
		if err != nil {
			t.Fatalf("reconcile: unexpected error: %v", err)
		}
		if res != (reconcile.Result{}) {
			t.Fatalf("expected zero result, got %+v", res)
		}

		updated := getNodeClaim(t, c, "claim-a")
		if updated.Status.ProviderID != pendingID {
			t.Fatalf("expected ProviderID to remain pending %q, got %q", pendingID, updated.Status.ProviderID)
		}
	})
}

// 5. Multi-SKU RafayNodeClass: the node's sku_name is the SELECTED instance type
// ("oci-inst"), which does not equal the NodeClass name ("worker-class"). The claim must
// still match via its node.kubernetes.io/instance-type label.
func TestReconcileMatchesMultiSKUInstanceTypeLabel(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	realID := "rafay://worker-pool-amd/oci-inst/host-w1-e6a5c"

	claim := withInstanceType(
		newNodeClaim("claim-a", "worker-pool-amd", "worker-class", pendingID, baseTime),
		"oci-inst",
	)
	node := newNode("node-1", "worker-pool-amd", "oci-inst", realID, baseTime.Add(time.Minute))

	c := newTestClient(claim, node)
	ctrl := newTestController(c)

	got := getNodeClaim(t, c, "claim-a")
	res, err := ctrl.Reconcile(context.Background(), got)
	if err != nil {
		t.Fatalf("reconcile: unexpected error: %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Fatalf("reconcile: expected zero result, got %+v", res)
	}

	updated := getNodeClaim(t, c, "claim-a")
	if updated.Status.ProviderID != realID {
		t.Fatalf("expected ProviderID %q, got %q", realID, updated.Status.ProviderID)
	}
}

// 6. Legacy single-SKU convention: the NodeClass is named after its one SKU and the NodeClaim
// carries no instance-type label. Matching on NodeClassRef.Name must still work.
func TestReconcileMatchesLegacyNodeClassNameSKU(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"
	realID := "rafay://worker-pool-amd/oci-inst/host-w1-e6a5c"

	claim := newNodeClaim("claim-a", "worker-pool-amd", "oci-inst", pendingID, baseTime)
	if _, ok := claim.Labels[corev1.LabelInstanceTypeStable]; ok {
		t.Fatalf("test setup: legacy claim must not carry an instance-type label")
	}
	node := newNode("node-1", "worker-pool-amd", "oci-inst", realID, baseTime.Add(time.Minute))

	c := newTestClient(claim, node)
	ctrl := newTestController(c)

	got := getNodeClaim(t, c, "claim-a")
	res, err := ctrl.Reconcile(context.Background(), got)
	if err != nil {
		t.Fatalf("reconcile: unexpected error: %v", err)
	}
	if res != (reconcile.Result{}) {
		t.Fatalf("reconcile: expected zero result, got %+v", res)
	}

	updated := getNodeClaim(t, c, "claim-a")
	if updated.Status.ProviderID != realID {
		t.Fatalf("expected ProviderID %q, got %q", realID, updated.Status.ProviderID)
	}
}

// 7. A node in the right nodepool whose sku_name matches neither the claim's instance-type
// label nor its NodeClass name must not be claimed.
func TestReconcileIgnoresNonMatchingSKU(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-a"

	claim := withInstanceType(
		newNodeClaim("claim-a", "worker-pool-amd", "worker-class", pendingID, baseTime),
		"oci-inst",
	)
	// Same nodepool, different SKU (e.g. a node provisioned for another NodeClass).
	node := newNode("node-1", "worker-pool-amd", "gpu-inst",
		"rafay://worker-pool-amd/gpu-inst/host-g1-11ab", baseTime.Add(time.Minute))

	c := newTestClient(claim, node)
	ctrl := newTestController(c)

	got := getNodeClaim(t, c, "claim-a")
	res, err := ctrl.Reconcile(context.Background(), got)
	if err != nil {
		t.Fatalf("reconcile: unexpected error: %v", err)
	}
	if res.RequeueAfter != nodeWaitRequeueTime {
		t.Fatalf("expected requeue after %v, got %+v", nodeWaitRequeueTime, res)
	}

	updated := getNodeClaim(t, c, "claim-a")
	if updated.Status.ProviderID != pendingID {
		t.Fatalf("expected ProviderID to remain pending %q, got %q", pendingID, updated.Status.ProviderID)
	}
}

// 8. nodesToNodeClaims maps a node event to reconcile requests for pending
// NodeClaims with matching nodepool and sku only.
func TestNodesToNodeClaims(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-"

	matching := newNodeClaim("claim-match", "pool-1", "sku-1", pendingID+"1", baseTime)
	wrongPool := newNodeClaim("claim-wrong-pool", "pool-2", "sku-1", pendingID+"2", baseTime)
	wrongSku := newNodeClaim("claim-wrong-sku", "pool-1", "sku-2", pendingID+"3", baseTime)
	nonPending := newNodeClaim("claim-non-pending", "pool-1", "sku-1", "rafay://cluster-1/node-9", baseTime)
	nilClassRef := newNodeClaim("claim-nil-classref", "pool-1", "sku-1", pendingID+"4", baseTime)
	nilClassRef.Spec.NodeClassRef = nil

	c := newTestClient(matching, wrongPool, wrongSku, nonPending, nilClassRef)
	ctrl := newTestController(c)

	node := newNode("node-1", "pool-1", "sku-1", "rafay://cluster-1/node-1", baseTime.Add(time.Minute))
	requests := ctrl.nodesToNodeClaims(context.Background(), node)

	if len(requests) != 1 {
		t.Fatalf("expected exactly 1 request, got %d: %+v", len(requests), requests)
	}
	if requests[0].Name != "claim-match" {
		t.Fatalf("expected request for claim-match, got %q", requests[0].Name)
	}

	t.Run("node missing labels returns nil", func(t *testing.T) {
		unlabeled := newNode("node-2", "", "", "rafay://cluster-1/node-2", baseTime.Add(time.Minute))
		unlabeled.Labels = map[string]string{}
		if got := ctrl.nodesToNodeClaims(context.Background(), unlabeled); got != nil {
			t.Fatalf("expected nil requests for unlabeled node, got %+v", got)
		}
	})

	t.Run("non-node object returns nil", func(t *testing.T) {
		if got := ctrl.nodesToNodeClaims(context.Background(), &corev1.Pod{}); got != nil {
			t.Fatalf("expected nil requests for non-node object, got %+v", got)
		}
	})
}

// 9. nodesToNodeClaims must map a node whose sku_name is the selected instance type of a
// multi-SKU NodeClass, and must not map claims whose SKUs don't match.
func TestNodesToNodeClaimsMultiSKU(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid-"

	// Matches via node.kubernetes.io/instance-type (NodeClass name "worker-class" != sku).
	byInstanceType := withInstanceType(
		newNodeClaim("claim-instance-type", "worker-pool-amd", "worker-class", pendingID+"1", baseTime),
		"oci-inst",
	)
	// Matches via the legacy NodeClass-name convention.
	byNodeClassName := newNodeClaim("claim-legacy", "worker-pool-amd", "oci-inst", pendingID+"2", baseTime)
	// Same NodeClass, different selected instance type: must not match.
	otherInstanceType := withInstanceType(
		newNodeClaim("claim-other-sku", "worker-pool-amd", "worker-class", pendingID+"3", baseTime),
		"gpu-inst",
	)

	c := newTestClient(byInstanceType, byNodeClassName, otherInstanceType)
	ctrl := newTestController(c)

	node := newNode("node-1", "worker-pool-amd", "oci-inst",
		"rafay://worker-pool-amd/oci-inst/host-w1-e6a5c", baseTime.Add(time.Minute))
	requests := ctrl.nodesToNodeClaims(context.Background(), node)

	gotNames := map[string]bool{}
	for _, r := range requests {
		gotNames[r.Name] = true
	}
	if len(requests) != 2 || !gotNames["claim-instance-type"] || !gotNames["claim-legacy"] {
		t.Fatalf("expected requests for claim-instance-type and claim-legacy only, got %+v", requests)
	}
}
