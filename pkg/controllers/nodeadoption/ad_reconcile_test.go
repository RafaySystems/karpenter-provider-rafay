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

package nodeadoption

import (
	"context"
	"errors"
	"strings"
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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

// adNewInterceptedClient is newTestClient with interceptor funcs, for injecting API errors.
func adNewInterceptedClient(funcs interceptor.Funcs, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithStatusSubresource(&karpv1.NodeClaim{}, &karpv1.NodePool{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
}

// adAdoptedNodes returns the set of node names the listed NodeClaims were adopted for.
func adAdoptedNodes(claims []karpv1.NodeClaim) map[string]bool {
	out := map[string]bool{}
	for i := range claims {
		if n := claims[i].Annotations[adoptedNodeAnnotationKey]; n != "" {
			out[n] = true
		}
	}
	return out
}

// TestAdReRegisteredNodeIsNotAdoptedTwice: the built provider ID is deterministic from
// pool/sku/hostname, so a Node object that is deleted and re-registered by its kubelet with an empty
// spec.providerID rebuilds the very ID its existing NodeClaim already carries. The node must get
// that ID stamped back (so the existing claim's Registration finds it) but no second NodeClaim.
func TestAdReRegisteredNodeIsNotAdoptedTwice(t *testing.T) {

	builtID := rafay.BuildProviderID(testPool, testSKU, "worker-a")
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool,
		newNode("worker-a", withCreated(baseTime.Add(time.Hour))),
		newNodeClaim("existing", testPool, builtID, baseTime),
	)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1 (the existing claim already owns %s)", len(claims), builtID)
	}
	if got := getNode(t, c, "worker-a").Spec.ProviderID; got != builtID {
		t.Errorf("node providerID = %q, want %q so the existing claim can register against it", got, builtID)
	}
}

// TestAdRejectsPoolRequirementKeyTheNodeLacks: validateAdoptable applies the same strict
// Requirements.Compatible the drift sub-controller does (no AllowUndefinedWellKnownLabels), so a
// node lacking a well-known label the pool requires is skipped — never adopted-then-drifted. This
// locks the strictness in; the doc comment on wellKnownNodeLabels describes it inaccurately
// (R1-prov-adoption-providerid-6).
func TestAdRejectsPoolRequirementKeyTheNodeLacks(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pool.Spec.Template.Spec.Requirements = append(pool.Spec.Template.Spec.Requirements,
		karpv1.NodeSelectorRequirementWithMinValues{
			Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"us-ashburn-1-ad-1"},
		})
	node := newNode("worker-a")
	if _, ok := node.Labels[corev1.LabelTopologyZone]; ok {
		t.Fatal("test setup: node must not carry a zone label")
	}
	it := newInstanceType(testSKU, "amd64")

	err := validateAdoptable(pool, it, adoptedNodeClaim(pool, node, testSKU, it).Labels)
	if err == nil {
		t.Fatal("validateAdoptable = nil, want an error for a node lacking the zone the pool requires")
	}
	if !strings.Contains(err.Error(), "do not satisfy the pool's requirements") {
		t.Errorf("validateAdoptable error = %q, want the requirements mismatch", err)
	}

	c := newTestClient(pool, node)
	ctrl, _ := newTestController(c, it)
	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 0 {
		t.Fatalf("got %d nodeclaims, want 0", len(claims))
	}
	if got := getNode(t, c, "worker-a").Spec.ProviderID; got != "" {
		t.Errorf("node providerID = %q, want it untouched", got)
	}
}

// TestAdValidateAdoptableOfferingMismatch covers the second check: labels that satisfy the pool's
// requirements but match no offering of the SKU. A node with a real zone label whose RafayNodeClass
// declares no zone (synthetic "default") is such a case — instanceTypeNotFound would drift it.
func TestAdValidateAdoptableOfferingMismatch(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	node := newNode("worker-a", withLabel(corev1.LabelTopologyZone, "us-ashburn-1-ad-1"))
	it := newInstanceType(testSKU, "amd64")

	err := validateAdoptable(pool, it, adoptedNodeClaim(pool, node, testSKU, it).Labels)
	if err == nil {
		t.Fatal("validateAdoptable = nil, want an offering mismatch for zone us-ashburn-1-ad-1 vs default")
	}
	if !strings.Contains(err.Error(), "no offering compatible") {
		t.Errorf("validateAdoptable error = %q, want the offering mismatch", err)
	}

	// The same node against a class whose offering names its zone is adoptable.
	zoned := adNewZonedInstanceType(testSKU, "amd64", "us-ashburn-1-ad-1")
	if err := validateAdoptable(pool, zoned, adoptedNodeClaim(pool, node, testSKU, zoned).Labels); err != nil {
		t.Errorf("validateAdoptable with a matching zone offering = %v, want nil", err)
	}
}

// TestAdAdoptedClaimHashStrippedAfterHashControllerStamp: the fork's nodepool hash controller
// back-fills karpenter.sh/nodepool-hash onto every managed NodeClaim of a pool that lacks the
// current hash-version annotation, which re-enables static drift for adopted nodes. If the
// exemption is made durable (fix option a), the adoption pass must strip the hash again while
// keeping the adoption annotations.
func TestAdAdoptedClaimHashStrippedAfterHashControllerStamp(t *testing.T) {

	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool, newNode("worker-a"))
	ctrl, _ := newTestController(c)
	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1", len(claims))
	}

	// What the hash controller does on a NodePoolHashVersion bump.
	stamped := claims[0].DeepCopy()
	stamped.Annotations[karpv1.NodePoolHashAnnotationKey] = pool.Hash()
	stamped.Annotations[karpv1.NodePoolHashVersionAnnotationKey] = karpv1.NodePoolHashVersion
	if err := c.Update(context.Background(), stamped); err != nil {
		t.Fatalf("stamp hash: %v", err)
	}

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims = listClaims(t, c)
	if len(claims) != 1 {
		t.Fatalf("got %d nodeclaims after re-reconcile, want 1", len(claims))
	}
	if v, ok := claims[0].Annotations[karpv1.NodePoolHashAnnotationKey]; ok {
		t.Errorf("adopted nodeclaim carries %s=%q, want it stripped", karpv1.NodePoolHashAnnotationKey, v)
	}
	if got := claims[0].Annotations[cprovider.AdoptedProviderIDAnnotationKey]; got == "" {
		t.Errorf("adopted nodeclaim lost %s", cprovider.AdoptedProviderIDAnnotationKey)
	}
}

// TestAdClaimedByPendingClaim pins the reservation rule: a pending claim reserves a node only when
// the node is newer than the claim and the claim's SKUs include the node's.
func TestAdClaimedByPendingClaim(t *testing.T) {
	pendingID := cprovider.PendingProviderIDPrefix + "uid"
	sameSKU := newNodeClaim("same", testPool, pendingID, baseTime)
	otherSKU := newNodeClaim("other", testPool, pendingID, baseTime)
	otherSKU.Labels[corev1.LabelInstanceTypeStable] = "gpu-inst"
	otherSKU.Spec.NodeClassRef.Name = "gpu-inst"
	// Multi-SKU class: NodeClass "worker-class" lists several SKUs; the claim's instance-type label
	// names the selected one.
	multi := newNodeClaim("multi", testPool, pendingID, baseTime)
	multi.Spec.NodeClassRef.Name = "worker-class"
	// Legacy single-SKU convention: no instance-type label, NodeClass named after the SKU.
	legacy := newNodeClaim("legacy", testPool, pendingID, baseTime)
	delete(legacy.Labels, corev1.LabelInstanceTypeStable)

	newer := newNode("newer", withCreated(baseTime.Add(time.Minute)))
	older := newNode("older", withCreated(baseTime.Add(-time.Minute)))
	sameTime := newNode("same-time", withCreated(baseTime))

	tests := []struct {
		name    string
		pending []*karpv1.NodeClaim
		node    *corev1.Node
		sku     string
		want    bool
	}{
		{name: "no pending claims", node: newer, sku: testSKU, want: false},
		{name: "same sku, newer node", pending: []*karpv1.NodeClaim{sameSKU}, node: newer, sku: testSKU, want: true},
		{name: "same sku, older node", pending: []*karpv1.NodeClaim{sameSKU}, node: older, sku: testSKU, want: false},
		{name: "same sku, same instant", pending: []*karpv1.NodeClaim{sameSKU}, node: sameTime, sku: testSKU, want: false},
		{name: "different sku, newer node", pending: []*karpv1.NodeClaim{otherSKU}, node: newer, sku: testSKU, want: false},
		{name: "different sku first, same sku second", pending: []*karpv1.NodeClaim{otherSKU, sameSKU}, node: newer, sku: testSKU, want: true},
		{name: "multi-sku class matches by instance-type label", pending: []*karpv1.NodeClaim{multi}, node: newer, sku: testSKU, want: true},
		{name: "legacy class matches by class name", pending: []*karpv1.NodeClaim{legacy}, node: newer, sku: testSKU, want: true},
		{name: "legacy class, other sku", pending: []*karpv1.NodeClaim{legacy}, node: newer, sku: "gpu-inst", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := claimedByPendingClaim(tt.pending, tt.node, tt.sku); got != tt.want {
				t.Errorf("claimedByPendingClaim = %t, want %t", got, tt.want)
			}
		})
	}
}

// TestAdClaimedByPendingClaimMultiSKUClassNameDoesNotMatch is the adoption-side form of
// R1-prov-cloudprovider-7: once Create() has stamped the selected instance type, a pending claim
// for a multi-SKU class must reserve only nodes of that SKU. A node whose sku_name happens to
// equal the NodeClass name (a class named after one of its SKUs) must not be held back for a
// claim that selected a different SKU.
func TestAdClaimedByPendingClaimMultiSKUClassNameDoesNotMatch(t *testing.T) {

	multi := newNodeClaim("multi", testPool, cprovider.PendingProviderIDPrefix+"uid", baseTime)
	multi.Spec.NodeClassRef.Name = "worker-class" // instance-type label stays testSKU
	newer := newNode("newer", withCreated(baseTime.Add(time.Minute)))

	if claimedByPendingClaim([]*karpv1.NodeClaim{multi}, newer, "worker-class") {
		t.Errorf("claimedByPendingClaim(sku=%q) = true, want false: the claim selected %q, the class name must not match", "worker-class", testSKU)
	}
}

// TestAdPendingClaimForOtherSKUDoesNotReserve: a scale-out for a different SKU in the same pool
// cannot be waiting for this node, so the node is adopted despite being newer than the claim.
func TestAdPendingClaimForOtherSKUDoesNotReserve(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pending := newNodeClaim("pending-gpu", testPool, cprovider.PendingProviderIDPrefix+"some-uid", baseTime)
	pending.Labels[corev1.LabelInstanceTypeStable] = "gpu-inst"
	pending.Spec.NodeClassRef.Name = "gpu-inst"
	node := newNode("worker-new", withCreated(baseTime.Add(time.Hour)))
	c := newTestClient(pool, node, pending)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 2 {
		t.Fatalf("got %d nodeclaims, want 2 (pending gpu + adopted)", len(claims))
	}
	if !adAdoptedNodes(claims)["worker-new"] {
		t.Errorf("worker-new was not adopted; claims = %+v", adAdoptedNodes(claims))
	}
}

// TestAdDeletingPendingClaimDoesNotReserve: a pending claim that is being deleted has given up on
// its node (its Delete resolves by labels or returns NotFound), so it must not keep a newer node
// from being adopted. Without a finalizer the fake client would drop the object, as the API server
// would, so one is set.
func TestAdDeletingPendingClaimDoesNotReserve(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pending := newNodeClaim("pending", testPool, cprovider.PendingProviderIDPrefix+"some-uid", baseTime)
	pending.Finalizers = []string{"karpenter.sh/termination"}
	now := metav1.NewTime(baseTime.Add(2 * time.Hour))
	pending.DeletionTimestamp = &now
	node := newNode("worker-new", withCreated(baseTime.Add(time.Hour)))
	c := newTestClient(pool, node, pending)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 2 {
		t.Fatalf("got %d nodeclaims, want 2 (deleting pending + adopted)", len(claims))
	}
	if !adAdoptedNodes(claims)["worker-new"] {
		t.Errorf("worker-new was not adopted; claims = %+v", adAdoptedNodes(claims))
	}
}

// TestAdCreateFailureDoesNotStopOtherAdoptions: a NodeClaim Create rejected for one node is
// appended to the returned error, the other nodes are still adopted in the same pass (no
// RequeueAfter: the error drives the retry), and the next pass converges without re-patching the
// node that was already prepared.
func TestAdCreateFailureDoesNotStopOtherAdoptions(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	boom := errors.New("admission webhook unavailable")
	failCreateFor := "worker-b"
	nodePatches := 0
	c := adNewInterceptedClient(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if nc, ok := obj.(*karpv1.NodeClaim); ok && nc.Annotations[adoptedNodeAnnotationKey] == failCreateFor {
				return boom
			}
			return cl.Create(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				nodePatches++
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, pool, newNode("worker-a"), newNode("worker-b"), newNode("worker-c"))
	ctrl, _ := newTestController(c)

	res, err := ctrl.Reconcile(context.Background(), pool)
	if !errors.Is(err, boom) {
		t.Fatalf("reconcile err = %v, want it to wrap %v", err, boom)
	}
	if !strings.Contains(err.Error(), "create nodeclaim for node worker-b") {
		t.Errorf("reconcile err = %q, want it to name worker-b", err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("result = %+v, want zero (the error drives the retry)", res)
	}
	claims := listClaims(t, c)
	if len(claims) != 2 {
		t.Fatalf("got %d nodeclaims, want 2 (worker-a and worker-c)", len(claims))
	}
	adopted := adAdoptedNodes(claims)
	if !adopted["worker-a"] || !adopted["worker-c"] || adopted["worker-b"] {
		t.Errorf("adopted %v, want worker-a and worker-c only", adopted)
	}
	// The failed node was prepared before the create, and stays prepared.
	builtB := rafay.BuildProviderID(testPool, testSKU, "worker-b")
	if got := getNode(t, c, "worker-b").Spec.ProviderID; got != builtB {
		t.Errorf("worker-b providerID = %q, want %q (prepared before the failed create)", got, builtB)
	}
	if nodePatches != 3 {
		t.Errorf("node patches = %d, want 3 (one per node)", nodePatches)
	}

	// Next pass: the create succeeds, and worker-b already carries providerID + registered label,
	// so prepareNode's no-op path issues no Patch.
	failCreateFor = ""
	res, err = ctrl.Reconcile(context.Background(), pool)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("second reconcile RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}
	claims = listClaims(t, c)
	if len(claims) != 3 {
		t.Fatalf("got %d nodeclaims after the retry, want 3", len(claims))
	}
	if nodePatches != 3 {
		t.Errorf("node patches after the retry = %d, want still 3 (worker-b was already prepared)", nodePatches)
	}
	var claimB *karpv1.NodeClaim
	for i := range claims {
		if claims[i].Annotations[adoptedNodeAnnotationKey] == "worker-b" {
			claimB = &claims[i]
		}
	}
	if claimB == nil {
		t.Fatal("no nodeclaim for worker-b after the retry")
	}
	if got := claimB.Annotations[cprovider.AdoptedProviderIDAnnotationKey]; got != builtB {
		t.Errorf("worker-b claim adopted providerID = %q, want %q", got, builtB)
	}
}

// TestAdNodePatchConflictIsReported: an optimistic-lock conflict on the node patch means someone
// else wrote the node; the reconcile must report it (no NodeClaim for that node), continue with the
// other nodes, and leave the retry to the error return.
func TestAdNodePatchConflictIsReported(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	gr := schema.GroupResource{Resource: "nodes"}
	c := adNewInterceptedClient(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if n, ok := obj.(*corev1.Node); ok && n.Name == "worker-a" {
				return kerrors.NewConflict(gr, n.Name, errors.New("the object has been modified"))
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, pool, newNode("worker-a"), newNode("worker-b"))
	ctrl, _ := newTestController(c)

	res, err := ctrl.Reconcile(context.Background(), pool)
	if err == nil || !kerrors.IsConflict(err) {
		t.Fatalf("reconcile err = %v, want a Conflict", err)
	}
	if !strings.Contains(err.Error(), "prepare node worker-a") {
		t.Errorf("reconcile err = %q, want it to name worker-a", err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("result = %+v, want zero", res)
	}
	claims := listClaims(t, c)
	if len(claims) != 1 || claims[0].Annotations[adoptedNodeAnnotationKey] != "worker-b" {
		t.Fatalf("nodeclaims adopted for %v, want worker-b only", adAdoptedNodes(claims))
	}
	if got := getNode(t, c, "worker-a").Spec.ProviderID; got != "" {
		t.Errorf("worker-a providerID = %q, want it left empty after the failed patch", got)
	}
}

// TestAdInstanceTypesErrorAbortsBeforeAnyNodeIsTouched: the RafayNodeClass lookup failing is a
// hard error for the whole pass, before any node has an immutable providerID stamped.
func TestAdInstanceTypesErrorAbortsBeforeAnyNodeIsTouched(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool, newNode("worker-a"), newNode("worker-b"))
	ctrl, stub := newTestController(c)
	stub.err = errors.New("nodeclass not found")

	res, err := ctrl.Reconcile(context.Background(), pool)
	if !errors.Is(err, stub.err) {
		t.Fatalf("reconcile err = %v, want it to wrap %v", err, stub.err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("result = %+v, want zero", res)
	}
	if claims := listClaims(t, c); len(claims) != 0 {
		t.Errorf("got %d nodeclaims, want 0", len(claims))
	}
	for _, name := range []string{"worker-a", "worker-b"} {
		n := getNode(t, c, name)
		if n.Spec.ProviderID != "" {
			t.Errorf("%s providerID = %q, want it left empty", name, n.Spec.ProviderID)
		}
		if _, ok := n.Labels[karpv1.NodeRegisteredLabelKey]; ok {
			t.Errorf("%s carries %s, want it unset", name, karpv1.NodeRegisteredLabelKey)
		}
	}
}

// TestAdInstanceTypesFetchedOnce: the RafayNodeClass is read lazily and once per pass, however
// many nodes are adopted, and not at all when nothing reaches the ownership checks.
func TestAdInstanceTypesFetchedOnce(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool, newNode("worker-a"), newNode("worker-b"), newNode("worker-c"))
	ctrl, stub := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 3 {
		t.Fatalf("got %d nodeclaims, want 3", len(claims))
	}
	if stub.calls != 1 {
		t.Errorf("GetInstanceTypes called %d times, want exactly 1", stub.calls)
	}

	// Steady state: every node is owned, so the class is not read again.
	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if stub.calls != 1 {
		t.Errorf("GetInstanceTypes called %d times after the steady-state pass, want still 1", stub.calls)
	}
}

// TestAdReRegisteredNodePrepareFailureIsReported: when the rebuilt ID is already claimed, the only
// write is stamping it back onto the node; a failure there is reported like any prepare failure and
// still does not produce a second NodeClaim.
func TestAdReRegisteredNodePrepareFailureIsReported(t *testing.T) {
	builtID := rafay.BuildProviderID(testPool, testSKU, "worker-a")
	pool := newNodePool(testPool, testSKU)
	boom := errors.New("node patch rejected")
	c := adNewInterceptedClient(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				return boom
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, pool,
		newNode("worker-a", withCreated(baseTime.Add(time.Hour))),
		newNodeClaim("existing", testPool, builtID, baseTime),
	)
	ctrl, _ := newTestController(c)

	_, err := ctrl.Reconcile(context.Background(), pool)
	if !errors.Is(err, boom) {
		t.Fatalf("reconcile err = %v, want it to wrap %v", err, boom)
	}
	if !strings.Contains(err.Error(), "prepare re-registered node worker-a") {
		t.Errorf("reconcile err = %q, want it to name the re-registered node", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1", len(claims))
	}
}

// TestAdHashStripTouchesOnlyAdoptedClaimsWithAHash: the strip is scoped to NodeClaims carrying the
// adoption marker AND a nodepool-hash. A Karpenter-provisioned claim keeps its hash (static drift
// is wanted there), an adopted claim without a hash is not patched at all, and a claim of another
// pool is never read.
func TestAdHashStripTouchesOnlyAdoptedClaimsWithAHash(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	provisioned := newNodeClaim("provisioned", testPool, "rafay://pool1/oci-inst/worker-p", baseTime)
	provisioned.Annotations = map[string]string{karpv1.NodePoolHashAnnotationKey: "abc"}
	adoptedNoHash := newNodeClaim("adopted-no-hash", testPool, "rafay://pool1/oci-inst/worker-a", baseTime)
	adoptedNoHash.Annotations = map[string]string{cprovider.AdoptedProviderIDAnnotationKey: "rafay://pool1/oci-inst/worker-a"}
	otherPool := newNodeClaim("other-pool", "pool2", "rafay://pool2/oci-inst/worker-o", baseTime)
	otherPool.Annotations = map[string]string{
		cprovider.AdoptedProviderIDAnnotationKey: "rafay://pool2/oci-inst/worker-o",
		karpv1.NodePoolHashAnnotationKey:         "abc",
	}
	adoptedStamped := newNodeClaim("adopted-stamped", testPool, "rafay://pool1/oci-inst/worker-s", baseTime)
	adoptedStamped.Annotations = map[string]string{
		cprovider.AdoptedProviderIDAnnotationKey: "rafay://pool1/oci-inst/worker-s",
		karpv1.NodePoolHashAnnotationKey:         "abc",
	}

	claimPatches := map[string]int{}
	c := adNewInterceptedClient(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if nc, ok := obj.(*karpv1.NodeClaim); ok {
				claimPatches[nc.Name]++
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, pool, provisioned, adoptedNoHash, otherPool, adoptedStamped)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(claimPatches) != 1 || claimPatches["adopted-stamped"] != 1 {
		t.Fatalf("nodeclaim patches = %v, want exactly one on adopted-stamped", claimPatches)
	}
	for name, wantHash := range map[string]bool{"provisioned": true, "adopted-no-hash": false, "other-pool": true, "adopted-stamped": false} {
		var nc karpv1.NodeClaim
		if err := c.Get(context.Background(), client.ObjectKey{Name: name}, &nc); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		if _, ok := nc.Annotations[karpv1.NodePoolHashAnnotationKey]; ok != wantHash {
			t.Errorf("%s carries nodepool-hash = %t, want %t", name, ok, wantHash)
		}
	}
	var stripped karpv1.NodeClaim
	if err := c.Get(context.Background(), client.ObjectKey{Name: "adopted-stamped"}, &stripped); err != nil {
		t.Fatal(err)
	}
	if got := stripped.Annotations[karpv1.NodePoolHashVersionAnnotationKey]; got != karpv1.NodePoolHashVersion {
		t.Errorf("stripped claim hash-version = %q, want %q so the hash controller leaves it alone", got, karpv1.NodePoolHashVersion)
	}
	if got := stripped.Annotations[cprovider.AdoptedProviderIDAnnotationKey]; got == "" {
		t.Error("stripped claim lost its adoption annotation")
	}
}

// TestAdHashStripFailureIsReported: a failed strip is appended to the reconcile error (so the
// error return retries it) without stopping the pass; a NotFound (claim deleted meanwhile) is not
// an error.
func TestAdHashStripFailureIsReported(t *testing.T) {
	stamp := func(name string) *karpv1.NodeClaim {
		nc := newNodeClaim(name, testPool, "rafay://pool1/oci-inst/"+name, baseTime)
		nc.Annotations = map[string]string{
			cprovider.AdoptedProviderIDAnnotationKey: "rafay://pool1/oci-inst/" + name,
			karpv1.NodePoolHashAnnotationKey:         "abc",
		}
		return nc
	}
	pool := newNodePool(testPool, testSKU)
	boom := errors.New("nodeclaim patch rejected")
	gr := schema.GroupResource{Group: "karpenter.sh", Resource: "nodeclaims"}
	c := adNewInterceptedClient(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			nc, ok := obj.(*karpv1.NodeClaim)
			if !ok {
				return cl.Patch(ctx, obj, patch, opts...)
			}
			switch nc.Name {
			case "fails":
				return boom
			case "gone":
				return kerrors.NewNotFound(gr, nc.Name)
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, pool, stamp("fails"), stamp("gone"), stamp("ok"), newNode("worker-new", withCreated(baseTime.Add(time.Hour))))
	ctrl, _ := newTestController(c)

	res, err := ctrl.Reconcile(context.Background(), pool)
	if !errors.Is(err, boom) {
		t.Fatalf("reconcile err = %v, want it to wrap %v", err, boom)
	}
	if !strings.Contains(err.Error(), "strip nodepool hash from nodeclaim fails") {
		t.Errorf("reconcile err = %q, want it to name the failing claim", err)
	}
	if strings.Contains(err.Error(), "gone") {
		t.Errorf("reconcile err = %q, want the NotFound ignored", err)
	}
	if res != (reconcile.Result{}) {
		t.Errorf("result = %+v, want zero (the error drives the retry)", res)
	}
	// The pass still completed: the third claim was stripped and the new node adopted.
	var ok karpv1.NodeClaim
	if err := c.Get(context.Background(), client.ObjectKey{Name: "ok"}, &ok); err != nil {
		t.Fatal(err)
	}
	if _, has := ok.Annotations[karpv1.NodePoolHashAnnotationKey]; has {
		t.Error("claim ok still carries nodepool-hash")
	}
	if !adAdoptedNodes(listClaims(t, c))["worker-new"] {
		t.Error("worker-new was not adopted in the same pass")
	}
}
