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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
)

// baseTime is a fixed reference so CreationTimestamp comparisons are deterministic.
var baseTime = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

const (
	testPool = "pool1"
	testSKU  = "oci-inst"
)

// stubInstanceTypes serves a fixed instance-type list, standing in for the CloudProvider's
// RafayNodeClass lookup.
type stubInstanceTypes struct {
	types []*cloudprovider.InstanceType
	err   error
	calls int
}

func (s *stubInstanceTypes) GetInstanceTypes(_ context.Context, _ *karpv1.NodePool) ([]*cloudprovider.InstanceType, error) {
	s.calls++
	return s.types, s.err
}

// newInstanceType mirrors what rafayInstanceTypesToKarpenter builds from a RafayNodeClass entry:
// requirements shared with a single on-demand offering, and the synthetic zone "default" that a
// class with no zone yields.
func newInstanceType(name, arch string) *cloudprovider.InstanceType {
	reqs := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "default"),
		scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, arch),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, "linux"),
	)
	return &cloudprovider.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    cloudprovider.Offerings{{Requirements: reqs, Price: 1, Available: true}},
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("12Gi"),
			corev1.ResourcePods:   resource.MustParse("110"),
		},
		Overhead: &cloudprovider.InstanceTypeOverhead{},
	}
}

func newTestClient(objs ...client.Object) client.Client {
	// karpv1's package init registers NodePool/NodeClaim into the default client-go scheme, which
	// also contains corev1.
	return fake.NewClientBuilder().
		WithScheme(clientgoscheme.Scheme).
		WithStatusSubresource(&karpv1.NodeClaim{}, &karpv1.NodePool{}).
		WithObjects(objs...).
		Build()
}

func newTestController(c client.Client, its ...*cloudprovider.InstanceType) (*Controller, *stubInstanceTypes) {
	if len(its) == 0 {
		its = []*cloudprovider.InstanceType{newInstanceType(testSKU, "amd64")}
	}
	stub := &stubInstanceTypes{types: its}
	ctrl := NewController(c, stub)
	// Register() normally sets apiReader from the manager; in tests both readers are the fake client.
	ctrl.apiReader = c
	return ctrl, stub
}

// newNodePool builds a NodePool shaped like the one edge-broker renders for a catalog row: pinned to
// one SKU, with the convenience "nodepool" label on the node template.
func newNodePool(name, sku string) *karpv1.NodePool {
	return &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
		Spec: karpv1.NodePoolSpec{
			Template: karpv1.NodeClaimTemplate{
				ObjectMeta: karpv1.ObjectMeta{Labels: map[string]string{"nodepool": name}},
				Spec: karpv1.NodeClaimTemplateSpec{
					Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
						{Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}},
						{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"}},
						{Key: karpv1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{karpv1.CapacityTypeOnDemand}},
						{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{sku}},
					},
					NodeClassRef: &karpv1.NodeClassReference{
						Group: v1alpha1.Group,
						Kind:  "RafayNodeClass",
						Name:  sku,
					},
				},
			},
		},
	}
}

type nodeOption func(*corev1.Node)

func withProviderID(id string) nodeOption {
	return func(n *corev1.Node) { n.Spec.ProviderID = id }
}

func withLabel(k, v string) nodeOption {
	return func(n *corev1.Node) { n.Labels[k] = v }
}

func withoutLabel(k string) nodeOption {
	return func(n *corev1.Node) { delete(n.Labels, k) }
}

func withCreated(t time.Time) nodeOption {
	return func(n *corev1.Node) { n.CreationTimestamp = metav1.NewTime(t) }
}

// withReady overrides the node's Ready condition. corev1.ConditionUnknown is what a node shows when
// the kubelet stops heartbeating; passing "" strips the condition entirely, which is how a node
// looks in the moment between being created and its kubelet first reporting.
func withReady(status corev1.ConditionStatus) nodeOption {
	return func(n *corev1.Node) {
		if status == "" {
			n.Status.Conditions = nil
			return
		}
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: status}}
	}
}

// newNode builds a worker node as the Rafay platform leaves it: pool and SKU labels, kubelet's
// arch/os/hostname labels, a Ready kubelet, and no providerID until the platform (or this
// controller) sets one.
func newNode(name string, opts ...nodeOption) *corev1.Node {
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(baseTime),
			Labels: map[string]string{
				nodepoolNameLabel:      testPool,
				skuNameLabel:           testSKU,
				corev1.LabelArchStable: "amd64",
				corev1.LabelOSStable:   "linux",
				corev1.LabelHostname:   name,
			},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, o := range opts {
		o(n)
	}
	return n
}

func newNodeClaim(name, pool, providerID string, created time.Time) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(created),
			Labels:            map[string]string{karpv1.NodePoolLabelKey: pool, corev1.LabelInstanceTypeStable: testSKU},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "RafayNodeClass", Name: testSKU},
		},
		Status: karpv1.NodeClaimStatus{ProviderID: providerID},
	}
}

func listClaims(t *testing.T, c client.Client) []karpv1.NodeClaim {
	t.Helper()
	var l karpv1.NodeClaimList
	if err := c.List(context.Background(), &l); err != nil {
		t.Fatalf("list nodeclaims: %v", err)
	}
	return l.Items
}

func getNode(t *testing.T, c client.Client, name string) *corev1.Node {
	t.Helper()
	var n corev1.Node
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &n); err != nil {
		t.Fatalf("get node %s: %v", name, err)
	}
	return &n
}

// TestAdoptsExistingNodes is the case the controller exists for: a pool created over nodes the
// platform already built. Each node must end up with a provider ID and exactly one NodeClaim.
func TestAdoptsExistingNodes(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool, newNode("worker-a"), newNode("worker-b"))
	ctrl, _ := newTestController(c)

	res, err := ctrl.Reconcile(context.Background(), pool)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}

	claims := listClaims(t, c)
	if len(claims) != 2 {
		t.Fatalf("got %d nodeclaims, want 2", len(claims))
	}
	seen := map[string]bool{}
	for i := range claims {
		nc := &claims[i]
		adoptedID := nc.Annotations[cprovider.AdoptedProviderIDAnnotationKey]
		if adoptedID == "" {
			t.Fatalf("nodeclaim %s has no %s annotation", nc.Name, cprovider.AdoptedProviderIDAnnotationKey)
		}
		seen[adoptedID] = true

		// The node must carry the same ID, so Karpenter's Registration can match the two.
		nodeName := nc.Annotations[adoptedNodeAnnotationKey]
		if got := getNode(t, c, nodeName).Spec.ProviderID; got != adoptedID {
			t.Errorf("node %s providerID = %q, want %q", nodeName, got, adoptedID)
		}
		if got := getNode(t, c, nodeName).Labels[karpv1.NodeRegisteredLabelKey]; got != "true" {
			t.Errorf("node %s %s = %q, want \"true\"", nodeName, karpv1.NodeRegisteredLabelKey, got)
		}
		if got := nc.Labels[karpv1.NodePoolLabelKey]; got != testPool {
			t.Errorf("nodeclaim %s nodepool label = %q, want %q", nc.Name, got, testPool)
		}
		if got := nc.Labels[corev1.LabelInstanceTypeStable]; got != testSKU {
			t.Errorf("nodeclaim %s instance-type label = %q, want %q", nc.Name, got, testSKU)
		}
		if len(nc.OwnerReferences) != 1 || nc.OwnerReferences[0].UID != pool.UID {
			t.Errorf("nodeclaim %s owner references = %+v, want one pointing at the NodePool", nc.Name, nc.OwnerReferences)
		}
	}
	for _, want := range []string{
		"rafay://pool1/oci-inst/worker-a",
		"rafay://pool1/oci-inst/worker-b",
	} {
		if !seen[want] {
			t.Errorf("no nodeclaim adopted providerID %q (got %v)", want, seen)
		}
	}
}

// TestKeepsExistingProviderID checks a node the platform already stamped is adopted as-is: the
// provider ID is immutable once set, so it must be read, not rebuilt.
func TestKeepsExistingProviderID(t *testing.T) {
	const platformID = "rafay://pool1/oci-inst/test-auto-2-e-e949w9-w0-af1cc"
	pool := newNodePool(testPool, testSKU)
	node := newNode("test-auto-2-e-e949w9-w0-af1cc", withProviderID(platformID))
	c := newTestClient(pool, node)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1", len(claims))
	}
	if got := claims[0].Annotations[cprovider.AdoptedProviderIDAnnotationKey]; got != platformID {
		t.Errorf("adopted providerID = %q, want %q", got, platformID)
	}
	if got := getNode(t, c, node.Name).Spec.ProviderID; got != platformID {
		t.Errorf("node providerID = %q, want it untouched (%q)", got, platformID)
	}
}

// TestSkipsNodeOwnedByExistingClaim covers the steady state: a node Karpenter provisioned already
// has a NodeClaim, and re-running adoption must not add a second one.
func TestSkipsNodeOwnedByExistingClaim(t *testing.T) {
	const providerID = "rafay://pool1/oci-inst/worker-a"
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool,
		newNode("worker-a", withProviderID(providerID)),
		newNodeClaim("existing", testPool, providerID, baseTime),
	)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1 (no duplicate)", len(claims))
	}
}

// TestSkipsNodeOwnedByAdoptionAnnotation is the double-adoption guard. An adopted NodeClaim has an
// empty status until Karpenter's Launch reconciler runs, so during that window the annotation is the
// only evidence the node is taken.
func TestSkipsNodeOwnedByAdoptionAnnotation(t *testing.T) {
	const providerID = "rafay://pool1/oci-inst/worker-a"
	pool := newNodePool(testPool, testSKU)
	claim := newNodeClaim("adopted", testPool, "", baseTime)
	claim.Annotations = map[string]string{cprovider.AdoptedProviderIDAnnotationKey: providerID}
	c := newTestClient(pool, newNode("worker-a", withProviderID(providerID)), claim)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1 (annotation must count as owned)", len(claims))
	}
}

// TestSkipsNodeAPendingClaimIsWaitingFor guards the scale-out path: a node newer than a pending
// NodeClaim is the one that claim asked the broker for, and the NodeProviderIDController will bind
// them. Adopting it would leave the claim unresolved until its registration timeout.
func TestSkipsNodeAPendingClaimIsWaitingFor(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pending := newNodeClaim("pending", testPool, cprovider.PendingProviderIDPrefix+"some-uid", baseTime)
	node := newNode("worker-new", withCreated(baseTime.Add(time.Hour)))
	c := newTestClient(pool, node, pending)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1 (the pending one must keep its node)", len(claims))
	}
	if got := getNode(t, c, node.Name).Spec.ProviderID; got != "" {
		t.Errorf("node providerID = %q, want it left empty for the pending claim", got)
	}
}

// TestAdoptsNodeOlderThanPendingClaim is the other half of that rule: a node that predates the
// pending claim cannot be what the broker is provisioning, so it is free to adopt.
func TestAdoptsNodeOlderThanPendingClaim(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pending := newNodeClaim("pending", testPool, cprovider.PendingProviderIDPrefix+"some-uid", baseTime.Add(time.Hour))
	c := newTestClient(pool, newNode("worker-old", withCreated(baseTime)), pending)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 2 {
		t.Fatalf("got %d nodeclaims, want 2 (pending + adopted)", len(claims))
	}
}

func TestSkipsNodesItMustNotAdopt(t *testing.T) {
	tests := []struct {
		name          string
		node          *corev1.Node
		instanceTypes []*cloudprovider.InstanceType
	}{
		{
			name: "control-plane node",
			node: newNode("master-a", withLabel("node-role.kubernetes.io/control-plane", "")),
		},
		{
			name: "legacy master node",
			node: newNode("master-b", withLabel("node-role.kubernetes.io/master", "")),
		},
		{
			name: "no sku_name label",
			node: newNode("worker-a", withoutLabel(skuNameLabel)),
		},
		{
			name: "sku not in the node class",
			node: newNode("worker-a", withLabel(skuNameLabel, "some-other-sku")),
		},
		{
			// An arm64 node labelled into an amd64 pool: adopting it produces a NodeClaim
			// Karpenter reads as drifted and replaces, i.e. it would delete a running node.
			name:          "architecture the pool does not allow",
			node:          newNode("worker-a", withLabel(corev1.LabelArchStable, "arm64")),
			instanceTypes: []*cloudprovider.InstanceType{newInstanceType(testSKU, "amd64")},
		},
		{
			name: "provider ID from another provider",
			node: newNode("worker-a", withProviderID("aws:///us-east-1a/i-0123456789")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newNodePool(testPool, testSKU)
			c := newTestClient(pool, tt.node)
			ctrl, _ := newTestController(c, tt.instanceTypes...)

			if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if claims := listClaims(t, c); len(claims) != 0 {
				t.Fatalf("got %d nodeclaims, want 0", len(claims))
			}
			if got := getNode(t, c, tt.node.Name).Spec.ProviderID; got != tt.node.Spec.ProviderID {
				t.Errorf("node providerID = %q, want it untouched (%q)", got, tt.node.Spec.ProviderID)
			}
		})
	}
}

// TestSkipsNotReadyNodes: adoption waits for the kubelet to report Ready. A NodeClaim for a node
// that is not Ready would be counted towards the pool's limits at the SKU's declared capacity while
// nothing can schedule on it, and it could never reach Initialized — Liveness does not reap a claim
// that registered, so a node that never comes up would leave a permanent phantom NodeClaim.
func TestSkipsNotReadyNodes(t *testing.T) {
	tests := []struct {
		name   string
		status corev1.ConditionStatus
	}{
		{name: "kubelet reports NotReady", status: corev1.ConditionFalse},
		{name: "kubelet stopped heartbeating", status: corev1.ConditionUnknown},
		{name: "node has no conditions yet", status: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newNodePool(testPool, testSKU)
			node := newNode("worker-a", withReady(tt.status))
			c := newTestClient(pool, node)
			ctrl, _ := newTestController(c)

			if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if claims := listClaims(t, c); len(claims) != 0 {
				t.Fatalf("got %d nodeclaims, want 0 for a node that is not Ready", len(claims))
			}
			// The node must be left completely untouched: spec.providerID is immutable once set, so
			// stamping one onto a machine that may be mid-teardown is not recoverable.
			stored := getNode(t, c, node.Name)
			if stored.Spec.ProviderID != "" {
				t.Errorf("node providerID = %q, want it left empty", stored.Spec.ProviderID)
			}
			if _, ok := stored.Labels[karpv1.NodeRegisteredLabelKey]; ok {
				t.Errorf("node carries %s, want it unset until the node is adopted", karpv1.NodeRegisteredLabelKey)
			}
		})
	}
}

// TestAdoptsNodeOnceItBecomesReady: the gate defers adoption, it does not abandon the node. The Node
// watch fires on the status update that flips the condition, so this is the sequence in production.
func TestAdoptsNodeOnceItBecomesReady(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	node := newNode("worker-a", withReady(corev1.ConditionFalse))
	c := newTestClient(pool, node)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile (NotReady): %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 0 {
		t.Fatalf("got %d nodeclaims while NotReady, want 0", len(claims))
	}

	live := getNode(t, c, node.Name)
	live.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("mark node Ready: %v", err)
	}

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile (Ready): %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 1 {
		t.Fatalf("got %d nodeclaims after the node became Ready, want 1", len(claims))
	}
	if got := claims[0].Annotations[cprovider.AdoptedProviderIDAnnotationKey]; got != "rafay://pool1/oci-inst/worker-a" {
		t.Errorf("adopted providerID = %q, want rafay://pool1/oci-inst/worker-a", got)
	}
}

// TestMixedReadinessPoolAdoptsOnlyTheReadyOnes: the gate is a per-node guard, not an early exit. A
// NotReady node in the middle of the list must not stop the Ready ones after it from being adopted in
// the same pass — the difference between `continue` and `break`, which no other test would catch.
func TestMixedReadinessPoolAdoptsOnlyTheReadyOnes(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool,
		newNode("worker-a"),
		newNode("worker-b", withReady(corev1.ConditionFalse)),
		newNode("worker-c"),
	)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	claims := listClaims(t, c)
	if len(claims) != 2 {
		t.Fatalf("got %d nodeclaims, want 2 (worker-a and worker-c, not worker-b)", len(claims))
	}
	adoptedNodes := map[string]bool{}
	for i := range claims {
		adoptedNodes[claims[i].Annotations[adoptedNodeAnnotationKey]] = true
	}
	if !adoptedNodes["worker-a"] || !adoptedNodes["worker-c"] {
		t.Errorf("adopted %v, want both worker-a and worker-c", adoptedNodes)
	}
	if adoptedNodes["worker-b"] {
		t.Error("worker-b is NotReady and must not be adopted")
	}
	if got := getNode(t, c, "worker-b").Spec.ProviderID; got != "" {
		t.Errorf("worker-b providerID = %q, want it left empty", got)
	}
}

// TestNotReadyCountExcludesAlreadyAdoptedNodes: the readiness gate is evaluated before the ownership
// checks (deliberately — see the comment there), so an already-adopted node that goes NotReady passes
// through it. The "waiting for N node(s) to become Ready" tail must not count such a node: it is not
// waiting for anything, and claiming otherwise contradicts the node/nodeclaim counts in the same line.
func TestNotReadyCountExcludesAlreadyAdoptedNodes(t *testing.T) {
	const providerID = "rafay://pool1/oci-inst/worker-a"
	pool := newNodePool(testPool, testSKU)
	adoptedButSick := newNode("worker-a", withProviderID(providerID), withReady(corev1.ConditionFalse))
	stillComingUp := newNode("worker-b", withReady(corev1.ConditionFalse))
	c := newTestClient(pool, adoptedButSick, stillComingUp,
		newNodeClaim("existing", testPool, providerID, baseTime),
	)
	ctrl, _ := newTestController(c)

	// Reconcile() does not surface the counter, so assert on the decision it drives: only worker-b is
	// genuinely outstanding, and neither node may be touched.
	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 {
		t.Fatalf("got %d nodeclaims, want 1 (no adoption while both nodes are NotReady)", len(claims))
	}
	if got := getNode(t, c, "worker-b").Spec.ProviderID; got != "" {
		t.Errorf("worker-b providerID = %q, want it left empty", got)
	}
	if got := getNode(t, c, "worker-a").Spec.ProviderID; got != providerID {
		t.Errorf("worker-a providerID = %q, want its existing %q untouched", got, providerID)
	}
}

// TestReadyGateDoesNotAffectAdoptedNodes: the gate decides when to take a node on, nothing more. A
// node that goes NotReady after adoption keeps its NodeClaim — Karpenter's repair and disruption
// paths own it from then on, and revoking the claim here would strip the pool's accounting for a
// machine that still exists.
func TestReadyGateDoesNotAffectAdoptedNodes(t *testing.T) {
	const providerID = "rafay://pool1/oci-inst/worker-a"
	pool := newNodePool(testPool, testSKU)
	c := newTestClient(pool,
		newNode("worker-a", withProviderID(providerID), withReady(corev1.ConditionFalse)),
		newNodeClaim("existing", testPool, providerID, baseTime),
	)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 || claims[0].Name != "existing" {
		t.Fatalf("nodeclaims = %+v, want the existing one left in place", claims)
	}
}

// TestIgnoresForeignNodePool checks a NodePool backed by another provider's node class is left
// alone, so this controller cannot claim nodes it does not understand.
func TestIgnoresForeignNodePool(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pool.Spec.Template.Spec.NodeClassRef = &karpv1.NodeClassReference{
		Group: "karpenter.k8s.aws", Kind: "EC2NodeClass", Name: "default",
	}
	c := newTestClient(pool, newNode("worker-a"))
	ctrl, stub := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 0 {
		t.Fatalf("got %d nodeclaims, want 0", len(claims))
	}
	if stub.calls != 0 {
		t.Errorf("GetInstanceTypes called %d times, want 0", stub.calls)
	}
}

// TestIgnoresAutoScalingDisabledNodePool: edge-broker renders a NodePool for every catalog row so
// the whole catalog is visible in-cluster, marking rows that did not opt into autoscaling with
// karpenter.rafay.io/auto-scaling=false. Such a pool's nodes must never get NodeClaims — a claim
// would hand Karpenter ownership (limit accounting, termination on claim delete) of a pool the
// user sized by hand. Only the explicit "false" skips: hand-written pools carry no annotation.
func TestIgnoresAutoScalingDisabledNodePool(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pool.Annotations = map[string]string{autoScalingAnnotationKey: "false"}
	c := newTestClient(pool, newNode("worker-a"))
	ctrl, stub := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 0 {
		t.Fatalf("got %d nodeclaims, want 0", len(claims))
	}
	if stub.calls != 0 {
		t.Errorf("GetInstanceTypes called %d times, want 0", stub.calls)
	}
	// The node itself must be untouched: no providerID stamped, no registered label.
	if got := getNode(t, c, "worker-a").Spec.ProviderID; got != "" {
		t.Errorf("node providerID = %q, want it left unset", got)
	}

	// The broker's "true" marker (and any value other than "false") adopts as usual.
	pool.Annotations[autoScalingAnnotationKey] = "true"
	if err := c.Update(context.Background(), pool); err != nil {
		t.Fatalf("update pool: %v", err)
	}
	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 1 {
		t.Fatalf("got %d nodeclaims after enabling, want 1", len(claims))
	}
}

// TestIgnoresDeletingNodePool: a pool being deleted is draining its nodes, so adopting more of them
// would create NodeClaims the cascade delete immediately tears down again.
func TestIgnoresDeletingNodePool(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	now := metav1.NewTime(baseTime)
	pool.DeletionTimestamp = &now
	pool.Finalizers = []string{"karpenter.sh/termination"}
	c := newTestClient(pool, newNode("worker-a"))
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if claims := listClaims(t, c); len(claims) != 0 {
		t.Fatalf("got %d nodeclaims, want 0", len(claims))
	}
}

// TestAdoptedNodeClaimShape pins the choices that keep adoption from disturbing a running node.
func TestAdoptedNodeClaimShape(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pool.Spec.Template.Spec.Taints = []corev1.Taint{
		{Key: "dedicated", Value: "gpu", Effect: corev1.TaintEffectNoExecute},
	}
	pool.Spec.Template.Spec.StartupTaints = []corev1.Taint{
		{Key: "starting", Effect: corev1.TaintEffectNoSchedule},
	}
	node := newNode("worker-a")
	it := newInstanceType(testSKU, "amd64")

	nc := adoptedNodeClaim(pool, node, testSKU, it)

	// Taints would be pushed onto the node by Registration; NoExecute would evict its pods.
	if len(nc.Spec.Taints) != 0 || len(nc.Spec.StartupTaints) != 0 {
		t.Errorf("taints = %+v / startupTaints = %+v, want both empty", nc.Spec.Taints, nc.Spec.StartupTaints)
	}
	// Static drift is only evaluated when both objects carry the hash; omitting it means a later
	// NodePool edit does not roll every adopted node.
	if _, ok := nc.Annotations[karpv1.NodePoolHashAnnotationKey]; ok {
		t.Errorf("annotations carry %s, want it omitted", karpv1.NodePoolHashAnnotationKey)
	}
	// The node has no zone, and the SKU's synthetic "default" must not be invented for it.
	if v, ok := nc.Labels[corev1.LabelTopologyZone]; ok {
		t.Errorf("zone label = %q, want it omitted when the node has none", v)
	}
	if got := nc.Labels[karpv1.CapacityTypeLabelKey]; got != karpv1.CapacityTypeOnDemand {
		t.Errorf("capacity-type label = %q, want %q", got, karpv1.CapacityTypeOnDemand)
	}
	if got := nc.Labels["nodepool"]; got != testPool {
		t.Errorf("pool template label nodepool = %q, want %q", got, testPool)
	}
	if got := nc.Labels[karpv1.NodeClassLabelKey(pool.Spec.Template.Spec.NodeClassRef.GroupKind())]; got != testSKU {
		t.Errorf("node class label = %q, want %q", got, testSKU)
	}
	if nc.GenerateName != testPool+"-" {
		t.Errorf("GenerateName = %q, want %q", nc.GenerateName, testPool+"-")
	}
	if len(nc.Spec.Resources.Requests) != 0 {
		t.Errorf("resource requests = %+v, want none", nc.Spec.Resources.Requests)
	}
}

// TestAdoptedNodeClaimUsesNodeTopology: when the node does carry topology labels they are its own
// truth, and copying them (rather than the SKU's) keeps Registration from overwriting them.
func TestAdoptedNodeClaimUsesNodeTopology(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	node := newNode("worker-a",
		withLabel(corev1.LabelTopologyZone, "us-ashburn-1-ad-1"),
		withLabel(corev1.LabelTopologyRegion, "us-ashburn-1"),
	)

	nc := adoptedNodeClaim(pool, node, testSKU, newInstanceType(testSKU, "amd64"))

	if got := nc.Labels[corev1.LabelTopologyZone]; got != "us-ashburn-1-ad-1" {
		t.Errorf("zone label = %q, want the node's own value", got)
	}
	if got := nc.Labels[corev1.LabelTopologyRegion]; got != "us-ashburn-1" {
		t.Errorf("region label = %q, want the node's own value", got)
	}
}

// TestPinInstanceType: the pool may allow several SKUs, but an adopted NodeClaim describes one
// machine that already exists, so its instance-type requirement states which.
func TestPinInstanceType(t *testing.T) {
	reqs := []karpv1.NodeSelectorRequirementWithMinValues{
		{Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}},
		{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"a", "b"}},
	}
	out := pinInstanceType(reqs, "b")

	var found int
	for _, r := range out {
		if r.Key != corev1.LabelInstanceTypeStable {
			continue
		}
		found++
		if len(r.Values) != 1 || r.Values[0] != "b" {
			t.Errorf("instance-type values = %v, want [b]", r.Values)
		}
	}
	if found != 1 {
		t.Errorf("found %d instance-type requirements, want exactly 1", found)
	}
	if len(out) != len(reqs) {
		t.Errorf("got %d requirements, want %d (the OS requirement must survive)", len(out), len(reqs))
	}
}

// TestNodeToNodePool: a worker node's events reach its pool's reconcile, and a node without the pool
// label reaches nothing.
func TestNodeToNodePool(t *testing.T) {
	reqs := nodeToNodePool(context.Background(), newNode("worker-a"))
	if len(reqs) != 1 || reqs[0].Name != testPool {
		t.Errorf("requests = %+v, want one for %q", reqs, testPool)
	}
	if got := nodeToNodePool(context.Background(), newNode("worker-a", withoutLabel(nodepoolNameLabel))); len(got) != 0 {
		t.Errorf("requests = %+v, want none for a node with no pool label", got)
	}
}

func TestEnabledFromEnv(t *testing.T) {
	tests := []struct {
		value string
		set   bool
		want  bool
	}{
		{set: false, want: true},
		{value: "", set: true, want: true},
		{value: "true", set: true, want: true},
		{value: "false", set: true, want: false},
		{value: "0", set: true, want: false},
		{value: "nonsense", set: true, want: true},
	}
	for _, tt := range tests {
		if tt.set {
			t.Setenv("KARPENTER_ADOPT_EXISTING_NODES", tt.value)
		}
		if got := EnabledFromEnv(); got != tt.want {
			t.Errorf("EnabledFromEnv() with %q (set=%t) = %t, want %t", tt.value, tt.set, got, tt.want)
		}
	}
}
