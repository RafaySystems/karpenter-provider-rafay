/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

import (
	"context"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	karpapis "sigs.k8s.io/karpenter/pkg/apis"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const priceEpsilon = 1e-9

// ─────────────────── rafayInstanceTypesToKarpenter ────────────────────

// TestRafayInstanceTypesToKarpenterSyntheticPricing verifies the synthetic relative cost:
// 1.0 per vCPU + 0.125 per GiB of memory.
func TestRafayInstanceTypesToKarpenterSyntheticPricing(t *testing.T) {
	specs := []v1alpha1.InstanceTypeSpec{
		{Name: "standard-4-8", CPU: "4", Memory: "8Gi"},
		{Name: "standard-8-16", CPU: "8000m", Memory: "16Gi"},
		{Name: "small-half-512", CPU: "500m", Memory: "512Mi"},
	}
	wantPrices := map[string]float64{
		"standard-4-8":   4*1.0 + 8*0.125,     // 5.0
		"standard-8-16":  8*1.0 + 16*0.125,    // 10.0
		"small-half-512": 0.5*1.0 + 0.5*0.125, // 0.5625
	}

	its, err := rafayInstanceTypesToKarpenter(specs)
	if err != nil {
		t.Fatalf("rafayInstanceTypesToKarpenter: unexpected error: %v", err)
	}
	if len(its) != len(specs) {
		t.Fatalf("got %d instance types, want %d", len(its), len(specs))
	}
	for _, it := range its {
		want, ok := wantPrices[it.Name]
		if !ok {
			t.Errorf("unexpected instance type %q", it.Name)
			continue
		}
		if len(it.Offerings) != 1 {
			t.Errorf("instance type %q: got %d offerings, want exactly 1", it.Name, len(it.Offerings))
			continue
		}
		offering := it.Offerings[0]
		if math.Abs(offering.Price-want) > priceEpsilon {
			t.Errorf("instance type %q: price = %v, want %v (1.0/vCPU + 0.125/GiB)", it.Name, offering.Price, want)
		}
		if !offering.Available {
			t.Errorf("instance type %q: offering should be available", it.Name)
		}
	}
}

func TestRafayInstanceTypesToKarpenterCapacity(t *testing.T) {
	its, err := rafayInstanceTypesToKarpenter([]v1alpha1.InstanceTypeSpec{
		{Name: "standard-4-8", CPU: "4", Memory: "8Gi"},
	})
	if err != nil {
		t.Fatalf("rafayInstanceTypesToKarpenter: unexpected error: %v", err)
	}
	it := its[0]
	if got, want := it.Capacity[corev1.ResourceCPU], resource.MustParse("4"); got.Cmp(want) != 0 {
		t.Errorf("cpu capacity = %s, want %s", got.String(), want.String())
	}
	if got, want := it.Capacity[corev1.ResourceMemory], resource.MustParse("8Gi"); got.Cmp(want) != 0 {
		t.Errorf("memory capacity = %s, want %s", got.String(), want.String())
	}
	if got, want := it.Capacity[corev1.ResourcePods], resource.MustParse("110"); got.Cmp(want) != 0 {
		t.Errorf("pods capacity = %s, want %s", got.String(), want.String())
	}
	if got := it.Requirements.Get(corev1.LabelInstanceTypeStable).Values(); len(got) != 1 || got[0] != "standard-4-8" {
		t.Errorf("instance-type requirement = %v, want [standard-4-8]", got)
	}
	if got := it.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 1 || got[0] != "default" {
		t.Errorf("zone requirement = %v, want [default] when spec zone is empty", got)
	}
	if _, ok := it.Capacity[temporaryGPUResourceName]; ok {
		t.Errorf("%s present in capacity, want absent when the spec declares no GPU", temporaryGPUResourceName)
	}
}

// TEMPORARY — delete with InstanceTypeSpec.GPU.
//
// A GPU absent from Capacity is what strands a pod requesting nvidia.com/gpu in Pending: the
// scheduler's fits check filters out every instance type and no NodeClaim is created. A GPU
// wrongly present is worse than useless — the NodeClaim inherits the request and Initialization
// blocks on a device plugin that will never advertise it, with no timeout to reap it. So both the
// declared and the zero/empty cases are pinned.
func TestRafayInstanceTypesToKarpenterGPUCapacity(t *testing.T) {
	tests := []struct {
		name string
		gpu  string
		want string // empty means "must not appear in Capacity"
	}{
		{name: "declared count is advertised", gpu: "8", want: "8"},
		{name: "unset is omitted", gpu: "", want: ""},
		{name: "zero is omitted", gpu: "0", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			its, err := rafayInstanceTypesToKarpenter([]v1alpha1.InstanceTypeSpec{
				{Name: "oci-inst", CPU: "4", Memory: "12Gi", GPU: tt.gpu},
			})
			if err != nil {
				t.Fatalf("rafayInstanceTypesToKarpenter: unexpected error: %v", err)
			}
			got, ok := its[0].Capacity[temporaryGPUResourceName]
			if tt.want == "" {
				if ok {
					t.Errorf("%s capacity = %s, want absent for gpu=%q", temporaryGPUResourceName, got.String(), tt.gpu)
				}
				return
			}
			if !ok {
				t.Fatalf("%s missing from capacity, want %s", temporaryGPUResourceName, tt.want)
			}
			if want := resource.MustParse(tt.want); got.Cmp(want) != 0 {
				t.Errorf("%s capacity = %s, want %s", temporaryGPUResourceName, got.String(), want.String())
			}
		})
	}
}

func TestRafayInstanceTypesToKarpenterQuantityParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		spec    v1alpha1.InstanceTypeSpec
		wantSub string
	}{
		{
			name:    "invalid cpu",
			spec:    v1alpha1.InstanceTypeSpec{Name: "bad-cpu", CPU: "four", Memory: "8Gi"},
			wantSub: `invalid cpu "four"`,
		},
		{
			name:    "invalid memory",
			spec:    v1alpha1.InstanceTypeSpec{Name: "bad-mem", CPU: "4", Memory: "8Gibberish"},
			wantSub: `invalid memory "8Gibberish"`,
		},
		{
			name:    "empty cpu",
			spec:    v1alpha1.InstanceTypeSpec{Name: "empty-cpu", CPU: "", Memory: "8Gi"},
			wantSub: "invalid cpu",
		},
		{
			// TEMPORARY — delete with InstanceTypeSpec.GPU.
			name:    "invalid gpu",
			spec:    v1alpha1.InstanceTypeSpec{Name: "bad-gpu", CPU: "4", Memory: "8Gi", GPU: "eight"},
			wantSub: `invalid nvidia.com/gpu "eight"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			its, err := rafayInstanceTypesToKarpenter([]v1alpha1.InstanceTypeSpec{tt.spec})
			if err == nil {
				t.Fatalf("rafayInstanceTypesToKarpenter(%+v) = %v, want error", tt.spec, its)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), tt.wantSub)
			}
			if !strings.Contains(err.Error(), tt.spec.Name) {
				t.Errorf("error = %q, want it to name the offending instance type %q", err.Error(), tt.spec.Name)
			}
		})
	}
}

// ─────────────────────── Create() instance selection ───────────────────────

// selectInstanceType mirrors Create()'s selection: filter compatible types, then stable-sort
// by synthetic price so the cheapest wins and NodeClass spec order is kept on ties.
// (Create itself needs a running NodeBatcher/BrokerClient, so the selection helpers are
// exercised directly.)
func selectInstanceType(its []*karpcp.InstanceType, nodeClaim *karpv1.NodeClaim) []*karpcp.InstanceType {
	compatible := filterCompatibleInstanceTypes(its, nodeClaim)
	sort.SliceStable(compatible, func(i, j int) bool {
		return cheapestOffering(compatible[i]) < cheapestOffering(compatible[j])
	})
	return compatible
}

func instanceTypeNames(its []*karpcp.InstanceType) []string {
	names := make([]string, 0, len(its))
	for _, it := range its {
		names = append(names, it.Name)
	}
	return names
}

func mustInstanceTypes(t *testing.T, specs []v1alpha1.InstanceTypeSpec) []*karpcp.InstanceType {
	t.Helper()
	its, err := rafayInstanceTypesToKarpenter(specs)
	if err != nil {
		t.Fatalf("rafayInstanceTypesToKarpenter: unexpected error: %v", err)
	}
	return its
}

func TestInstanceSelectionPicksCheapest(t *testing.T) {
	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{
		{Name: "big", CPU: "8", Memory: "16Gi"},  // 10.0
		{Name: "tie-a", CPU: "4", Memory: "8Gi"}, // 5.0
		{Name: "tie-b", CPU: "4", Memory: "8Gi"}, // 5.0
		{Name: "small", CPU: "2", Memory: "4Gi"}, // 2.5
	})
	// Unconstrained NodeClaim: every type is compatible.
	sorted := selectInstanceType(its, &karpv1.NodeClaim{})
	got := instanceTypeNames(sorted)
	want := []string{"small", "tie-a", "tie-b", "big"}
	if len(got) != len(want) {
		t.Fatalf("selection order = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selection order = %v, want %v (cheapest first, spec order on ties)", got, want)
		}
	}
}

// TestInstanceSelectionStableTieOrder swaps the declaration order of two equally priced types
// and verifies the sort keeps NodeClass spec order, so ties resolve deterministically.
func TestInstanceSelectionStableTieOrder(t *testing.T) {
	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{
		{Name: "tie-b", CPU: "4", Memory: "8Gi"}, // 5.0, declared first
		{Name: "tie-a", CPU: "4", Memory: "8Gi"}, // 5.0
	})
	sorted := selectInstanceType(its, &karpv1.NodeClaim{})
	got := instanceTypeNames(sorted)
	if len(got) != 2 || got[0] != "tie-b" || got[1] != "tie-a" {
		t.Fatalf("tie order = %v, want [tie-b tie-a] (stable sort keeps spec order)", got)
	}
}

func TestInstanceSelectionRespectsRequirements(t *testing.T) {
	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{
		{Name: "big", CPU: "8", Memory: "16Gi"},  // 10.0
		{Name: "tie-b", CPU: "4", Memory: "8Gi"}, // 5.0
		{Name: "small", CPU: "2", Memory: "4Gi"}, // 2.5, cheapest but excluded by requirement
	})
	nodeClaim := &karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
				{
					Key:      corev1.LabelInstanceTypeStable,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"tie-b", "big"},
				},
			},
		},
	}
	sorted := selectInstanceType(its, nodeClaim)
	got := instanceTypeNames(sorted)
	if len(got) != 2 || got[0] != "tie-b" || got[1] != "big" {
		t.Fatalf("selection = %v, want [tie-b big] (small excluded by instance-type requirement, cheapest compatible first)", got)
	}
}

func TestInstanceSelectionRespectsResourceRequests(t *testing.T) {
	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{
		{Name: "tie-a", CPU: "4", Memory: "8Gi"}, // 5.0
		{Name: "small", CPU: "2", Memory: "4Gi"}, // 2.5, cheapest but too small
	})
	nodeClaim := &karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{
			Resources: karpv1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("3"),
				},
			},
		},
	}
	sorted := selectInstanceType(its, nodeClaim)
	got := instanceTypeNames(sorted)
	if len(got) != 1 || got[0] != "tie-a" {
		t.Fatalf("selection = %v, want [tie-a] (small cannot fit a 3-cpu request)", got)
	}
}

func TestInstanceSelectionNoCompatibleType(t *testing.T) {
	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{
		{Name: "small", CPU: "2", Memory: "4Gi"},
	})
	nodeClaim := &karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
				{
					Key:      corev1.LabelInstanceTypeStable,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"nonexistent"},
				},
			},
		},
	}
	if got := selectInstanceType(its, nodeClaim); len(got) != 0 {
		t.Fatalf("selection = %v, want empty (no compatible type)", instanceTypeNames(got))
	}
}

// ─────────────────────────── cluster/project identity ───────────────────────────

// TestNewCloudProviderUsesEnvIDs pins the surviving identity contract: the Rafay
// cluster/project IDs come from the constructor arguments (which main.go fills from
// RAFAY_CLUSTER_ID / RAFAY_PROJECT_ID) and nowhere else. RafayNodeClass used to be able to
// override them per class; that field pair was removed, so there is no precedence rule left.
//
// This only guards constructor plumbing. Asserting the IDs actually reach
// AddNodesRequest/RemoveNodesRequest is not possible from this package: CloudProvider.batcher
// is a concrete *rafay.NodeBatcher and rafay's broker seam (batchBroker) is unexported, so
// Create/Delete cannot be driven with a fake broker.
func TestNewCloudProviderUsesEnvIDs(t *testing.T) {
	c := NewCloudProvider(nil, nil, nil, "env-cluster", "env-project", nil, nil)
	if c.clusterID != "env-cluster" {
		t.Errorf("clusterID = %q, want %q", c.clusterID, "env-cluster")
	}
	if c.projectID != "env-project" {
		t.Errorf("projectID = %q, want %q", c.projectID, "env-project")
	}
}

// ────────────────────────── NewBatchFailureHandler ──────────────────────────

func newFailureHandlerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	gv := schema.GroupVersion{Group: karpapis.Group, Version: "v1"}
	s.AddKnownTypes(gv, &karpv1.NodeClaim{}, &karpv1.NodeClaimList{})
	metav1.AddToGroupVersion(s, gv)
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add v1alpha1 scheme: %v", err)
	}
	return s
}

func newNodeClaim(name, uid, providerID string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			UID:  types.UID(uid),
		},
		Status: karpv1.NodeClaimStatus{
			ProviderID: providerID,
		},
	}
}

func newFakeClientWithClaims(t *testing.T, claims ...*karpv1.NodeClaim) client.WithWatch {
	t.Helper()
	builder := fake.NewClientBuilder().WithScheme(newFailureHandlerScheme(t))
	for _, c := range claims {
		builder = builder.WithObjects(c)
	}
	return builder.Build()
}

func nodeClaimExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKey{Name: name}, &karpv1.NodeClaim{})
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("get nodeclaim %q: %v", name, err)
	return false
}

func TestBatchFailureHandlerDeletesPendingClaim(t *testing.T) {
	claim := newNodeClaim("pending-claim", "op-1", PendingProviderIDPrefix+"op-1")
	cl := newFakeClientWithClaims(t, claim)
	handler := NewBatchFailureHandler(cl, cl, nil)

	handler(context.Background(), "op-1", "add", "node provisioning failed")

	if nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("pending NodeClaim with matching UID should be deleted on add failure")
	}
}

func TestBatchFailureHandlerKeepsResolvedClaim(t *testing.T) {
	claim := newNodeClaim("resolved-claim", "op-2", "rafay://cluster-1/node-1")
	cl := newFakeClientWithClaims(t, claim)
	handler := NewBatchFailureHandler(cl, cl, nil)

	handler(context.Background(), "op-2", "add", "late failure after node joined")

	if !nodeClaimExists(t, cl, "resolved-claim") {
		t.Fatal("NodeClaim with a resolved (non-pending) ProviderID must NOT be deleted")
	}
}

func TestBatchFailureHandlerIgnoresRemoveFailures(t *testing.T) {
	claim := newNodeClaim("pending-claim", "op-3", PendingProviderIDPrefix+"op-3")
	cl := newFakeClientWithClaims(t, claim)
	handler := NewBatchFailureHandler(cl, cl, nil)

	// Even with a matching UID and a pending ProviderID, kind "remove" never deletes.
	handler(context.Background(), "op-3", "remove", "remove failed")

	if !nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("remove-operation failures must never delete NodeClaims")
	}
}

func TestBatchFailureHandlerMissingClaimIsNoop(t *testing.T) {
	other := newNodeClaim("other-claim", "other-uid", PendingProviderIDPrefix+"other-uid")
	cl := newFakeClientWithClaims(t, other)
	handler := NewBatchFailureHandler(cl, cl, nil)

	// No NodeClaim matches this operationID; the handler must not panic or touch other claims.
	handler(context.Background(), "no-such-op", "add", "failure for unknown claim")

	if !nodeClaimExists(t, cl, "other-claim") {
		t.Fatal("failure for an unknown operationID must not delete unrelated NodeClaims")
	}
}

// ────────────────────────── List ──────────────────────────

func newListScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add v1alpha1 scheme: %v", err)
	}
	return s
}

func newNode(name, providerID string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.NodeSpec{ProviderID: providerID},
	}
}

// TestListNodesFromKubeListsAllRafayNodes pins the GC-safety contract: every Node carrying a
// rafay:// ProviderID must appear in List(), regardless of RAFAY_CLUSTER_ID.
//
// Real ProviderIDs are stamped rafay://<nodepoolname>/<sku_name>/<hostname> — the first segment is
// the node pool, NOT a cluster ID. A cluster-ID filter over that segment can never match, so List()
// would come back empty and the core garbage-collection controller would delete every Registered
// NodeClaim whose node briefly went NotReady. Non-rafay nodes are still excluded.
func TestListNodesFromKubeListsAllRafayNodes(t *testing.T) {
	// Real platform format: first segment is the nodepool name.
	nodeA := newNode("node-a", "rafay://worker-pool-amd/oci-inst/host-w1-e6a5c")
	nodeB := newNode("node-b", "rafay://worker-pool-gpu/gpu-inst/host-w2-f7b6d")
	nonRafayNode := newNode("non-rafay-node", "aws:///us-east-1a/i-123")
	noProviderID := newNode("no-provider-id", "")

	cl := fake.NewClientBuilder().WithScheme(newListScheme(t)).
		WithObjects(nodeA, nodeB, nonRafayNode, noProviderID).Build()
	// clusterID is set, exactly as in production — it must not filter anything out.
	cp := &CloudProvider{kubeClient: cl, clusterID: "cluster-env"}

	got, err := cp.listNodesFromKube(context.Background())
	if err != nil {
		t.Fatalf("listNodesFromKube: %v", err)
	}
	ids := map[string]bool{}
	for _, claim := range got {
		ids[claim.Status.ProviderID] = true
	}
	if !ids["rafay://worker-pool-amd/oci-inst/host-w1-e6a5c"] {
		t.Error("rafay node must be listed even though its first segment is a pool name, not the clusterID")
	}
	if !ids["rafay://worker-pool-gpu/gpu-inst/host-w2-f7b6d"] {
		t.Error("second rafay node must be listed")
	}
	if len(got) != 2 {
		t.Errorf("expected exactly the 2 rafay nodes, got %d: %v", len(got), ids)
	}
}

// ────────────────────────── Create() adoption ──────────────────────────

// TestCreateAdoptedNodeClaimSkipsBroker pins the contract the NodeAdoptionController relies on: a
// NodeClaim annotated with an already-running node's ProviderID must be reported as launched without
// any broker traffic. The CloudProvider is deliberately built with a nil NodeBatcher — reaching the
// provisioning path would panic, which is exactly the regression worth catching, since it would mean
// adoption provisions a second machine for a node the cluster already has.
func TestCreateAdoptedNodeClaimSkipsBroker(t *testing.T) {
	const adoptedID = "rafay://pool1/oci-inst/test-auto-2-e-e949w9-w0-af1cc"
	nodeClass := &v1alpha1.RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "oci-inst"},
		Spec: v1alpha1.RafayNodeClassSpec{
			InstanceTypes: []v1alpha1.InstanceTypeSpec{
				{Name: "oci-inst", CPU: "8", Memory: "12Gi", Architectures: []string{"amd64"}, OperatingSystems: []string{"linux"}},
			},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(newListScheme(t)).WithObjects(nodeClass).Build()
	cp := &CloudProvider{kubeClient: cl, clusterID: "cluster-env", projectID: "project-env"}

	nodeClaim := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pool1-abcde",
			Annotations: map[string]string{AdoptedProviderIDAnnotationKey: adoptedID},
			Labels:      map[string]string{karpv1.NodePoolLabelKey: "pool1", corev1.LabelInstanceTypeStable: "oci-inst"},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "RafayNodeClass", Name: "oci-inst"},
			Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
				{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"oci-inst"}},
			},
		},
	}

	out, err := cp.Create(context.Background(), nodeClaim)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if out.Status.ProviderID != adoptedID {
		t.Errorf("providerID = %q, want the adopted node's %q", out.Status.ProviderID, adoptedID)
	}
	if out.Status.Capacity.Cpu().String() != "8" {
		t.Errorf("capacity cpu = %q, want 8 from the instance type", out.Status.Capacity.Cpu())
	}
	// A zone the node never had must not be introduced here: Karpenter merges these labels into the
	// stored NodeClaim, from where Registration copies them onto the running node.
	if v, ok := out.Labels[corev1.LabelTopologyZone]; ok {
		t.Errorf("zone label = %q, want Create() to add none for an adopted NodeClaim", v)
	}
}

// ────────────────────────── NodeClaimSKUs ──────────────────────────

// TestNodeClaimSKUs verifies a node's sku_name label may match either the selected instance type
// (multi-SKU NodeClasses) or the NodeClass name (legacy single-SKU convention).
func TestNodeClaimSKUs(t *testing.T) {
	nc := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{corev1.LabelInstanceTypeStable: "oci-inst-large"},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{Name: "mixed-class"},
		},
	}
	skus := NodeClaimSKUs(nc)
	if !skus["oci-inst-large"] {
		t.Error("selected instance type must be an acceptable sku_name (multi-SKU NodeClass)")
	}
	if !skus["mixed-class"] {
		t.Error("NodeClass name must remain an acceptable sku_name (legacy single-SKU)")
	}
	if skus["something-else"] {
		t.Error("unrelated sku must not match")
	}
}

// ───────────────── pool-at-maximum backoff (failure handler + GetInstanceTypes) ─────────────────

// Every instance type counts as one node so NodePool.spec.limits.nodes binds within a scheduling
// round: the scheduler subtracts a newly planned NodeClaim's instance type capacity from the
// pool's remaining limits, and a "nodes" entry of 0 would let it plan past the ceiling.
func TestInstanceTypeCapacityCountsOneNode(t *testing.T) {
	its, err := rafayInstanceTypesToKarpenter([]v1alpha1.InstanceTypeSpec{{Name: "bm", CPU: "4", Memory: "8Gi"}})
	if err != nil {
		t.Fatalf("rafayInstanceTypesToKarpenter: %v", err)
	}
	if got, want := its[0].Capacity[resources.Node], resource.MustParse("1"); got.Cmp(want) != 0 {
		t.Errorf("Capacity[nodes] = %v, want %v", got, want)
	}
	// The node count is scheduler bookkeeping, not something a NodeClaim's status should report
	// as if the kubelet had — the cluster state adds it back for accounting.
	status := nodeClaimResources(its[0].Capacity)
	if _, ok := status[resources.Node]; ok {
		t.Errorf("nodeClaimResources kept the nodes entry: %v", status)
	}
	if got, want := status[corev1.ResourceCPU], resource.MustParse("4"); got.Cmp(want) != 0 {
		t.Errorf("nodeClaimResources dropped cpu: %v", status)
	}
}

func newPoolClaim(name, uid, providerID, pool string) *karpv1.NodeClaim {
	nc := newNodeClaim(name, uid, providerID)
	nc.Labels = map[string]string{karpv1.NodePoolLabelKey: pool}
	return nc
}

func TestBatchFailureHandlerPoolAtMaxHoldsPoolAndDeletesClaim(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-max", PendingProviderIDPrefix+"op-max", "pool1")
	cl := newFakeClientWithClaims(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, cl, backoff)

	handler(context.Background(), "op-max", "add", `batch add: pool at maximum: pool "pool1" has 3 of max 4 nodes; 2 requested, 1 refused`)

	if nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("a refused add must still delete the pending NodeClaim (kept, it is a phantom in-flight node for 60 min)")
	}
	until, held := backoff.Until("pool1")
	if !held {
		t.Fatal("pool1 must be held back after a pool-at-maximum refusal")
	}
	if remaining := time.Until(until); remaining <= 4*time.Minute || remaining > 5*time.Minute {
		t.Errorf("hold ends in %s, want ~5m", remaining)
	}
}

// The wording brokers used before partial acceptance still counts as the same refusal.
func TestBatchFailureHandlerLegacyAtMaxWordingHoldsPool(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-old", PendingProviderIDPrefix+"op-old", "pool1")
	cl := newFakeClientWithClaims(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, cl, backoff)

	handler(context.Background(), "op-old", "add", `batch add: pool "pool1" is at its maximum: 3 + 2 would exceed max 4`)

	if _, held := backoff.Until("pool1"); !held {
		t.Fatal("legacy 'is at its maximum' detail must hold the pool back")
	}
}

func TestBatchFailureHandlerOtherFailuresDoNotHoldPool(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-err", PendingProviderIDPrefix+"op-err", "pool1")
	cl := newFakeClientWithClaims(t, claim)
	backoff := NewPoolBackoff(5 * time.Minute)
	handler := NewBatchFailureHandler(cl, cl, backoff)

	handler(context.Background(), "op-err", "add", "batch add: ApplyCluster: still conflicting after 3 attempts")

	if nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("a transient failure must delete the pending NodeClaim so Karpenter reprovisions")
	}
	if _, held := backoff.Until("pool1"); held {
		t.Fatal("a transient failure must not hold the pool back")
	}
}

func TestBatchFailureHandlerNilBackoffStillDeletes(t *testing.T) {
	claim := newPoolClaim("pending-claim", "op-nil", PendingProviderIDPrefix+"op-nil", "pool1")
	cl := newFakeClientWithClaims(t, claim)
	handler := NewBatchFailureHandler(cl, cl, nil)

	handler(context.Background(), "op-nil", "add", "pool at maximum")

	if nodeClaimExists(t, cl, "pending-claim") {
		t.Fatal("with no backoff configured the handler must fall back to plain delete-and-reprovision")
	}
}

func newNodePoolFor(name, nodeClass string) *karpv1.NodePool {
	np := &karpv1.NodePool{}
	np.Name = name
	np.Spec.Template.Spec.NodeClassRef = &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "RafayNodeClass", Name: nodeClass}
	return np
}

func TestGetInstanceTypesWithholdsOfferingsWhilePoolIsHeld(t *testing.T) {
	nodeClass := &v1alpha1.RafayNodeClass{}
	nodeClass.Name = "bm-class"
	nodeClass.Spec.InstanceTypes = []v1alpha1.InstanceTypeSpec{{Name: "bm", CPU: "4", Memory: "8Gi"}}
	cl := fake.NewClientBuilder().WithScheme(newFailureHandlerScheme(t)).WithObjects(nodeClass).Build()
	backoff := NewPoolBackoff(5 * time.Minute)
	c := NewCloudProvider(cl, cl, nil, "cluster", "project", nil, backoff)

	available := func(its []*karpcp.InstanceType) int {
		n := 0
		for _, it := range its {
			n += len(it.Offerings.Available())
		}
		return n
	}

	its, err := c.GetInstanceTypes(context.Background(), newNodePoolFor("pool1", "bm-class"))
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	if available(its) != 1 {
		t.Fatalf("pool not held: want 1 available offering, got %d", available(its))
	}

	backoff.Mark("pool1")
	its, err = c.GetInstanceTypes(context.Background(), newNodePoolFor("pool1", "bm-class"))
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	if len(its) != 1 || available(its) != 0 {
		t.Fatalf("held pool: want the instance type with 0 available offerings, got %d types / %d available", len(its), available(its))
	}

	// Another pool on the same NodeClass is unaffected: the hold is per NodePool, not per SKU.
	its, err = c.GetInstanceTypes(context.Background(), newNodePoolFor("pool2", "bm-class"))
	if err != nil {
		t.Fatalf("GetInstanceTypes: %v", err)
	}
	if available(its) != 1 {
		t.Fatalf("unrelated pool affected by the hold: %d available offerings", available(its))
	}
}
