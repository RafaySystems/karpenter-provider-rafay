/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_create_test.go — CloudProvider.Create's provisioning (broker) path, driven through the
// nodeBatcher seam. The adopted path is covered by TestCreateAdoptedNodeClaimSkipsBroker.

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// cpCreateClaim is a fresh NodeClaim as the launch reconciler hands it to Create(): pool label,
// nodeClassRef and requirements, but no instance-type label and no status yet.
func cpCreateClaim(uid, pool, class string, requests corev1.ResourceList) *karpv1.NodeClaim {
	nc := cpClaim("pool1-"+uid, uid, pool, class, "")
	delete(nc.Labels, corev1.LabelInstanceTypeStable)
	nc.Spec.Resources.Requests = requests
	return nc
}

func cpTwoSKUClass() *v1alpha1.RafayNodeClass {
	return cpNodeClass("mixed",
		v1alpha1.InstanceTypeSpec{Name: "large", CPU: "8", Memory: "16Gi"}, // 10.0
		v1alpha1.InstanceTypeSpec{Name: "small", CPU: "2", Memory: "4Gi"},  // 2.5
	)
}

func TestCreateGuards(t *testing.T) {
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, cpNewFakeBatcher())

	if _, err := cp.Create(context.Background(), nil); err == nil {
		t.Error("Create(nil) = nil error, want error")
	}
	noRef := cpCreateClaim("uid-noref", "pool1", "mixed", nil)
	noRef.Spec.NodeClassRef = nil
	if _, err := cp.Create(context.Background(), noRef); err == nil || !strings.Contains(err.Error(), "no nodeClassRef") {
		t.Errorf("Create(no nodeClassRef) = %v, want a 'no nodeClassRef' error", err)
	}
}

func TestCreateNodeClassWithoutInstanceTypes(t *testing.T) {
	cl := cpFakeClient(t, cpNodeClass("empty"))
	cp := cpProvider(cl, nil, nil, cpNewFakeBatcher())

	_, err := cp.Create(context.Background(), cpCreateClaim("uid-e", "pool1", "empty", nil))
	if err == nil || !strings.Contains(err.Error(), "add at least one entry to spec.instanceTypes") {
		t.Fatalf("Create = %v, want the user-facing instanceTypes hint", err)
	}
}

func TestCreateNoCompatibleInstanceTypeIsInsufficientCapacity(t *testing.T) {
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, b)

	// 32 CPUs fit neither SKU.
	claim := cpCreateClaim("uid-big", "pool1", "mixed", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32")})
	_, err := cp.Create(context.Background(), claim)
	if !karpcp.IsInsufficientCapacityError(err) {
		t.Fatalf("Create = %v, want InsufficientCapacityError", err)
	}
	if len(b.addCalls()) != 0 {
		t.Errorf("Enqueue called (%v) although nothing was compatible", b.addCalls())
	}
}

// The broker path: cheapest compatible SKU, AddNodesRequest mapping, pending providerID,
// status capacity without the scheduler-only "nodes" entry, overlaid labels and the batch-id
// annotation the batchresume controller reads after a restart.
func TestCreateBrokerPathStampsPendingClaim(t *testing.T) {
	b := cpNewFakeBatcher()
	b.result.BatchID = "batch-add-7"
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, b)

	// 3 CPUs: "small" (2) cannot fit, "large" is selected even though it is dearer.
	claim := cpCreateClaim("uid-7", "pool1", "mixed", corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")})
	out, err := cp.Create(context.Background(), claim)
	if err != nil {
		t.Fatalf("Create = %v", err)
	}

	adds := b.addCalls()
	if len(adds) != 1 {
		t.Fatalf("Enqueue calls = %d, want 1", len(adds))
	}
	if adds[0].operationID != "uid-7" || adds[0].req.OperationID != "uid-7" {
		t.Errorf("operationID = %q / req.OperationID = %q, want the NodeClaim UID", adds[0].operationID, adds[0].req.OperationID)
	}
	if r := adds[0].req; r.ClusterID != cpClusterID || r.ProjectID != cpProjectID || r.InstanceType != "large" || r.NodePoolName != "pool1" {
		t.Errorf("AddNodesRequest = %+v, want cluster/project from the provider, InstanceType=large, NodePoolName=pool1", r)
	}

	if want := PendingProviderIDPrefix + "uid-7"; out.Status.ProviderID != want {
		t.Errorf("providerID = %q, want %q", out.Status.ProviderID, want)
	}
	if got, want := out.Status.Capacity[corev1.ResourceCPU], resource.MustParse("8"); got.Cmp(want) != 0 {
		t.Errorf("status capacity cpu = %s, want 8 (the selected SKU)", got.String())
	}
	if _, ok := out.Status.Capacity[resources.Node]; ok {
		t.Error("status.capacity must not carry the scheduler-only nodes entry")
	}
	if _, ok := out.Status.Allocatable[resources.Node]; ok {
		t.Error("status.allocatable must not carry the scheduler-only nodes entry")
	}
	wantLabels := map[string]string{
		corev1.LabelInstanceTypeStable: "large",
		corev1.LabelTopologyZone:       "default",
		karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
		karpv1.NodePoolLabelKey:        "pool1",
	}
	for k, v := range wantLabels {
		if out.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, out.Labels[k], v)
		}
	}
	if got := out.Annotations[BatchIDAnnotationKey]; got != "batch-add-7" {
		t.Errorf("annotation %s = %q, want batch-add-7", BatchIDAnnotationKey, got)
	}
	// The input is not mutated: Karpenter merges the returned copy.
	if claim.Status.ProviderID != "" || claim.Annotations[BatchIDAnnotationKey] != "" {
		t.Error("Create mutated its input NodeClaim instead of returning a copy")
	}
}

func TestCreateSelectsCheapestCompatible(t *testing.T) {
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, b)

	out, err := cp.Create(context.Background(), cpCreateClaim("uid-c", "pool1", "mixed", nil))
	if err != nil {
		t.Fatalf("Create = %v", err)
	}
	if adds := b.addCalls(); len(adds) != 1 || adds[0].req.InstanceType != "small" {
		t.Fatalf("adds = %+v, want one add for the cheapest SKU 'small'", adds)
	}
	if out.Labels[corev1.LabelInstanceTypeStable] != "small" {
		t.Errorf("instance-type label = %q, want small", out.Labels[corev1.LabelInstanceTypeStable])
	}
}

func TestCreateWithoutBatchIDAddsNoAnnotation(t *testing.T) {
	b := cpNewFakeBatcher() // ACK with an empty BatchID
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, b)

	out, err := cp.Create(context.Background(), cpCreateClaim("uid-nb", "pool1", "mixed", nil))
	if err != nil {
		t.Fatalf("Create = %v", err)
	}
	if _, ok := out.Annotations[BatchIDAnnotationKey]; ok {
		t.Errorf("annotation %s set to %q with no batch id, want absent", BatchIDAnnotationKey, out.Annotations[BatchIDAnnotationKey])
	}
}

func TestCreateResultErrIsCreateError(t *testing.T) {
	b := cpNewFakeBatcher()
	sendErr := errors.New("send add batch failed: broker rejected")
	b.result.Err = sendErr
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, b)

	out, err := cp.Create(context.Background(), cpCreateClaim("uid-err", "pool1", "mixed", nil))
	if out != nil {
		t.Errorf("Create returned %+v alongside an error", out)
	}
	var ce *karpcp.CreateError
	if !errors.As(err, &ce) {
		t.Fatalf("Create = %v (%T), want *cloudprovider.CreateError", err, err)
	}
	if ce.ConditionReason != "AddNodesFailed" || ce.ConditionMessage != sendErr.Error() {
		t.Errorf("CreateError reason/message = %q/%q, want AddNodesFailed/%q", ce.ConditionReason, ce.ConditionMessage, sendErr.Error())
	}
	if !errors.Is(err, sendErr) {
		t.Errorf("CreateError does not wrap the batcher error: %v", err)
	}
}

func TestCreateContextCancelledWhileQueued(t *testing.T) {
	b := cpNewFakeBatcher()
	b.hold = true
	cl := cpFakeClient(t, cpTwoSKUClass())
	cp := cpProvider(cl, nil, nil, b)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := cp.Create(ctx, cpCreateClaim("uid-ctx", "pool1", "mixed", nil))
	if !errors.Is(err, context.Canceled) || out != nil {
		t.Fatalf("Create with cancelled ctx = (%v, %v), want (nil, context.Canceled)", out, err)
	}
	if len(b.addCalls()) != 1 {
		t.Errorf("Enqueue calls = %d, want 1 (the item stays queued for the retry to dedup)", len(b.addCalls()))
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-8 ────────────────────────────

// TestCreateMissingNodeClassIsNodeClassNotReady: a NodeClaim whose RafayNodeClass is gone must
// be dropped at once by the launch reconciler (NodeClassNotReadyError) instead of retried for the
// 5-minute LaunchTimeout and counted as a launch failure against the NodePool.
func TestCreateMissingNodeClassIsNodeClassNotReady(t *testing.T) {
	cl := cpFakeClient(t) // no RafayNodeClass at all
	cp := cpProvider(cl, nil, nil, cpNewFakeBatcher())

	_, err := cp.Create(context.Background(), cpCreateClaim("uid-nc", "pool1", "retired-sku", nil))
	if err == nil {
		t.Fatal("Create with a missing NodeClass = nil, want an error")
	}
	if !apierrors.IsNotFound(err) {
		t.Fatalf("precondition: the class must be NotFound, got %v", err)
	}
	if !karpcp.IsNodeClassNotReadyError(err) {
		t.Fatalf("Create with a missing NodeClass = %v, want NodeClassNotReadyError", err)
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-5 ────────────────────────────

// TestInstanceTypeAllocatableAccountsForOverhead: a real node's allocatable is below the SKU's
// nominal memory (kernel reserve, kube/system-reserved, eviction threshold). Advertising nominal
// memory as allocatable over-packs planned nodes and provisions an extra node per round.
func TestInstanceTypeAllocatableAccountsForOverhead(t *testing.T) {
	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{{Name: "standard-4-8", CPU: "4", Memory: "8Gi"}})
	it := its[0]
	capMem := it.Capacity[corev1.ResourceMemory]
	allocMem := it.Allocatable()[corev1.ResourceMemory]
	if allocMem.Cmp(capMem) >= 0 {
		t.Fatalf("Allocatable memory %s >= Capacity %s, want allocatable reduced by the modelled overhead", allocMem.String(), capMem.String())
	}

	// Create() copies the same view onto the NodeClaim status, which cluster state uses until
	// the node is initialized.
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t, cpNodeClass("standard-4-8", v1alpha1.InstanceTypeSpec{Name: "standard-4-8", CPU: "4", Memory: "8Gi"}))
	cp := cpProvider(cl, nil, nil, b)
	out, err := cp.Create(context.Background(), cpCreateClaim("uid-oh", "pool1", "standard-4-8", nil))
	if err != nil {
		t.Fatalf("Create = %v", err)
	}
	sc, sa := out.Status.Capacity[corev1.ResourceMemory], out.Status.Allocatable[corev1.ResourceMemory]
	if sa.Cmp(sc) >= 0 {
		t.Fatalf("status.allocatable memory %s >= status.capacity %s, want the overhead reflected", sa.String(), sc.String())
	}
}
