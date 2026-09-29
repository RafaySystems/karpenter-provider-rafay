/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_overhead_test.go — the instance type overhead model (R1-prov-cloudprovider-5), the
// well-known-label requirements (R1-karpenter-core-interaction-8) and Create()'s NodeClass
// readiness mapping (R1-prov-cloudprovider-8).

import (
	"context"
	"testing"

	"github.com/awslabs/operatorpkg/status"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

func TestOverheadFromEnvDefaultsAndOverrides(t *testing.T) {
	t.Setenv("RAFAY_VM_MEMORY_OVERHEAD_PERCENT", "")
	t.Setenv("RAFAY_KUBE_RESERVED_CPU", "")
	t.Setenv("RAFAY_KUBE_RESERVED_MEMORY", "")
	oh := overheadFromEnv()
	if oh.vmMemoryPercent != defaultVMMemoryOverheadPercent {
		t.Errorf("default vm percent = %v, want %v", oh.vmMemoryPercent, defaultVMMemoryOverheadPercent)
	}
	if oh.kubeReservedCPU.String() != defaultKubeReservedCPU || oh.kubeReservedMemory.String() != defaultKubeReservedMemory || oh.evictionMemory.String() != defaultEvictionMemory {
		t.Errorf("defaults = cpu %s mem %s evict %s", oh.kubeReservedCPU.String(), oh.kubeReservedMemory.String(), oh.evictionMemory.String())
	}

	t.Setenv("RAFAY_VM_MEMORY_OVERHEAD_PERCENT", "5")
	t.Setenv("RAFAY_KUBE_RESERVED_CPU", "200m")
	t.Setenv("RAFAY_KUBE_RESERVED_MEMORY", "1Gi")
	oh = overheadFromEnv()
	if oh.vmMemoryPercent != 5 || oh.kubeReservedCPU.String() != "200m" || oh.kubeReservedMemory.String() != "1Gi" {
		t.Errorf("overrides not applied: %+v", oh)
	}

	// Garbage keeps the defaults.
	t.Setenv("RAFAY_VM_MEMORY_OVERHEAD_PERCENT", "lots")
	t.Setenv("RAFAY_KUBE_RESERVED_CPU", "-1")
	t.Setenv("RAFAY_KUBE_RESERVED_MEMORY", "1 gig")
	oh = overheadFromEnv()
	if oh.vmMemoryPercent != defaultVMMemoryOverheadPercent || oh.kubeReservedCPU.String() != defaultKubeReservedCPU || oh.kubeReservedMemory.String() != defaultKubeReservedMemory {
		t.Errorf("invalid values must keep the defaults: %+v", oh)
	}
	t.Setenv("RAFAY_VM_MEMORY_OVERHEAD_PERCENT", "100")
	if oh = overheadFromEnv(); oh.vmMemoryPercent != defaultVMMemoryOverheadPercent {
		t.Errorf("100%% is not a usable overhead, want the default, got %v", oh.vmMemoryPercent)
	}
}

// The modelled overhead: 7.5% of nominal memory as system-reserved, kube-reserved cpu/memory and
// the kubelet's default eviction threshold; Allocatable() reflects all of it, Capacity stays
// nominal.
func TestInstanceTypeOverheadValues(t *testing.T) {
	oh := instanceTypeOverhead{
		vmMemoryPercent:    7.5,
		kubeReservedCPU:    resource.MustParse("80m"),
		kubeReservedMemory: resource.MustParse("255Mi"),
		evictionMemory:     resource.MustParse("100Mi"),
	}
	mem := resource.MustParse("8Gi")
	got := oh.overheadFor(mem)
	if v := got.SystemReserved[corev1.ResourceMemory]; v.Value() != int64(float64(mem.Value())*0.075) {
		t.Errorf("system-reserved memory = %s, want 7.5%% of 8Gi", v.String())
	}
	if v := got.KubeReserved[corev1.ResourceCPU]; v.String() != "80m" {
		t.Errorf("kube-reserved cpu = %s, want 80m", v.String())
	}
	total := got.Total()
	wantMem := int64(float64(mem.Value())*0.075) + 255*1024*1024 + 100*1024*1024
	if v := total[corev1.ResourceMemory]; v.Value() != wantMem {
		t.Errorf("total memory overhead = %d, want %d", v.Value(), wantMem)
	}

	its := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{{Name: "standard-4-8", CPU: "4", Memory: "8Gi"}})
	it := its[0]
	if c := it.Capacity[corev1.ResourceMemory]; c.Cmp(mem) != 0 {
		t.Errorf("capacity memory = %s, want the nominal 8Gi", c.String())
	}
	alloc := it.Allocatable()
	if a := alloc[corev1.ResourceCPU]; a.Cmp(resource.MustParse("4")) >= 0 {
		t.Errorf("allocatable cpu = %s, want below the nominal 4", a.String())
	}
	if a := alloc[corev1.ResourceMemory]; a.Cmp(mem) >= 0 || a.Sign() <= 0 {
		t.Errorf("allocatable memory = %s, want between 0 and 8Gi", a.String())
	}
	// The scheduler-only nodes entry is untouched by the overhead.
	if n := alloc[resources.Node]; n.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("allocatable nodes = %s, want 1", n.String())
	}
}

// A pod requesting exactly the nominal SKU size no longer fits the planned node (it would not fit
// the real one either); the next size up is selected.
func TestInstanceSelectionAccountsForOverhead(t *testing.T) {
	b := cpNewFakeBatcher()
	cl := cpFakeClient(t, cpTwoSKUClass()) // small 2/4Gi, large 8/16Gi
	cp := cpProvider(cl, nil, nil, b)

	claim := cpCreateClaim("uid-full", "pool1", "mixed", corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("4Gi")})
	if _, err := cp.Create(context.Background(), claim); err != nil {
		t.Fatalf("Create = %v", err)
	}
	if adds := b.addCalls(); len(adds) != 1 || adds[0].req.InstanceType != "large" {
		t.Fatalf("adds = %+v, want the large SKU (4Gi does not fit small's allocatable)", adds)
	}
}

// Region is defined as Exists: no label is stamped, and a pod pinning a region is still
// compatible with the SKU.
func TestRegionRequirementIsExistsWithoutLabel(t *testing.T) {
	it := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{{Name: "oci-inst", CPU: "4", Memory: "8Gi"}})[0]
	if r := it.Requirements.Get(corev1.LabelTopologyRegion); r == nil || r.Operator() != corev1.NodeSelectorOpExists {
		t.Fatalf("region requirement = %v, want Exists", r)
	}
	if _, ok := requirementsToLabels(it.Requirements)[corev1.LabelTopologyRegion]; ok {
		t.Error("an Exists region requirement must not become a NodeClaim label")
	}
	labels := requirementsToLabels(it.Requirements)
	if labels[corev1.LabelArchStable] != defaultArchitecture || labels[corev1.LabelOSStable] != defaultOperatingSystem {
		t.Errorf("labels = %v, want the default arch/os stamped", labels)
	}
	claim := &karpv1.NodeClaim{Spec: karpv1.NodeClaimSpec{Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
		{Key: corev1.LabelTopologyRegion, Operator: corev1.NodeSelectorOpIn, Values: []string{"dc-1"}},
		{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{defaultArchitecture}},
	}}}
	if got := filterCompatibleInstanceTypes([]*karpcp.InstanceType{it}, claim); len(got) != 1 {
		t.Errorf("a region-pinning amd64 NodeClaim must stay compatible, got %v", instanceTypeNames(got))
	}
}

// ──────────────────────────── Create: NodeClass readiness ────────────────────────────

func TestCreateNotReadyOrInvalidNodeClassIsNodeClassNotReady(t *testing.T) {
	notReady := cpNodeClass("bad", v1alpha1.InstanceTypeSpec{Name: "bad", CPU: "4", Memory: "8Gi"})
	notReady.StatusConditions().SetFalse(status.ConditionReady, "ValidationFailed", "instanceTypes[0].cpu: invalid")
	invalid := cpNodeClass("garbage", v1alpha1.InstanceTypeSpec{Name: "garbage", CPU: "four", Memory: "8Gi"})
	cl := cpFakeClient(t, notReady, invalid, cpNodeClass("empty"))
	cp := cpProvider(cl, nil, nil, cpNewFakeBatcher())

	for _, class := range []string{"bad", "garbage", "empty"} {
		_, err := cp.Create(context.Background(), cpCreateClaim("uid-"+class, "pool1", class, nil))
		if !karpcp.IsNodeClassNotReadyError(err) {
			t.Errorf("Create with class %q = %v, want NodeClassNotReadyError", class, err)
		}
	}

	// A transient API error stays plain so the launch is retried.
	failing := &cpClient{Client: cl, getErr: cpErrBoom}
	cpErr := cpProvider(failing, nil, nil, cpNewFakeBatcher())
	_, err := cpErr.Create(context.Background(), cpCreateClaim("uid-api", "pool1", "bad", nil))
	if err == nil || karpcp.IsNodeClassNotReadyError(err) {
		t.Errorf("Create with a failing Get = %v, want a plain (retryable) error", err)
	}
}
