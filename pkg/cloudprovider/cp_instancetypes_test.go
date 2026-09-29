/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_instancetypes_test.go — rafayInstanceTypesToKarpenter requirements (arch/os/zone),
// requirementsToLabels and the GetInstanceTypes error paths.

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func cpSortedValues(reqs scheduling.Requirements, key string) []string {
	if !reqs.Has(key) {
		return nil
	}
	vals := reqs.Get(key).Values()
	// Values() order is not specified; normalise for comparison.
	out := append([]string(nil), vals...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func cpEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRafayInstanceTypesToKarpenterArchOSRequirements(t *testing.T) {
	tests := []struct {
		name     string
		spec     v1alpha1.InstanceTypeSpec
		wantArch []string // nil: requirement must be absent
		wantOS   []string
	}{
		{
			name:     "values are trimmed and empties dropped",
			spec:     v1alpha1.InstanceTypeSpec{Name: "a", CPU: "4", Memory: "8Gi", Architectures: []string{" amd64 ", "", "arm64"}, OperatingSystems: []string{"linux "}},
			wantArch: []string{"amd64", "arm64"},
			wantOS:   []string{"linux"},
		},
		// A class that lists nothing usable gets the platform defaults (R1-karpenter-core-
		// interaction-8): every well-known label must be defined on an instance type.
		{
			name:     "only blank entries yields the defaults",
			spec:     v1alpha1.InstanceTypeSpec{Name: "b", CPU: "4", Memory: "8Gi", Architectures: []string{"  ", ""}, OperatingSystems: []string{""}},
			wantArch: []string{defaultArchitecture},
			wantOS:   []string{defaultOperatingSystem},
		},
		{
			name:     "unset lists yield the defaults",
			spec:     v1alpha1.InstanceTypeSpec{Name: "c", CPU: "4", Memory: "8Gi"},
			wantArch: []string{defaultArchitecture},
			wantOS:   []string{defaultOperatingSystem},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{tt.spec})[0]
			if got := cpSortedValues(it.Requirements, corev1.LabelArchStable); !cpEqualStrings(got, tt.wantArch) {
				t.Errorf("arch requirement = %v, want %v", got, tt.wantArch)
			}
			if got := cpSortedValues(it.Requirements, corev1.LabelOSStable); !cpEqualStrings(got, tt.wantOS) {
				t.Errorf("os requirement = %v, want %v", got, tt.wantOS)
			}
			// The offering carries the same requirement set.
			if len(it.Offerings) != 1 || !cpEqualStrings(cpSortedValues(it.Offerings[0].Requirements, corev1.LabelArchStable), tt.wantArch) {
				t.Errorf("offering requirements diverge from the instance type's")
			}
		})
	}
}

func TestRafayInstanceTypesToKarpenterExplicitZone(t *testing.T) {
	it := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{{Name: "z", CPU: "4", Memory: "8Gi", Zone: "az-1"}})[0]
	if got := it.Requirements.Get(corev1.LabelTopologyZone).Values(); len(got) != 1 || got[0] != "az-1" {
		t.Errorf("zone requirement = %v, want [az-1]", got)
	}
	if got := it.Requirements.Get(karpv1.CapacityTypeLabelKey).Values(); len(got) != 1 || got[0] != karpv1.CapacityTypeOnDemand {
		t.Errorf("capacity-type requirement = %v, want [%s]", got, karpv1.CapacityTypeOnDemand)
	}
	if labels := requirementsToLabels(it.Requirements); labels[corev1.LabelTopologyZone] != "az-1" {
		t.Errorf("zone label = %q, want az-1", labels[corev1.LabelTopologyZone])
	}
}

// requirementsToLabels is what Create() stamps on a NodeClaim: single-valued In requirements
// become labels; multi-valued arch/os are skipped (pinning one would be misleading); other
// operators are ignored.
func TestRequirementsToLabels(t *testing.T) {
	reqs := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, "oci-inst"),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "default"),
		scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, "amd64", "arm64"),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, "linux"),
		scheduling.NewRequirement("example.com/exists", corev1.NodeSelectorOpExists),
		scheduling.NewRequirement("example.com/notin", corev1.NodeSelectorOpNotIn, "x"),
	)
	got := requirementsToLabels(reqs)
	want := map[string]string{
		corev1.LabelInstanceTypeStable: "oci-inst",
		corev1.LabelTopologyZone:       "default",
		karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
		corev1.LabelOSStable:           "linux",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("label %s = %q, want %q", k, got[k], v)
		}
	}
	if _, ok := got[corev1.LabelArchStable]; ok {
		t.Errorf("multi-valued arch pinned to %q, want no arch label", got[corev1.LabelArchStable])
	}
	for _, k := range []string{"example.com/exists", "example.com/notin"} {
		if _, ok := got[k]; ok {
			t.Errorf("non-In requirement %s emitted as label %q", k, got[k])
		}
	}
	if len(got) != len(want) {
		t.Errorf("labels = %v, want exactly %v", got, want)
	}
	if got := requirementsToLabels(scheduling.NewRequirements()); len(got) != 0 {
		t.Errorf("requirementsToLabels(empty) = %v, want empty", got)
	}
}

// ──────────────────────────── regression: R1-karpenter-core-interaction-8 ────────────────────────────

// TestInstanceTypeRequirementsDefineWellKnownLabels: Karpenter's InstanceType contract
// (cloudprovider/types.go: "Must be defined for every well known label, even if empty") is what
// keeps AllowUndefinedWellKnownLabels from matching an arch-selecting pod to an SKU of the wrong
// architecture. Confirmed by reading rafayInstanceTypesToKarpenter: arch/os are added only when
// the class lists them and region never is.
func TestInstanceTypeRequirementsDefineWellKnownLabels(t *testing.T) {
	it := mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{{Name: "oci-inst", CPU: "4", Memory: "8Gi"}})[0]
	for _, key := range []string{corev1.LabelArchStable, corev1.LabelOSStable, corev1.LabelTopologyRegion} {
		if !it.Requirements.Has(key) {
			t.Errorf("requirement %s undefined for a class that does not set it; the contract requires every well-known label to be defined", key)
		}
	}

	// The observable consequence: a pod requiring arm64 must not be compatible with an SKU that
	// never declared arm64.
	claim := &karpv1.NodeClaim{Spec: karpv1.NodeClaimSpec{Requirements: []karpv1.NodeSelectorRequirementWithMinValues{
		{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"arm64"}},
	}}}
	if got := filterCompatibleInstanceTypes(mustInstanceTypes(t, []v1alpha1.InstanceTypeSpec{{Name: "oci-inst", CPU: "4", Memory: "8Gi"}}), claim); len(got) != 0 {
		t.Errorf("an arm64-requiring NodeClaim matched %v, an SKU with no declared architecture", instanceTypeNames(got))
	}
}

// ──────────────────────────── GetInstanceTypes ────────────────────────────

func TestGetInstanceTypesErrors(t *testing.T) {
	cl := cpFakeClient(t, cpNodeClass("empty-class"))
	cp := cpProvider(cl, nil, nil, nil)

	if _, err := cp.GetInstanceTypes(context.Background(), nil); err == nil {
		t.Error("GetInstanceTypes(nil) = nil error, want error")
	}

	noRef := &karpv1.NodePool{}
	noRef.Name = "pool-noref"
	if _, err := cp.GetInstanceTypes(context.Background(), noRef); err == nil || !strings.Contains(err.Error(), "no nodeClassRef") {
		t.Errorf("GetInstanceTypes(no nodeClassRef) = %v, want a 'no nodeClassRef' error naming the pool", err)
	}

	_, err := cp.GetInstanceTypes(context.Background(), newNodePoolFor("pool-missing", "missing-class"))
	if err == nil || !apierrors.IsNotFound(err) || !strings.Contains(err.Error(), `"missing-class"`) {
		t.Errorf("GetInstanceTypes(missing class) = %v, want a NotFound error naming the class", err)
	}

	_, err = cp.GetInstanceTypes(context.Background(), newNodePoolFor("pool-empty", "empty-class"))
	if err == nil || !strings.Contains(err.Error(), "add at least one entry to spec.instanceTypes") {
		t.Errorf("GetInstanceTypes(zero instanceTypes) = %v, want the user-facing hint", err)
	}
}

// A nil poolBackoff (the constructor's "no backoff" case) must not hold anything or panic.
func TestGetInstanceTypesNilBackoff(t *testing.T) {
	cl := cpFakeClient(t, cpNodeClass("bm", v1alpha1.InstanceTypeSpec{Name: "bm", CPU: "4", Memory: "8Gi"}))
	cp := NewCloudProvider(cl, cl, nil, cpClusterID, cpProjectID, nil, nil)

	its, err := cp.GetInstanceTypes(context.Background(), newNodePoolFor("pool1", "bm"))
	if err != nil {
		t.Fatalf("GetInstanceTypes = %v", err)
	}
	if len(its) != 1 || len(its[0].Offerings.Available()) != 1 {
		t.Fatalf("want one instance type with its offering available, got %d types", len(its))
	}
}
