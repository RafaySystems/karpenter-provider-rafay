/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_findnode_test.go — findNodeProviderID matching rules and NodeClaimSKUs.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestFindNodeProviderIDMatchingRules(t *testing.T) {
	const joinedID = "rafay://pool1/oci-inst/host-j"
	after := cpBase.Add(10 * time.Minute)

	tests := []struct {
		name      string
		claim     func() *karpv1.NodeClaim
		objects   []client.Object
		want      string
		wantLists int // -1 means "do not check"
	}{
		{
			name:    "matches a node whose sku_name is the instance-type label",
			claim:   func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst-large") },
			objects: []client.Object{cpNode("n", "pool1", "oci-inst-large", joinedID, after)},
			want:    joinedID, wantLists: 2,
		},
		{
			name: "legacy: matches the NodeClass name when the claim has no instance-type label",
			claim: func() *karpv1.NodeClaim {
				c := cpPendingClaim("c", "uid-c", "pool1", "oci-inst")
				delete(c.Labels, corev1.LabelInstanceTypeStable)
				return c
			},
			objects: []client.Object{cpNode("n", "pool1", "oci-inst", joinedID, after)},
			want:    joinedID, wantLists: 2,
		},
		{
			name:    "a node of another sku is ignored",
			claim:   func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{cpNode("n", "pool1", "gpu-inst", joinedID, after)},
			want:    "", wantLists: 2,
		},
		{
			name:    "a node of another pool is ignored",
			claim:   func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{cpNode("n", "pool2", "oci-inst", "rafay://pool2/oci-inst/host-j", after)},
			want:    "", wantLists: 2,
		},
		{
			name:  "a node owned by another non-pending NodeClaim is excluded",
			claim: func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{
				cpNode("n", "pool1", "oci-inst", joinedID, after),
				cpClaim("owner", "uid-owner", "pool1", "oci-inst", joinedID),
			},
			want: "", wantLists: 2,
		},
		{
			name:  "the claim's own record in the list does not exclude anything",
			claim: func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{
				cpNode("n", "pool1", "oci-inst", joinedID, after),
				// Stored copy of the same claim (same UID) already carrying the real ID: skipped as self.
				cpClaim("c", "uid-c", "pool1", "oci-inst", joinedID),
			},
			want: joinedID, wantLists: 2,
		},
		{
			name:    "a node created before the claim is excluded (pre-existing node)",
			claim:   func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{cpNode("n", "pool1", "oci-inst", joinedID, cpBase.Add(-time.Minute))},
			want:    "", wantLists: 2,
		},
		{
			name:    "a node created at the same instant as the claim is excluded",
			claim:   func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{cpNode("n", "pool1", "oci-inst", joinedID, cpBase)},
			want:    "", wantLists: 2,
		},
		{
			name:    "a node with an empty spec.providerID is skipped",
			claim:   func() *karpv1.NodeClaim { return cpPendingClaim("c", "uid-c", "pool1", "oci-inst") },
			objects: []client.Object{cpNode("n", "pool1", "oci-inst", "", after)},
			want:    "", wantLists: 2,
		},
		{
			name: "no nodepool label short-circuits without a LIST",
			claim: func() *karpv1.NodeClaim {
				c := cpPendingClaim("c", "uid-c", "pool1", "oci-inst")
				delete(c.Labels, karpv1.NodePoolLabelKey)
				return c
			},
			objects: []client.Object{cpNode("n", "pool1", "oci-inst", joinedID, after)},
			want:    "", wantLists: 0,
		},
		{
			name: "no sku at all (no label, no nodeClassRef) short-circuits without a LIST",
			claim: func() *karpv1.NodeClaim {
				c := cpPendingClaim("c", "uid-c", "pool1", "oci-inst")
				delete(c.Labels, corev1.LabelInstanceTypeStable)
				c.Spec.NodeClassRef = nil
				return c
			},
			objects: []client.Object{cpNode("n", "pool1", "oci-inst", joinedID, after)},
			want:    "", wantLists: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := cpFakeClient(t, tt.objects...)
			reader := &cpReader{Reader: cl}
			cp := cpProvider(cl, reader, nil, nil)

			got, err := cp.findNodeProviderID(context.Background(), tt.claim())
			if err != nil {
				t.Fatalf("findNodeProviderID: %v", err)
			}
			if got != tt.want {
				t.Errorf("findNodeProviderID = %q, want %q", got, tt.want)
			}
			if tt.wantLists >= 0 && reader.listCount() != tt.wantLists {
				t.Errorf("LIST calls = %d, want %d", reader.listCount(), tt.wantLists)
			}
		})
	}
}

// Both LISTs go through the uncached reader and either failure is returned, wrapped with which
// list failed.
func TestFindNodeProviderIDListErrors(t *testing.T) {
	tests := []struct {
		name    string
		failOn  func(client.ObjectList) bool
		wantSub string
	}{
		{
			name:    "nodeclaim LIST error",
			failOn:  func(l client.ObjectList) bool { _, ok := l.(*karpv1.NodeClaimList); return ok },
			wantSub: "list nodeclaims",
		},
		{
			name:    "node LIST error",
			failOn:  func(l client.ObjectList) bool { _, ok := l.(*corev1.NodeList); return ok },
			wantSub: "list nodes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := cpFakeClient(t, cpNode("n", "pool1", "oci-inst", "rafay://pool1/oci-inst/host-j", cpBase.Add(time.Minute)))
			reader := &cpReader{Reader: cl, listErr: func(l client.ObjectList) error {
				if tt.failOn(l) {
					return cpErrBoom
				}
				return nil
			}}
			cp := cpProvider(cl, reader, nil, nil)

			got, err := cp.findNodeProviderID(context.Background(), cpPendingClaim("c", "uid-c", "pool1", "oci-inst"))
			if !errors.Is(err, cpErrBoom) {
				t.Fatalf("err = %v, want it to wrap %v", err, cpErrBoom)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("err = %q, want it to say %q", err, tt.wantSub)
			}
			if got != "" {
				t.Errorf("providerID = %q on error, want empty", got)
			}
		})
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-3 (adoption annotation) ────────────────────────────

// TestFindNodeProviderIDTreatsAdoptedAnnotationAsUsed: a NodeClaim the adoption controller just
// created carries karpenter.rafay.io/adopted-provider-id=X with an empty status.providerID until
// its first Create(). That node is spoken for and must not be handed to a pending claim's Delete.
func TestFindNodeProviderIDTreatsAdoptedAnnotationAsUsed(t *testing.T) {
	const adoptedID = "rafay://pool1/oci-inst/host-console"
	adopted := cpClaim("adopted", "uid-adopted", "pool1", "oci-inst", "")
	adopted.Annotations = map[string]string{AdoptedProviderIDAnnotationKey: adoptedID}
	node := cpNode("host-console", "pool1", "oci-inst", adoptedID, cpBase.Add(time.Minute))
	cl := cpFakeClient(t, adopted, node)
	cp := cpProvider(cl, nil, nil, nil)

	got, err := cp.findNodeProviderID(context.Background(), cpPendingClaim("c", "uid-c", "pool1", "oci-inst"))
	if err != nil {
		t.Fatalf("findNodeProviderID: %v", err)
	}
	if got == adoptedID {
		t.Fatalf("findNodeProviderID returned %s, which another NodeClaim is adopting", got)
	}
}

// ──────────────────────────── NodeClaimSKUs ────────────────────────────

// TestNodeClaimSKUsLegacyFallbackWithoutLabel: a NodeClaim that has not been through Create() yet
// (no instance-type label) still resolves through the single-SKU NodeClass name.
func TestNodeClaimSKUsLegacyFallbackWithoutLabel(t *testing.T) {
	nc := &karpv1.NodeClaim{
		Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{Name: "oci-inst"}},
	}
	skus := NodeClaimSKUs(nc)
	if len(skus) != 1 || !skus["oci-inst"] {
		t.Fatalf("NodeClaimSKUs = %v, want exactly {oci-inst}", skus)
	}
	// Nothing to match on at all.
	if got := NodeClaimSKUs(&karpv1.NodeClaim{}); len(got) != 0 {
		t.Errorf("NodeClaimSKUs(empty) = %v, want empty", got)
	}
	empty := &karpv1.NodeClaim{Spec: karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{Name: ""}}}
	if got := NodeClaimSKUs(empty); len(got) != 0 {
		t.Errorf("NodeClaimSKUs(empty class name) = %v, want empty", got)
	}
}

// ──────────────────────────── regression: R1-prov-cloudprovider-7 ────────────────────────────

// TestNodeClaimSKUsPrefersInstanceTypeLabel: once Create() has stamped the instance type it
// selected, the NodeClass name must not also match — a multi-SKU class named after one of its SKUs
// would otherwise let a NodeClaim that selected SKU B bind to (or remove) an A machine.
func TestNodeClaimSKUsPrefersInstanceTypeLabel(t *testing.T) {
	nc := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{corev1.LabelInstanceTypeStable: "oci-inst-large"}},
		Spec:       karpv1.NodeClaimSpec{NodeClassRef: &karpv1.NodeClassReference{Name: "oci-inst"}},
	}
	skus := NodeClaimSKUs(nc)
	if !skus["oci-inst-large"] {
		t.Error("selected instance type must be an acceptable sku_name")
	}
	if skus["oci-inst"] {
		t.Errorf("NodeClaimSKUs = %v: the NodeClass name must not match once an instance-type label is present", skus)
	}
}
