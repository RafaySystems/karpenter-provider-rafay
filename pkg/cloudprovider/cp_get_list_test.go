/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_get_list_test.go — CloudProvider.Get / List and their kube fallbacks (getNodeFromKube,
// listNodesFromKube). rafay.Client is an exported interface, so every branch is driven with a stub.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
)

const cpGetID = "rafay://pool1/oci-inst/host-get"

// ──────────────────────────── Get ────────────────────────────

func TestGetEmptyProviderIDIsError(t *testing.T) {
	rc := &cpFakeRafayClient{}
	cp := cpProvider(cpFakeClient(t), nil, rc, nil)
	if _, err := cp.Get(context.Background(), ""); err == nil {
		t.Fatal("Get(\"\") = nil error, want an error")
	} else if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Get(\"\") = %v, want a plain error, not NodeClaimNotFound", err)
	}
	if len(rc.getCalls) != 0 {
		t.Errorf("client called for an empty ID: %v", rc.getCalls)
	}
}

// A pending ID means "provisioning in progress": the instance exists, no lookup is made.
func TestGetPendingProviderIDExists(t *testing.T) {
	rc := &cpFakeRafayClient{}
	cp := cpProvider(cpFakeClient(t), nil, rc, nil)
	id := PendingProviderIDPrefix + "uid-1"

	nc, err := cp.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(pending) = %v, want nil", err)
	}
	if nc == nil || nc.Status.ProviderID != id {
		t.Fatalf("Get(pending) = %+v, want a NodeClaim carrying %s", nc, id)
	}
	if len(rc.getCalls) != 0 {
		t.Errorf("client called for a pending ID: %v", rc.getCalls)
	}
}

func TestGetFromRafayClient(t *testing.T) {
	rc := &cpFakeRafayClient{getNodeFn: func(providerID string) (*rafay.NodeInfo, error) {
		return &rafay.NodeInfo{ProviderID: providerID, Capacity: map[string]string{
			"cpu":    "4",
			"memory": "8Gi",
			"bogus":  "not-a-quantity",
		}}, nil
	}}
	cp := cpProvider(cpFakeClient(t), nil, rc, nil)

	nc, err := cp.Get(context.Background(), cpGetID)
	if err != nil {
		t.Fatalf("Get = %v", err)
	}
	if nc.Status.ProviderID != cpGetID {
		t.Errorf("providerID = %q, want %q", nc.Status.ProviderID, cpGetID)
	}
	if got, want := nc.Status.Capacity[corev1.ResourceCPU], resource.MustParse("4"); got.Cmp(want) != 0 {
		t.Errorf("cpu = %s, want 4", got.String())
	}
	if got, want := nc.Status.Capacity[corev1.ResourceMemory], resource.MustParse("8Gi"); got.Cmp(want) != 0 {
		t.Errorf("memory = %s, want 8Gi", got.String())
	}
	if _, ok := nc.Status.Capacity["bogus"]; ok {
		t.Error("an unparseable capacity value must be dropped, not fail the lookup")
	}
	if len(rc.getCalls) != 1 || rc.getCalls[0] != cpGetID {
		t.Errorf("client GetNode calls = %v, want [%s]", rc.getCalls, cpGetID)
	}
}

func TestGetErrorMapping(t *testing.T) {
	other := errors.New("broker: connection reset")
	tests := []struct {
		name         string
		err          error
		wantNotFound bool
		wantIs       error
	}{
		{name: "ErrNodeNotFound maps to NodeClaimNotFound", err: rafay.ErrNodeNotFound, wantNotFound: true},
		{name: "wrapped ErrNodeNotFound maps to NodeClaimNotFound", err: fmt.Errorf("lookup: %w", rafay.ErrNodeNotFound), wantNotFound: true},
		{name: "other errors are propagated", err: other, wantIs: other},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &cpFakeRafayClient{getNodeFn: func(string) (*rafay.NodeInfo, error) { return nil, tt.err }}
			cp := cpProvider(cpFakeClient(t), nil, rc, nil)

			nc, err := cp.Get(context.Background(), cpGetID)
			if nc != nil {
				t.Errorf("Get returned %+v alongside an error", nc)
			}
			if karpcp.IsNodeClaimNotFoundError(err) != tt.wantNotFound {
				t.Errorf("IsNodeClaimNotFoundError(%v) = %v, want %v", err, !tt.wantNotFound, tt.wantNotFound)
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Errorf("err = %v, want it to be %v", err, tt.wantIs)
			}
		})
	}
}

// ErrGetNodeUnsupported (the broker client) falls back to the Node objects: found with the
// node's capacity, or NodeClaimNotFound — which is what lets the termination controller skip
// the drain of a node that is already gone.
func TestGetFallsBackToKube(t *testing.T) {
	node := cpNode("host-get", "pool1", "oci-inst", cpGetID, cpBase)
	node.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}
	other := cpNode("host-other", "pool1", "oci-inst", "rafay://pool1/oci-inst/host-other", cpBase)
	cl := cpFakeClient(t, node, other)
	rc := &cpFakeRafayClient{} // GetNode → ErrGetNodeUnsupported
	cp := cpProvider(cl, nil, rc, nil)

	nc, err := cp.Get(context.Background(), cpGetID)
	if err != nil {
		t.Fatalf("Get (kube fallback) = %v", err)
	}
	if nc.Status.ProviderID != cpGetID {
		t.Errorf("providerID = %q, want %q", nc.Status.ProviderID, cpGetID)
	}
	if got, want := nc.Status.Capacity[corev1.ResourceCPU], resource.MustParse("8"); got.Cmp(want) != 0 {
		t.Errorf("cpu = %s, want the Node's 8", got.String())
	}

	_, err = cp.Get(context.Background(), "rafay://pool1/oci-inst/host-gone")
	if !karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatalf("Get of an ID no Node carries = %v, want NodeClaimNotFoundError", err)
	}
}

func TestGetKubeFallbackListError(t *testing.T) {
	cl := &cpClient{Client: cpFakeClient(t), listErr: cpErrBoom}
	cp := cpProvider(cl, nil, &cpFakeRafayClient{}, nil)

	_, err := cp.Get(context.Background(), cpGetID)
	if !errors.Is(err, cpErrBoom) {
		t.Fatalf("Get with failing Node LIST = %v, want %v", err, cpErrBoom)
	}
	if karpcp.IsNodeClaimNotFoundError(err) {
		t.Fatal("a LIST failure must not be reported as NodeClaimNotFound")
	}
}

// ──────────────────────────── List ────────────────────────────

func cpProviderIDs(claims []*karpv1.NodeClaim) map[string]bool {
	ids := make(map[string]bool, len(claims))
	for _, c := range claims {
		ids[c.Status.ProviderID] = true
	}
	return ids
}

func TestListFromRafayClient(t *testing.T) {
	rc := &cpFakeRafayClient{listNodesFn: func(string) ([]*rafay.NodeInfo, error) {
		return []*rafay.NodeInfo{{ProviderID: "rafay://p/s/a"}, {ProviderID: "rafay://p/s/b", Capacity: map[string]string{"cpu": "2"}}}, nil
	}}
	cp := cpProvider(cpFakeClient(t, cpNode("kube-only", "p", "s", "rafay://p/s/kube", cpBase)), nil, rc, nil)

	got, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List = %v", err)
	}
	ids := cpProviderIDs(got)
	if len(got) != 2 || !ids["rafay://p/s/a"] || !ids["rafay://p/s/b"] {
		t.Fatalf("List = %v, want the two client nodes", ids)
	}
	if len(rc.listCalls) != 1 || rc.listCalls[0] != cpClusterID {
		t.Errorf("ListNodes calls = %v, want [%s]", rc.listCalls, cpClusterID)
	}
}

func TestListFallsBackToKube(t *testing.T) {
	cl := cpFakeClient(t,
		cpNode("a", "p", "s", "rafay://p/s/a", cpBase),
		newNode("aws", "aws:///us-east-1a/i-123"),
	)
	cp := cpProvider(cl, nil, &cpFakeRafayClient{}, nil) // ListNodes → ErrListNodesUnsupported

	got, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List = %v", err)
	}
	if ids := cpProviderIDs(got); len(got) != 1 || !ids["rafay://p/s/a"] {
		t.Fatalf("List = %v, want only the rafay node from kube", ids)
	}
}

func TestListPropagatesOtherErrors(t *testing.T) {
	other := errors.New("broker: unavailable")
	rc := &cpFakeRafayClient{listNodesFn: func(string) ([]*rafay.NodeInfo, error) { return nil, other }}
	cp := cpProvider(cpFakeClient(t, cpNode("a", "p", "s", "rafay://p/s/a", cpBase)), nil, rc, nil)

	got, err := cp.List(context.Background())
	if !errors.Is(err, other) {
		t.Fatalf("List = (%v, %v), want the client error (no silent kube fallback)", got, err)
	}
}

// A Node that is Terminating (Karpenter's finalizer still on it) must stay in List(): the core
// garbage-collection controller deletes any Registered NodeClaim whose providerID is absent from
// List(), which would reap a NodeClaim mid-termination.
func TestListNodesFromKubeKeepsTerminatingNode(t *testing.T) {
	terminating := cpNode("t", "p", "s", "rafay://p/s/t", cpBase)
	ts := metav1.NewTime(cpBase.Add(time.Hour))
	terminating.DeletionTimestamp = &ts
	terminating.Finalizers = []string{karpv1.TerminationFinalizer}
	cl := cpFakeClient(t, terminating, cpNode("live", "p", "s", "rafay://p/s/live", cpBase))
	cp := cpProvider(cl, nil, nil, nil)

	got, err := cp.listNodesFromKube(context.Background())
	if err != nil {
		t.Fatalf("listNodesFromKube = %v", err)
	}
	if ids := cpProviderIDs(got); len(got) != 2 || !ids["rafay://p/s/t"] {
		t.Fatalf("listNodesFromKube = %v, want the Terminating node still listed", ids)
	}
}

func TestListNodesFromKubeListError(t *testing.T) {
	cl := &cpClient{Client: cpFakeClient(t), listErr: cpErrBoom}
	cp := cpProvider(cl, nil, &cpFakeRafayClient{}, nil)

	if _, err := cp.List(context.Background()); !errors.Is(err, cpErrBoom) {
		t.Fatalf("List with failing Node LIST = %v, want %v", err, cpErrBoom)
	}
}
