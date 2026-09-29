/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package nodeadoption

// p2_ownership_test.go — regression test for R1-prov-adoption-providerid-2: the adoption pass and
// NodeProviderIDController each serialise within themselves but used to run unserialised against
// each other. Adoption LISTs NodeClaims while a scale-out's NodeClaim P is still empty (Create()
// awaiting the broker ACK) and finds operator-added node N unowned; P then turns pending and the
// provider-ID controller, whose LIST does not contain adoption's claim for N yet, binds N to P;
// adoption creates its claim for N — two NodeClaims for one machine. cprovider.NodeOwnershipMu
// serialises the two read-decide-write cycles.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/controllers/nodeproviderid"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

// TestP2AdoptionAndProviderIDBindingAreSerialised: mid-pass (between adoption's NodeClaim LIST
// and its Create for N), P turns pending and a NodeProviderIDController reconcile of P starts.
// It must wait for the pass to finish, then see adoption's claim for N and leave P pending — N
// ends up owned by exactly one NodeClaim.
func TestP2AdoptionAndProviderIDBindingAreSerialised(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	nodeID := rafay.BuildProviderID(testPool, testSKU, "worker-n")
	// N is newer than P, so the provider-ID controller would bind it to P.
	n := newNode("worker-n", withProviderID(nodeID), withCreated(baseTime.Add(2*time.Minute)))
	p := newNodeClaim("claim-p", testPool, "", baseTime)

	var (
		c        client.Client
		bindDone = make(chan struct{})
		bindErr  error
		once     sync.Once
	)
	c = adNewInterceptedClient(interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if nc, ok := obj.(*karpv1.NodeClaim); ok && nc.Annotations[adoptedNodeAnnotationKey] == "worker-n" {
				once.Do(func() {
					// Create() returned: Karpenter's Launch stamps the pending providerID on P.
					stored := &karpv1.NodeClaim{}
					if err := cl.Get(ctx, client.ObjectKeyFromObject(p), stored); err != nil {
						t.Errorf("get claim-p: %v", err)
						return
					}
					patched := stored.DeepCopy()
					patched.Status.ProviderID = cprovider.PendingProviderIDPrefix + "uid-p"
					if err := cl.Status().Patch(ctx, patched, client.MergeFrom(stored)); err != nil {
						t.Errorf("mark claim-p pending: %v", err)
						return
					}
					// The provider-ID controller reconciles P now, concurrently with this pass.
					go func() {
						defer close(bindDone)
						_, bindErr = nodeproviderid.NewControllerWithReader(c, c).Reconcile(context.Background(), patched)
					}()
					// Give it time to reach (and block on) the ownership lock.
					time.Sleep(100 * time.Millisecond)
				})
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, pool, n, p)
	ctrl, _ := newTestController(c)

	if _, err := ctrl.Reconcile(context.Background(), pool); err != nil {
		t.Fatalf("adoption reconcile: %v", err)
	}
	select {
	case <-bindDone:
	case <-time.After(10 * time.Second):
		t.Fatal("provider-ID reconcile did not finish")
	}
	if bindErr != nil {
		t.Fatalf("provider-ID reconcile: %v", bindErr)
	}

	owners := 0
	for _, nc := range listClaims(t, c) {
		if nc.Status.ProviderID == nodeID || nc.Annotations[cprovider.AdoptedProviderIDAnnotationKey] == nodeID {
			owners++
		}
	}
	if owners != 1 {
		t.Fatalf("node %s is owned by %d NodeClaims, want exactly 1", nodeID, owners)
	}
	got := getNodeClaimP(t, c).Status.ProviderID
	if !strings.HasPrefix(got, cprovider.PendingProviderIDPrefix) {
		t.Fatalf("claim-p providerID = %q, want still pending (N was adopted first)", got)
	}
}

func getNodeClaimP(t *testing.T, c client.Client) *karpv1.NodeClaim {
	t.Helper()
	nc := &karpv1.NodeClaim{}
	if err := c.Get(context.Background(), client.ObjectKey{Name: "claim-p"}, nc); err != nil {
		t.Fatalf("get claim-p: %v", err)
	}
	return nc
}
