/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package batchresume

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

// msClaimWithAnnotation builds a pending NodeClaim whose batch annotation is stored verbatim
// (claim() only sets it when non-empty, which hides the whitespace cases).
func msClaimWithAnnotation(name, uid, annotation string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			UID:         types.UID(uid),
			Annotations: map[string]string{cprovider.BatchIDAnnotationKey: annotation},
		},
		Status: karpv1.NodeClaimStatus{ProviderID: cprovider.PendingProviderIDPrefix + uid},
	}
}

// msFailingListReader is an API reader whose List always fails.
func msFailingListReader(objs ...client.Object) client.Reader {
	return interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(objs...).Build(),
		interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("apiserver unavailable")
			},
		})
}

// msResolvedBatch reports whether Enqueue(op) resolves immediately as an in-flight duplicate and
// with which batch: that is only true for ops the batcher is tracking, so it tells resumed ops
// apart from ones that were never registered without touching the batcher's internals.
func msResolvedBatch(b *rafay.NodeBatcher, op string) (string, bool) {
	select {
	case r := <-b.Enqueue(op, rafay.AddNodesRequest{OperationID: op}):
		return r.BatchID, r.Duplicate
	default:
		return "", false
	}
}

// A List failure must not stop the manager: Start logs, returns nil, and resumes nothing.
func TestMsStartListErrorIsSwallowed(t *testing.T) {
	reader := msFailingListReader(msClaimWithAnnotation("a", "uid-a", "batch-1"))
	batcher := rafay.NewNodeBatcher(nil)

	if err := NewController(reader, batcher).Start(context.Background()); err != nil {
		t.Fatalf("Start must not propagate a List error, got %v", err)
	}
	if batcher.Tracking("batch-1") {
		t.Error("nothing may be resumed from a failed List")
	}
}

// The batch annotation is trimmed before use; a whitespace-only annotation counts as absent.
func TestMsStartTrimsBatchAnnotation(t *testing.T) {
	reader := newReader(
		msClaimWithAnnotation("padded", "uid-p", "  batch-9\n"),
		msClaimWithAnnotation("blank", "uid-b", "   "),
	)
	batcher := rafay.NewNodeBatcher(nil)
	if err := NewController(reader, batcher).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !batcher.Tracking("batch-9") {
		t.Error("a padded annotation must resume batch-9")
	}
	if batcher.Tracking("   ") || batcher.Tracking("") {
		t.Error("a blank annotation must not register a batch")
	}
	if id, dup := msResolvedBatch(batcher, "uid-p"); !dup || id != "batch-9" {
		t.Errorf("uid-p should be in flight under batch-9, got (%q, %v)", id, dup)
	}
	if _, dup := msResolvedBatch(batcher, "uid-b"); dup {
		t.Error("uid-b carries no usable batch id and must not be in flight")
	}
}

// A batch the batcher already tracks (say it was resumed by an earlier pass, or sent by this
// process) is left alone — its item set is not replaced — while other batches in the same pass
// are still resumed.
func TestMsStartDoesNotReplaceAlreadyTrackedBatch(t *testing.T) {
	batcher := rafay.NewNodeBatcher(nil)
	if !batcher.ResumeAddBatch("batch-1", []string{"pre-op"}) {
		t.Fatal("pre-registering batch-1 failed")
	}
	reader := newReader(
		msClaimWithAnnotation("a", "uid-a", "batch-1"), // batch already tracked with a different op
		msClaimWithAnnotation("c", "uid-c", "batch-2"), // fresh batch in the same pass
	)
	if err := NewController(reader, batcher).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !batcher.Tracking("batch-1") || !batcher.Tracking("batch-2") {
		t.Fatal("both batches should be tracked afterwards")
	}
	if id, dup := msResolvedBatch(batcher, "pre-op"); !dup || id != "batch-1" {
		t.Errorf("the pre-registered op must still be in flight under batch-1, got (%q, %v)", id, dup)
	}
	if id, dup := msResolvedBatch(batcher, "uid-a"); dup {
		t.Errorf("uid-a belongs to an already-tracked batch and must not be re-registered, got batch %q", id)
	}
	if id, dup := msResolvedBatch(batcher, "uid-c"); !dup || id != "batch-2" {
		t.Errorf("batch-2 must still be resumed in the same pass, got (%q, %v)", id, dup)
	}
}

// NodeClaims nodeadoption creates carry a real providerID and no batch annotation; Start must
// skip them without registering anything.
func TestMsStartSkipsAdoptedNodeClaims(t *testing.T) {
	adopted := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "adopted", UID: types.UID("uid-x")},
		Status:     karpv1.NodeClaimStatus{ProviderID: "rafay://pool1/oci-inst/host-w0-abc"},
	}
	batcher := rafay.NewNodeBatcher(nil)
	if err := NewController(newReader(adopted), batcher).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, dup := msResolvedBatch(batcher, "uid-x"); dup {
		t.Error("an adopted NodeClaim must not be treated as an in-flight add")
	}
}
