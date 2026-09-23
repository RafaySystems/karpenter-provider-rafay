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

package batchresume

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	cprovider "github.com/RafaySystems/karpenter-provider-rafay/pkg/cloudprovider"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

func claim(name, uid, providerID, batchID string, deleting bool) *karpv1.NodeClaim {
	nc := &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
		Status:     karpv1.NodeClaimStatus{ProviderID: providerID},
	}
	if batchID != "" {
		nc.Annotations = map[string]string{cprovider.BatchIDAnnotationKey: batchID}
	}
	if deleting {
		now := metav1.NewTime(time.Now())
		nc.DeletionTimestamp = &now
		nc.Finalizers = []string{"test/finalizer"} // the fake client rejects a deleting object with none
	}
	return nc
}

func newReader(objs ...client.Object) client.Reader {
	// karpv1's package init registers NodeClaim into the default client-go scheme.
	return fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(objs...).Build()
}

// TestStartResumesPendingAnnotatedBatches is the whole contract: pending NodeClaims are grouped by
// their batch annotation and each batch is handed to the batcher once; everything else — already
// bound, deleting, or from before the annotation existed — is left alone.
func TestStartResumesPendingAnnotatedBatches(t *testing.T) {
	pending := cprovider.PendingProviderIDPrefix
	reader := newReader(
		claim("a", "uid-a", pending+"uid-a", "batch-1", false),
		claim("b", "uid-b", pending+"uid-b", "batch-1", false), // same batch as a
		claim("c", "uid-c", pending+"uid-c", "batch-2", false),
		claim("bound", "uid-d", "rafay://pool1/oci-inst/host-w3-abc", "batch-3", false), // node already joined
		claim("deleting", "uid-e", pending+"uid-e", "batch-4", true),                    // being removed
		claim("legacy", "uid-f", pending+"uid-f", "", false),                            // pre-annotation provider
	)
	batcher := rafay.NewNodeBatcher(nil)

	if err := NewController(reader, batcher).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for _, id := range []string{"batch-1", "batch-2"} {
		if !batcher.Tracking(id) {
			t.Errorf("%s should be resumed", id)
		}
	}
	for _, id := range []string{"batch-3", "batch-4"} {
		if batcher.Tracking(id) {
			t.Errorf("%s should not be resumed (bound or deleting NodeClaim)", id)
		}
	}

	// Ops of a resumed batch are in flight: a retry must not re-send them and must know its batch.
	if r := <-batcher.Enqueue("uid-a", rafay.AddNodesRequest{OperationID: "uid-a"}); r.BatchID != "batch-1" {
		t.Errorf("retry of a resumed op learned BatchID %q, want batch-1", r.BatchID)
	}

	// A second pass is idempotent.
	if err := NewController(reader, batcher).Start(context.Background()); err != nil {
		t.Fatalf("second Start: %v", err)
	}
}

// TestStartWithNothingPendingIsANoop keeps the common case cheap and quiet.
func TestStartWithNothingPendingIsANoop(t *testing.T) {
	batcher := rafay.NewNodeBatcher(nil)
	if err := NewController(newReader(), batcher).Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}
