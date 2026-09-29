/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package cloudprovider

// cp_fakes_test.go — fakes and fixtures shared by the cp_*_test.go files.
//
// cpFakeBatcher stands in for *rafay.NodeBatcher through the unexported nodeBatcher seam so
// Create()/Delete() can be driven end to end without a broker: every enqueue is recorded and
// resolved synchronously with a scripted rafay.BatchResult (or never, when hold is set, to
// exercise the ctx.Done() branch). Cancel is recorded synchronously instead of spawning a
// goroutine, so tests can assert on it without waiting.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// cpBase is the reference instant every fixture's CreationTimestamp is derived from.
var cpBase = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

const (
	cpClusterID = "cluster-1"
	cpProjectID = "project-1"
)

// ──────────────────────────── fake batcher ────────────────────────────

type cpEnqueuedAdd struct {
	operationID string
	req         rafay.AddNodesRequest
}

type cpEnqueuedRemove struct {
	operationID string
	req         rafay.RemoveNodesRequest
}

type cpFakeBatcher struct {
	mu        sync.Mutex
	succeeded map[string]bool
	// result is delivered on every Enqueue/EnqueueRemove channel unless hold is set, in which case
	// the channel is never written (the caller must give up via its context).
	result rafay.BatchResult
	hold   bool

	adds    []cpEnqueuedAdd
	removes []cpEnqueuedRemove
	cancels []string
}

var _ nodeBatcher = (*cpFakeBatcher)(nil)

func cpNewFakeBatcher() *cpFakeBatcher {
	return &cpFakeBatcher{succeeded: map[string]bool{}}
}

func (b *cpFakeBatcher) Succeeded(operationID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.succeeded[operationID]
}

func (b *cpFakeBatcher) markSucceeded(operationID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.succeeded[operationID] = true
}

func (b *cpFakeBatcher) resolve() <-chan rafay.BatchResult {
	ch := make(chan rafay.BatchResult, 1)
	if !b.hold {
		ch <- b.result
	}
	return ch
}

func (b *cpFakeBatcher) Enqueue(operationID string, req rafay.AddNodesRequest) <-chan rafay.BatchResult {
	b.mu.Lock()
	b.adds = append(b.adds, cpEnqueuedAdd{operationID: operationID, req: req})
	b.mu.Unlock()
	return b.resolve()
}

func (b *cpFakeBatcher) EnqueueRemove(operationID string, req rafay.RemoveNodesRequest) <-chan rafay.BatchResult {
	b.mu.Lock()
	b.removes = append(b.removes, cpEnqueuedRemove{operationID: operationID, req: req})
	b.mu.Unlock()
	return b.resolve()
}

func (b *cpFakeBatcher) Cancel(_ context.Context, operationID string) (bool, error) {
	b.mu.Lock()
	b.cancels = append(b.cancels, operationID)
	b.mu.Unlock()
	return true, nil
}

func (b *cpFakeBatcher) SucceededResult(operationID string) ([]string, string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.succeeded[operationID] {
		return nil, "", false
	}
	return nil, "", true
}

func (b *cpFakeBatcher) removeCalls() []cpEnqueuedRemove {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]cpEnqueuedRemove(nil), b.removes...)
}

func (b *cpFakeBatcher) addCalls() []cpEnqueuedAdd {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]cpEnqueuedAdd(nil), b.adds...)
}

func (b *cpFakeBatcher) cancelCalls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.cancels...)
}

// ──────────────────────────── fake rafay.Client ────────────────────────────

type cpFakeRafayClient struct {
	getNodeFn   func(providerID string) (*rafay.NodeInfo, error)
	listNodesFn func(clusterID string) ([]*rafay.NodeInfo, error)
	getCalls    []string
	listCalls   []string
}

var _ rafay.Client = (*cpFakeRafayClient)(nil)

func (c *cpFakeRafayClient) GetNode(_ context.Context, providerID string) (*rafay.NodeInfo, error) {
	c.getCalls = append(c.getCalls, providerID)
	if c.getNodeFn != nil {
		return c.getNodeFn(providerID)
	}
	return nil, rafay.ErrGetNodeUnsupported
}

func (c *cpFakeRafayClient) ListNodes(_ context.Context, clusterID string) ([]*rafay.NodeInfo, error) {
	c.listCalls = append(c.listCalls, clusterID)
	if c.listNodesFn != nil {
		return c.listNodesFn(clusterID)
	}
	return nil, rafay.ErrListNodesUnsupported
}

// ──────────────────────────── reader / client wrappers ────────────────────────────

// cpReader wraps a client.Reader, counting List calls and optionally failing them. listErr is
// consulted with the list object so a test can fail only the NodeClaim LIST or only the Node LIST.
type cpReader struct {
	client.Reader
	mu      sync.Mutex
	lists   int
	listErr func(list client.ObjectList) error
}

func (r *cpReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.mu.Lock()
	r.lists++
	fn := r.listErr
	r.mu.Unlock()
	if fn != nil {
		if err := fn(list); err != nil {
			return err
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

func (r *cpReader) listCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lists
}

// cpFailAllLists is a cpReader.listErr that fails every LIST.
func cpFailAllLists(err error) func(client.ObjectList) error {
	return func(client.ObjectList) error { return err }
}

// cpClient wraps a client.Client and lets a test fail Get, List or Delete.
type cpClient struct {
	client.Client
	getErr    error
	listErr   error
	deleteErr error
}

func (c *cpClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if c.getErr != nil {
		return c.getErr
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *cpClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if c.listErr != nil {
		return c.listErr
	}
	return c.Client.List(ctx, list, opts...)
}

func (c *cpClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if c.deleteErr != nil {
		return c.deleteErr
	}
	return c.Client.Delete(ctx, obj, opts...)
}

// ──────────────────────────── fixtures ────────────────────────────

// cpFakeClient builds a controller-runtime fake client over the core + Karpenter + v1alpha1 scheme,
// with the operator's spec.providerID Node index (nodeProviderIDIndex, which Get and Delete list
// on) and NodeClaim status as a subresource (Delete reserves a resolved providerID through
// Status().Patch, exactly like NodeProviderIDController).
func cpFakeClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newFailureHandlerScheme(t)).
		WithIndex(&corev1.Node{}, nodeProviderIDIndex, func(o client.Object) []string {
			return []string{o.(*corev1.Node).Spec.ProviderID}
		}).
		WithStatusSubresource(&karpv1.NodeClaim{}).
		WithObjects(objs...).
		Build()
}

// cpNode returns a platform-shaped worker node in pool/sku carrying providerID, created at the
// given instant.
func cpNode(name, pool, sku, providerID string, created time.Time) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Labels:            map[string]string{nodepoolNameLabel: pool, skuNameLabel: sku},
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: corev1.NodeSpec{ProviderID: providerID},
	}
}

// cpClaim returns a NodeClaim in pool whose selected instance type (and NodeClass name) is sku,
// with the given status.providerID, created at cpBase.
func cpClaim(name, uid, pool, sku, providerID string) *karpv1.NodeClaim {
	return &karpv1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			UID:               types.UID(uid),
			CreationTimestamp: metav1.NewTime(cpBase),
			Labels: map[string]string{
				karpv1.NodePoolLabelKey:        pool,
				corev1.LabelInstanceTypeStable: sku,
			},
		},
		Spec: karpv1.NodeClaimSpec{
			NodeClassRef: &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "RafayNodeClass", Name: sku},
		},
		Status: karpv1.NodeClaimStatus{ProviderID: providerID},
	}
}

// cpPendingClaim is cpClaim with the synthetic pending ProviderID Create() stamps.
func cpPendingClaim(name, uid, pool, sku string) *karpv1.NodeClaim {
	return cpClaim(name, uid, pool, sku, PendingProviderIDPrefix+uid)
}

// cpNodeClass returns a RafayNodeClass named after its single SKU.
func cpNodeClass(name string, specs ...v1alpha1.InstanceTypeSpec) *v1alpha1.RafayNodeClass {
	nc := &v1alpha1.RafayNodeClass{}
	nc.Name = name
	nc.Spec.InstanceTypes = specs
	return nc
}

// cpProvider assembles a CloudProvider over cl (used for both cached and uncached reads unless
// reader is non-nil), the fake rafay client rc (may be nil) and the fake batcher b (may be nil,
// in which case any batcher call panics — exactly like production with no batcher).
func cpProvider(cl client.Client, reader client.Reader, rc rafay.Client, b *cpFakeBatcher) *CloudProvider {
	if reader == nil {
		reader = cl
	}
	cp := &CloudProvider{
		kubeClient: cl,
		apiReader:  reader,
		client:     rc,
		clusterID:  cpClusterID,
		projectID:  cpProjectID,
	}
	if b != nil {
		cp.batcher = b
	}
	return cp
}

var cpErrBoom = errors.New("etcdserver: request timed out")
