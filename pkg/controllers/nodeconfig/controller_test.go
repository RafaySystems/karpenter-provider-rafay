/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package nodeconfig

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	edgev1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
)

// Manifests in the shape edge-broker renders them (see its karpenter_config_render.go).
const (
	nodeClassYAML = `apiVersion: karpenter.rafay.io/v1alpha1
kind: RafayNodeClass
metadata:
  name: oci-inst
spec:
  instanceTypes:
    - name: oci-inst
      cpu: "4"
      memory: 12Gi
      architectures:
        - amd64
      operatingSystems:
        - linux
`

	nodePoolYAML = `apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: pool1
spec:
  weight: 100
  template:
    metadata:
      labels:
        nodepool: pool1
    spec:
      requirements:
        - key: kubernetes.io/os
          operator: In
          values:
            - linux
      nodeClassRef:
        group: karpenter.rafay.io
        kind: RafayNodeClass
        name: oci-inst
  limits:
    cpu: "1000"
    memory: 1000Gi
  disruption:
    consolidationPolicy: WhenEmpty
    consolidateAfter: 5m
`
)

// stubFetcher stands in for the broker client.
type stubFetcher struct {
	resp  *edgev1.KarpenterConfigResponse
	err   error
	calls int
	// gotClusterID / gotProjectID record what the controller forwarded.
	gotClusterID string
	gotProjectID string
}

func (s *stubFetcher) GetKarpenterConfig(_ context.Context, clusterID, projectID string) (*edgev1.KarpenterConfigResponse, error) {
	s.calls++
	s.gotClusterID = clusterID
	s.gotProjectID = projectID
	return s.resp, s.err
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add v1alpha1 scheme: %v", err)
	}
	// karpenter's apis/v1 package registers itself into the global client-go scheme in init()
	// rather than exposing a SchemeBuilder, so the types are added explicitly here.
	gv := schema.GroupVersion{Group: "karpenter.sh", Version: "v1"}
	metav1.AddToGroupVersion(s, gv)
	s.AddKnownTypes(gv, &karpv1.NodePool{}, &karpv1.NodePoolList{})
	return s
}

func newTestController(t *testing.T, fetcher ConfigFetcher) (*Controller, client.Client) {
	t.Helper()
	cl := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	c := NewController(fetcher, "cluster-1", "project-1", time.Minute)
	c.kubeClient = cl
	return c, cl
}

// patchRecorder captures the objects the controller sends without letting them reach a store.
//
// It exists because the fake client cannot round-trip a NodePool that carries
// spec.disruption.consolidateAfter more than once: its tracker converts the stored typed object
// back to unstructured, and karpenter's NillableDuration.ToUnstructured returns its raw JSON
// bytes, which re-encode as base64 ("IjVtIg==") and then fail to parse. That is a fake-client
// artifact — a real API server stores the JSON we PATCH and never converts through the Go type
// — but it means any test that applies the same NodePool twice has to assert on the request
// rather than on the stored object.
type patchRecorder struct {
	client.Client
	patches []recordedPatch
}

type recordedPatch struct {
	obj        *unstructured.Unstructured
	patchType  string
	force      bool
	fieldOwner string
}

func newPatchRecorder(t *testing.T) *patchRecorder {
	t.Helper()
	rec := &patchRecorder{}
	rec.Client = interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(),
		interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				u, ok := obj.(*unstructured.Unstructured)
				if !ok {
					t.Fatalf("expected an unstructured object, got %T", obj)
				}
				p := recordedPatch{obj: u.DeepCopy(), patchType: string(patch.Type())}
				var applyOpts client.PatchOptions
				applyOpts.ApplyOptions(opts)
				p.force = applyOpts.Force != nil && *applyOpts.Force
				p.fieldOwner = applyOpts.FieldManager
				rec.patches = append(rec.patches, p)
				return nil
			},
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return nil // orphan reporting is covered by TestSyncDoesNotPruneOrphans
			},
		})
	return rec
}

func (r *patchRecorder) kinds() []string {
	out := make([]string, 0, len(r.patches))
	for _, p := range r.patches {
		out = append(out, p.obj.GetKind()+"/"+p.obj.GetName())
	}
	return out
}

func newRecordingController(t *testing.T, fetcher ConfigFetcher) (*Controller, *patchRecorder) {
	t.Helper()
	rec := newPatchRecorder(t)
	c := NewController(fetcher, "cluster-1", "project-1", time.Minute)
	c.kubeClient = rec
	return c, rec
}

func fullConfig() *edgev1.KarpenterConfigResponse {
	return &edgev1.KarpenterConfigResponse{
		ClusterName:   "test-auto-2",
		NodeClassYaml: nodeClassYAML,
		NodePoolYaml:  nodePoolYAML,
		Revision:      "rev1",
		AutoScaling:   true,
	}
}

func getUnstructured(t *testing.T, cl client.Client, apiVersion, kind, name string) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	if err := cl.Get(context.Background(), client.ObjectKey{Name: name}, obj); err != nil {
		t.Fatalf("get %s %q: %v", kind, name, err)
	}
	return obj
}

func TestSyncAppliesConfig(t *testing.T) {
	fetcher := &stubFetcher{resp: fullConfig()}
	c, cl := newTestController(t, fetcher)

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	if fetcher.gotClusterID != "cluster-1" || fetcher.gotProjectID != "project-1" {
		t.Errorf("controller identity not forwarded to the broker: cluster=%q project=%q",
			fetcher.gotClusterID, fetcher.gotProjectID)
	}

	nc := getUnstructured(t, cl, "karpenter.rafay.io/v1alpha1", "RafayNodeClass", "oci-inst")
	if got := nc.GetLabels()[managedByLabel]; got != managedByValue {
		t.Errorf("node class managed-by label: want %q, got %q", managedByValue, got)
	}
	if got := nc.GetAnnotations()[revisionAnnotation]; got != "rev1" {
		t.Errorf("node class revision annotation: want rev1, got %q", got)
	}
	// The SKU capacity has to survive the round trip — it is what Karpenter schedules against.
	its, found, err := unstructured.NestedSlice(nc.Object, "spec", "instanceTypes")
	if err != nil || !found || len(its) != 1 {
		t.Fatalf("spec.instanceTypes: found=%v err=%v value=%v", found, err, its)
	}
	it := its[0].(map[string]interface{})
	if it["name"] != "oci-inst" || it["cpu"] != "4" || it["memory"] != "12Gi" {
		t.Errorf("instance type not applied faithfully: %+v", it)
	}

	np := getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool1")
	if got := np.GetLabels()[managedByLabel]; got != managedByValue {
		t.Errorf("node pool managed-by label: want %q, got %q", managedByValue, got)
	}
	refName, found, err := unstructured.NestedString(np.Object, "spec", "template", "spec", "nodeClassRef", "name")
	if err != nil || !found || refName != "oci-inst" {
		t.Errorf("nodeClassRef.name: found=%v err=%v value=%q", found, err, refName)
	}

	if c.lastRevision != "rev1" {
		t.Errorf("lastRevision: want rev1, got %q", c.lastRevision)
	}
}

// Objects must be sent as a forced server-side apply: anything else either fails on a field
// owned by a previous `kubectl apply` or silently clobbers fields the broker does not set.
func TestSyncUsesForcedServerSideApply(t *testing.T) {
	c, rec := newRecordingController(t, &stubFetcher{resp: fullConfig()})
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// Node classes must go out before the pools that reference them, or the pools sit
	// NotReady until the class shows up.
	if got := rec.kinds(); len(got) != 2 || got[0] != "RafayNodeClass/oci-inst" || got[1] != "NodePool/pool1" {
		t.Fatalf("want the node class applied before the pool, got %v", got)
	}
	for _, p := range rec.patches {
		if p.patchType != "application/apply-patch+yaml" {
			t.Errorf("%s: want a server-side apply patch, got %q", p.obj.GetKind(), p.patchType)
		}
		if !p.force {
			t.Errorf("%s: apply must force ownership, otherwise a conflict wedges every resync", p.obj.GetKind())
		}
		if p.fieldOwner != string(fieldOwner) {
			t.Errorf("%s: field owner: want %q, got %q", p.obj.GetKind(), fieldOwner, p.fieldOwner)
		}
	}
}

// A resync that returns the same revision must not re-apply: that is the whole point of the
// broker sending one.
func TestSyncSkipsUnchangedRevision(t *testing.T) {
	fetcher := &stubFetcher{resp: fullConfig()}
	c, rec := newRecordingController(t, fetcher)

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(rec.patches) != 2 {
		t.Errorf("unchanged revision should not re-apply: %v", rec.kinds())
	}
	if fetcher.calls != 2 {
		t.Errorf("both syncs should still fetch: calls=%d", fetcher.calls)
	}
}

// A changed revision must be re-applied even though the objects already exist — this is how a
// pool resized or a SKU respecced in the catalog reaches the cluster.
func TestSyncAppliesChangedRevision(t *testing.T) {
	fetcher := &stubFetcher{resp: fullConfig()}
	c, rec := newRecordingController(t, fetcher)
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	updated := fullConfig()
	updated.Revision = "rev2"
	updated.NodePoolYaml = strings.Replace(nodePoolYAML, `cpu: "1000"`, `cpu: "500"`, 1)
	fetcher.resp = updated

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if len(rec.patches) != 4 {
		t.Fatalf("changed revision should re-apply every object, got %v", rec.kinds())
	}
	np := rec.patches[3].obj
	if np.GetKind() != "NodePool" {
		t.Fatalf("unexpected last patch: %s", np.GetKind())
	}
	cpu, found, err := unstructured.NestedString(np.Object, "spec", "limits", "cpu")
	if err != nil || !found || cpu != "500" {
		t.Errorf("changed limit not applied: found=%v err=%v value=%q", found, err, cpu)
	}
	if got := np.GetAnnotations()[revisionAnnotation]; got != "rev2" {
		t.Errorf("revision annotation not updated: %q", got)
	}
	if c.lastRevision != "rev2" {
		t.Errorf("lastRevision: want rev2, got %q", c.lastRevision)
	}
}

// A NotFound from the broker means "this cluster has no Karpenter config to serve" — no
// workspace compute instance behind the edge, or no worker-pool catalog on it. That is a
// property of the cluster, not a transient fault: sync must treat it as a successful no-op so
// the loop waits the full sync interval instead of error-backoff-spamming the same answer.
func TestSyncTreatsConfigUnavailableAsNoOp(t *testing.T) {
	fetcher := &stubFetcher{err: status.Errorf(codes.NotFound,
		"karpenter config: karpenter config unavailable for this edge: no workspace compute instance named %q", "k8s-autoscale-01")}
	c, cl := newTestController(t, fetcher)

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("a config-unavailable fetch must not be a sync error, got: %v", err)
	}
	var pools unstructured.UnstructuredList
	pools.SetAPIVersion("karpenter.sh/v1")
	pools.SetKind("NodePoolList")
	if err := cl.List(context.Background(), &pools); err != nil {
		t.Fatalf("list node pools: %v", err)
	}
	if len(pools.Items) != 0 {
		t.Errorf("nothing must be applied, got %d NodePools", len(pools.Items))
	}
	if c.lastRevision != "" {
		t.Errorf("a no-op sync must not record a revision, got %q", c.lastRevision)
	}

	// Any other failure stays an error so the retry backoff still applies.
	fetcher.err = status.Errorf(codes.Internal, "karpenter config: GetWorkspaceComputeInstance: db down")
	if err := c.sync(context.Background()); err == nil {
		t.Fatal("a non-NotFound fetch failure must still be a sync error")
	}
}

// Applying NodePools to a cluster whose owner turned autoscaling off would start scaling it.
func TestSyncSkipsWhenAutoScalingDisabled(t *testing.T) {
	resp := fullConfig()
	resp.AutoScaling = false
	c, cl := newTestController(t, &stubFetcher{resp: resp})

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	var pools unstructured.UnstructuredList
	pools.SetAPIVersion("karpenter.sh/v1")
	pools.SetKind("NodePoolList")
	if err := cl.List(context.Background(), &pools); err != nil {
		t.Fatalf("list node pools: %v", err)
	}
	if len(pools.Items) != 0 {
		t.Errorf("autoscaling disabled: want no NodePools applied, got %d", len(pools.Items))
	}
	if c.lastRevision != "" {
		t.Errorf("a skipped sync must not record a revision, got %q", c.lastRevision)
	}
}

// An empty response leaves the cluster alone rather than being treated as "delete everything".
func TestSyncIgnoresEmptyConfig(t *testing.T) {
	c, cl := newTestController(t, &stubFetcher{resp: &edgev1.KarpenterConfigResponse{
		ClusterName: "empty", AutoScaling: true, Revision: "rev0",
	}})
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	var pools unstructured.UnstructuredList
	pools.SetAPIVersion("karpenter.sh/v1")
	pools.SetKind("NodePoolList")
	if err := cl.List(context.Background(), &pools); err != nil {
		t.Fatalf("list node pools: %v", err)
	}
	if len(pools.Items) != 0 {
		t.Errorf("want nothing applied, got %d NodePools", len(pools.Items))
	}
	if c.lastRevision != "" {
		t.Errorf("an empty config must not be recorded as applied, got %q", c.lastRevision)
	}
}

// A pool that vanishes from the broker's config is reported but never deleted: the broker also
// drops pools when a SKU lookup fails, and deleting a NodePool drains its nodes.
func TestSyncDoesNotPruneOrphans(t *testing.T) {
	fetcher := &stubFetcher{resp: fullConfig()}
	c, cl := newTestController(t, fetcher)
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	shrunk := fullConfig()
	shrunk.Revision = "rev2"
	shrunk.NodePoolYaml = strings.Replace(nodePoolYAML, "name: pool1", "name: pool2", 1)
	fetcher.resp = shrunk

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	// pool1 disappeared from the config but must still exist.
	getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool1")
	getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool2")
}

func TestSyncFetchError(t *testing.T) {
	c, _ := newTestController(t, &stubFetcher{err: fmt.Errorf("broker unreachable")})
	err := c.sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), "broker unreachable") {
		t.Errorf("want the broker error surfaced, got %v", err)
	}
	if c.lastRevision != "" {
		t.Errorf("a failed sync must not record a revision, got %q", c.lastRevision)
	}
}

func TestDecodeManifests(t *testing.T) {
	t.Run("multi document", func(t *testing.T) {
		objs, err := decodeManifests(nodeClassYAML + "---\n" + nodePoolYAML)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(objs) != 2 {
			t.Fatalf("want 2 objects, got %d", len(objs))
		}
		if objs[0].GetKind() != "RafayNodeClass" || objs[1].GetKind() != "NodePool" {
			t.Errorf("document order not preserved: %s, %s", objs[0].GetKind(), objs[1].GetKind())
		}
	})

	t.Run("empty input", func(t *testing.T) {
		objs, err := decodeManifests("   \n")
		if err != nil || objs != nil {
			t.Errorf("want (nil, nil), got (%v, %v)", objs, err)
		}
	})

	t.Run("trailing separator", func(t *testing.T) {
		objs, err := decodeManifests(nodePoolYAML + "---\n")
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(objs) != 1 {
			t.Errorf("empty trailing document should be skipped, got %d objects", len(objs))
		}
	})

	// A document the API server would reject is caught before any apply, so a bad manifest
	// cannot half-apply a config.
	t.Run("rejects incomplete documents", func(t *testing.T) {
		if _, err := decodeManifests("foo: bar\n"); err == nil {
			t.Error("document without apiVersion/kind should error")
		}
		if _, err := decodeManifests("apiVersion: karpenter.sh/v1\nkind: NodePool\nspec: {}\n"); err == nil {
			t.Error("document without metadata.name should error")
		}
	})
}

func TestSyncIntervalFromEnv(t *testing.T) {
	if got := SyncIntervalFromEnv(); got != defaultSyncInterval {
		t.Errorf("unset: want %s, got %s", defaultSyncInterval, got)
	}
	t.Setenv("KARPENTER_CONFIG_SYNC_INTERVAL", "45s")
	if got := SyncIntervalFromEnv(); got != 45*time.Second {
		t.Errorf("want 45s, got %s", got)
	}
	t.Setenv("KARPENTER_CONFIG_SYNC_INTERVAL", "nonsense")
	if got := SyncIntervalFromEnv(); got != defaultSyncInterval {
		t.Errorf("invalid value should fall back to the default, got %s", got)
	}
	t.Setenv("KARPENTER_CONFIG_SYNC_INTERVAL", "0s")
	if got := SyncIntervalFromEnv(); got != defaultSyncInterval {
		t.Errorf("non-positive value should fall back to the default, got %s", got)
	}
}

// The value reaching this env var comes from a salt pillar rendered through jinja
// (infra-states/edge/file/autoscaling/deployment.yaml). A pillar written as a YAML boolean
// renders capitalised — "False", not "false" — so the capitalised spellings have to be honoured
// or an operator who disabled bootstrap would silently still get it.
func TestEnabledFromEnv(t *testing.T) {
	if !EnabledFromEnv() {
		t.Error("bootstrap should default to enabled when unset")
	}
	for _, tc := range []struct {
		val  string
		want bool
	}{
		{"false", false},
		{"False", false}, // salt pillar written as a YAML bool
		{"FALSE", false},
		{"0", false},
		{"true", true},
		{"True", true},
		{"1", true},
		// Anything unparseable fails safe to enabled: a typo'd pillar must not silently
		// disable the only thing that puts NodePools on the cluster.
		{"maybe", true},
		{"yes", true},
		{"", true},
	} {
		t.Setenv("KARPENTER_CONFIG_BOOTSTRAP", tc.val)
		if got := EnabledFromEnv(); got != tc.want {
			t.Errorf("KARPENTER_CONFIG_BOOTSTRAP=%q: want %v, got %v", tc.val, tc.want, got)
		}
	}
}

// NewController must not accept a zero/negative interval, which would make Start spin.
func TestNewControllerIntervalFloor(t *testing.T) {
	if got := NewController(&stubFetcher{}, "c", "p", 0).syncInterval; got != defaultSyncInterval {
		t.Errorf("zero interval: want %s, got %s", defaultSyncInterval, got)
	}
	if got := NewController(&stubFetcher{}, "c", "p", -time.Second).syncInterval; got != defaultSyncInterval {
		t.Errorf("negative interval: want %s, got %s", defaultSyncInterval, got)
	}
}
