/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package nodeconfig

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	edgev1 "github.com/RafaySystems/edge-common/pkg/edge/v1"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
)

// msBrokerNodeClassYAML and msBrokerNodePoolYAML are the verbatim output of edge-broker's
// RenderKarpenterConfig (pkg/context/karpenter_config_render.go, yaml.v3-emitted) for a GPU
// pool carrying catalog labels, annotations and a taint. They are the real cross-repo contract:
// the broker renders with yaml.v3 struct tags, the provider decodes with apimachinery's
// YAML-or-JSON decoder and then hands the result to the typed CRD schema.
const (
	msBrokerNodeClassYAML = `apiVersion: karpenter.rafay.io/v1alpha1
kind: RafayNodeClass
metadata:
  name: gpu-h100
spec:
  instanceTypes:
    - name: gpu-h100
      cpu: "64"
      memory: 1024Gi
      nvidia.com/gpu: "8"
      architectures:
        - amd64
      operatingSystems:
        - linux
`

	msBrokerNodePoolYAML = `apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: ml
  annotations:
    karpenter.rafay.io/auto-scaling: "true"
spec:
  weight: 100
  template:
    metadata:
      labels:
        nodepool: ml
        tier: gpu
      annotations:
        owner: ml-team
    spec:
      requirements:
        - key: kubernetes.io/os
          operator: In
          values:
            - linux
        - key: kubernetes.io/arch
          operator: In
          values:
            - amd64
        - key: karpenter.sh/capacity-type
          operator: In
          values:
            - on-demand
        - key: node.kubernetes.io/instance-type
          operator: In
          values:
            - gpu-h100
      taints:
        - key: nvidia.com/gpu
          value: "true"
          effect: NoSchedule
      nodeClassRef:
        group: karpenter.rafay.io
        kind: RafayNodeClass
        name: gpu-h100
  limits:
    cpu: "1000"
    memory: 1000Gi
    nvidia.com/gpu: "16"
  disruption:
    consolidationPolicy: WhenEmpty
    consolidateAfter: 5m
`

	// msPoolNoDisruptionYAML is pool1 without the disruption block, for tests that have to
	// update the stored NodePool through the fake client (see patchRecorder for why a
	// consolidateAfter cannot survive a second round trip there).
	msPoolNoDisruptionYAML = `apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: pool1
spec:
  weight: 100
  template:
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
`
)

func msBrokerConfig() *edgev1.KarpenterConfigResponse {
	return &edgev1.KarpenterConfigResponse{
		ClusterName:   "gpu-cluster",
		NodeClassYaml: msBrokerNodeClassYAML,
		NodePoolYaml:  msBrokerNodePoolYAML,
		Revision:      "6528d0658ac6f786",
		AutoScaling:   true,
	}
}

// msCaptureKlog routes klog output into a buffer for the rest of the test.
func msCaptureKlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&buf)
	t.Cleanup(func() {
		klog.Flush()
		klog.SetOutput(io.Discard)
		klog.LogToStderr(true)
	})
	return &buf
}

// msFlakyPatchClient is a store-less client whose Patch fails for one kind while failPatchKind
// is set, so a partial apply can be simulated.
type msFlakyPatchClient struct {
	client.Client
	failPatchKind string
	kinds         []string
}

func msNewFlakyPatchClient(t *testing.T) *msFlakyPatchClient {
	t.Helper()
	fc := &msFlakyPatchClient{}
	fc.Client = interceptor.NewClient(
		fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(),
		interceptor.Funcs{
			Patch: func(_ context.Context, _ client.WithWatch, obj client.Object, _ client.Patch, _ ...client.PatchOption) error {
				kind := obj.GetObjectKind().GroupVersionKind().Kind
				if kind == fc.failPatchKind {
					return errors.New("injected apply failure")
				}
				fc.kinds = append(fc.kinds, kind+"/"+obj.GetName())
				return nil
			},
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return nil
			},
		})
	return fc
}

// msCancellingFetcher cancels the controller's context from inside the broker call, which is
// the only way to drive Start through one iteration without waiting on its real timers.
type msCancellingFetcher struct {
	cancel context.CancelFunc
	resp   *edgev1.KarpenterConfigResponse
	err    error
	calls  int
}

func (f *msCancellingFetcher) GetKarpenterConfig(context.Context, string, string) (*edgev1.KarpenterConfigResponse, error) {
	f.calls++
	f.cancel()
	return f.resp, f.err
}

func msCreateUnstructured(t *testing.T, cl client.Client, apiVersion, kind, name string, managed bool) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetName(name)
	if managed {
		obj.SetLabels(map[string]string{managedByLabel: managedByValue})
	}
	if err := cl.Create(context.Background(), obj); err != nil {
		t.Fatalf("create %s %q: %v", kind, name, err)
	}
}

// Regression test for R1-prov-misc-startup-2: a managed NodePool deleted by hand must come
// back on the next resync even though the broker's revision has not changed. The revision
// only fingerprints the broker's YAML, so sync must not skip the apply on it — before the fix
// it returned before looking at the cluster and the pool stayed gone until the catalog changed
// or the pod restarted.
func TestMsSyncReappliesDeletedObjectDespiteUnchangedRevision(t *testing.T) {
	fetcher := &stubFetcher{resp: fullConfig()}
	c, cl := newTestController(t, fetcher)
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	pool := getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool1")
	if err := cl.Delete(context.Background(), pool); err != nil {
		t.Fatalf("delete pool1: %v", err)
	}

	// Same revision as before: the broker's fingerprint only covers its own rendered YAML.
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	if fetcher.calls != 2 {
		t.Fatalf("both syncs must fetch, calls=%d", fetcher.calls)
	}
	restored := &unstructured.Unstructured{}
	restored.SetAPIVersion("karpenter.sh/v1")
	restored.SetKind("NodePool")
	if err := cl.Get(context.Background(), client.ObjectKey{Name: "pool1"}, restored); err != nil {
		t.Fatalf("pool1 was deleted by hand and the resync did not re-apply it: %v", err)
	}
	if got := restored.GetAnnotations()[revisionAnnotation]; got != "rev1" {
		t.Errorf("re-applied pool1 revision annotation: want rev1, got %q", got)
	}
}

// Regression test for R1-prov-misc-startup-2, hand-edit variant: a field an operator changed on
// a managed NodePool must be overwritten by the next resync (deployment.yaml promises "the
// resync overwrites whatever that owner writes"), regardless of the broker revision.
func TestMsSyncRestoresHandEditedObjectDespiteUnchangedRevision(t *testing.T) {
	resp := fullConfig()
	resp.NodePoolYaml = msPoolNoDisruptionYAML
	c, cl := newTestController(t, &stubFetcher{resp: resp})
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("first sync: %v", err)
	}

	pool := getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool1")
	if err := unstructured.SetNestedField(pool.Object, "1", "spec", "limits", "cpu"); err != nil {
		t.Fatalf("set limits.cpu: %v", err)
	}
	if err := cl.Update(context.Background(), pool); err != nil {
		t.Fatalf("hand-edit pool1: %v", err)
	}

	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	after := getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool1")
	cpu, _, _ := unstructured.NestedString(after.Object, "spec", "limits", "cpu")
	// The fake client stores the typed NodePool, so the Quantity comes back canonicalised
	// ("1000" -> "1k"); compare values, not spellings.
	if got, err := resource.ParseQuantity(cpu); err != nil || got.Cmp(resource.MustParse("1000")) != 0 {
		t.Errorf("hand-edited limits.cpu must be overwritten by the resync: want 1000, got %q (%v)", cpu, err)
	}
}

// With no revision from the broker the skip logic is disabled: every sync re-applies, nothing
// is remembered, and no revision annotation is stamped.
func TestMsSyncEmptyRevisionAppliesEveryInterval(t *testing.T) {
	resp := fullConfig()
	resp.Revision = "  "
	c, rec := newRecordingController(t, &stubFetcher{resp: resp})

	for i := 0; i < 2; i++ {
		if err := c.sync(context.Background()); err != nil {
			t.Fatalf("sync #%d: %v", i+1, err)
		}
	}
	if len(rec.patches) != 4 {
		t.Fatalf("an empty revision must re-apply on every sync, got %v", rec.kinds())
	}
	if c.lastRevision != "" {
		t.Errorf("lastRevision must stay empty, got %q", c.lastRevision)
	}
	for _, p := range rec.patches {
		if _, ok := p.obj.GetAnnotations()[revisionAnnotation]; ok {
			t.Errorf("%s: no revision annotation may be stamped when the broker sent none: %v",
				p.obj.GetKind(), p.obj.GetAnnotations())
		}
		if p.obj.GetLabels()[managedByLabel] != managedByValue {
			t.Errorf("%s: managed-by label must be stamped regardless of revision", p.obj.GetKind())
		}
	}
}

// When the second apply fails after the first succeeded, the revision must not be recorded, so
// the next sync re-applies both objects instead of skipping the half-applied config forever.
func TestMsSyncPartialApplyFailureKeepsRevisionUnset(t *testing.T) {
	fc := msNewFlakyPatchClient(t)
	fc.failPatchKind = "NodePool"
	c := NewController(&stubFetcher{resp: fullConfig()}, "cluster-1", "project-1", time.Minute)
	c.kubeClient = fc

	err := c.sync(context.Background())
	if err == nil || !strings.Contains(err.Error(), `apply NodePool "pool1"`) || !strings.Contains(err.Error(), "injected apply failure") {
		t.Fatalf("want the NodePool apply failure surfaced with kind and name, got %v", err)
	}
	if c.lastRevision != "" {
		t.Fatalf("a partial apply must not record the revision, got %q", c.lastRevision)
	}
	if len(fc.kinds) != 1 || fc.kinds[0] != "RafayNodeClass/oci-inst" {
		t.Fatalf("only the node class should have been applied before the failure, got %v", fc.kinds)
	}

	fc.failPatchKind = ""
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync after the apply recovered: %v", err)
	}
	if want := []string{"RafayNodeClass/oci-inst", "RafayNodeClass/oci-inst", "NodePool/pool1"}; strings.Join(fc.kinds, ",") != strings.Join(want, ",") {
		t.Errorf("the retry must re-apply both objects: want %v, got %v", want, fc.kinds)
	}
	if c.lastRevision != "rev1" {
		t.Errorf("lastRevision after the successful retry: want rev1, got %q", c.lastRevision)
	}
}

// apply stamps the managed-by label and revision annotation on every object, and must do so
// without dropping the labels/annotations the broker rendered (the auto-scaling annotation and
// the pool's node-template labels are what operators and the broker key off).
func TestMsApplyStampsOwnershipAndPreservesRenderedMetadata(t *testing.T) {
	c, rec := newRecordingController(t, &stubFetcher{resp: msBrokerConfig()})
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := rec.kinds(); len(got) != 2 || got[0] != "RafayNodeClass/gpu-h100" || got[1] != "NodePool/ml" {
		t.Fatalf("unexpected applies: %v", got)
	}

	// The node class had no metadata.labels/annotations at all: the nil-map path.
	nc := rec.patches[0].obj
	if nc.GetLabels()[managedByLabel] != managedByValue {
		t.Errorf("node class managed-by label missing: %v", nc.GetLabels())
	}
	if nc.GetAnnotations()[revisionAnnotation] != "6528d0658ac6f786" {
		t.Errorf("node class revision annotation missing: %v", nc.GetAnnotations())
	}

	// The pool already carried a broker annotation that must survive next to ours.
	np := rec.patches[1].obj
	ann := np.GetAnnotations()
	if ann["karpenter.rafay.io/auto-scaling"] != "true" {
		t.Errorf("broker-rendered annotation dropped by apply: %v", ann)
	}
	if ann[revisionAnnotation] != "6528d0658ac6f786" {
		t.Errorf("pool revision annotation missing: %v", ann)
	}
	if np.GetLabels()[managedByLabel] != managedByValue {
		t.Errorf("pool managed-by label missing: %v", np.GetLabels())
	}
	tmplLabels, _, _ := unstructured.NestedStringMap(np.Object, "spec", "template", "metadata", "labels")
	if tmplLabels["nodepool"] != "ml" || tmplLabels["tier"] != "gpu" {
		t.Errorf("node-template labels must be untouched by apply: %v", tmplLabels)
	}
	if _, ok := tmplLabels[managedByLabel]; ok {
		t.Errorf("managed-by must be stamped on the NodePool, not on its node template: %v", tmplLabels)
	}
}

// Cross-repo golden: edge-broker's yaml.v3 output has to decode through the provider's decoder
// and then fit the typed CRD schemas (fake client converts to v1alpha1.RafayNodeClass and
// karpv1.NodePool on store), which is what the real API server enforces via the CRD.
func TestMsDecodeBrokerRenderedManifests(t *testing.T) {
	classes, err := decodeManifests(msBrokerNodeClassYAML)
	if err != nil {
		t.Fatalf("decode broker node class: %v", err)
	}
	pools, err := decodeManifests(msBrokerNodePoolYAML)
	if err != nil {
		t.Fatalf("decode broker node pool: %v", err)
	}
	if len(classes) != 1 || len(pools) != 1 {
		t.Fatalf("want 1 class and 1 pool, got %d and %d", len(classes), len(pools))
	}
	// yaml.v3 quotes numeric-looking strings; they must arrive as strings, not numbers.
	gpu, found, err := unstructured.NestedString(classes[0].Object, "spec", "instanceTypes")
	if err == nil && found {
		t.Fatalf("instanceTypes decoded as a string: %q", gpu)
	}
	its, _, _ := unstructured.NestedSlice(classes[0].Object, "spec", "instanceTypes")
	it := its[0].(map[string]interface{})
	if it["cpu"] != "64" || it["nvidia.com/gpu"] != "8" || it["memory"] != "1024Gi" {
		t.Errorf("quoted quantities must decode as strings: %+v", it)
	}

	c, cl := newTestController(t, &stubFetcher{resp: msBrokerConfig()})
	if err := c.sync(context.Background()); err != nil {
		t.Fatalf("sync broker-rendered config: %v", err)
	}

	var nc v1alpha1.RafayNodeClass
	if err := cl.Get(context.Background(), client.ObjectKey{Name: "gpu-h100"}, &nc); err != nil {
		t.Fatalf("get typed RafayNodeClass: %v", err)
	}
	if len(nc.Spec.InstanceTypes) != 1 {
		t.Fatalf("typed instance types: %+v", nc.Spec.InstanceTypes)
	}
	typed := nc.Spec.InstanceTypes[0]
	if typed.Name != "gpu-h100" || typed.CPU != "64" || typed.Memory != "1024Gi" || typed.GPU != "8" {
		t.Errorf("instance type did not survive the typed conversion: %+v", typed)
	}
	if len(typed.Architectures) != 1 || typed.Architectures[0] != "amd64" || len(typed.OperatingSystems) != 1 || typed.OperatingSystems[0] != "linux" {
		t.Errorf("arch/os lists did not survive the typed conversion: %+v", typed)
	}

	var np karpv1.NodePool
	if err := cl.Get(context.Background(), client.ObjectKey{Name: "ml"}, &np); err != nil {
		t.Fatalf("get typed NodePool: %v", err)
	}
	if np.Spec.Weight == nil || *np.Spec.Weight != 100 {
		t.Errorf("weight: %+v", np.Spec.Weight)
	}
	if got := np.Spec.Limits["nvidia.com/gpu"]; got.String() != "16" {
		t.Errorf("gpu limit: want 16, got %s", got.String())
	}
	if len(np.Spec.Template.Spec.Taints) != 1 || np.Spec.Template.Spec.Taints[0].Key != "nvidia.com/gpu" {
		t.Errorf("taints: %+v", np.Spec.Template.Spec.Taints)
	}
	if np.Spec.Template.Spec.NodeClassRef == nil || np.Spec.Template.Spec.NodeClassRef.Name != "gpu-h100" {
		t.Errorf("nodeClassRef: %+v", np.Spec.Template.Spec.NodeClassRef)
	}
	if np.Spec.Disruption.ConsolidateAfter.Duration == nil || *np.Spec.Disruption.ConsolidateAfter.Duration != 5*time.Minute {
		t.Errorf("consolidateAfter: %+v", np.Spec.Disruption.ConsolidateAfter)
	}
	if np.Annotations["karpenter.rafay.io/auto-scaling"] != "true" {
		t.Errorf("broker annotation must reach the stored object: %v", np.Annotations)
	}
}

// logOrphans must name every managed object the broker no longer renders — NodePools and
// RafayNodeClasses alike — and must ignore hand-written objects (no managed-by label).
func TestMsLogOrphansReportsOnlyManagedObjectsMissingFromConfig(t *testing.T) {
	c, cl := newTestController(t, &stubFetcher{})
	msCreateUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "pool1", true)
	msCreateUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "stale-pool", true)
	msCreateUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "manual-pool", false)
	msCreateUnstructured(t, cl, "karpenter.rafay.io/v1alpha1", "RafayNodeClass", "oci-inst", true)
	msCreateUnstructured(t, cl, "karpenter.rafay.io/v1alpha1", "RafayNodeClass", "old-class", true)
	msCreateUnstructured(t, cl, "karpenter.rafay.io/v1alpha1", "RafayNodeClass", "manual-class", false)

	classes, err := decodeManifests(nodeClassYAML)
	if err != nil {
		t.Fatal(err)
	}
	pools, err := decodeManifests(nodePoolYAML)
	if err != nil {
		t.Fatal(err)
	}

	logs := msCaptureKlog(t)
	c.logOrphans(context.Background(), classes, pools)
	klog.Flush()
	out := logs.String()

	for _, want := range []string{`NodePool "stale-pool"`, `RafayNodeClass "old-class"`} {
		if !strings.Contains(out, want) {
			t.Errorf("orphan %s must be reported; log:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{`"manual-pool"`, `"manual-class"`, `"pool1"`, `"oci-inst"`} {
		if strings.Contains(out, unwanted) {
			t.Errorf("%s must not be reported as an orphan (hand-written or still in config); log:\n%s", unwanted, out)
		}
	}
	// Reporting is not pruning.
	getUnstructured(t, cl, "karpenter.sh/v1", "NodePool", "stale-pool")
	getUnstructured(t, cl, "karpenter.rafay.io/v1alpha1", "RafayNodeClass", "old-class")
}

// A failed sync never turns into a Start error (that would take the manager down); once the
// context ends the loop returns nil.
func TestMsStartReturnsNilWhenCancelledAfterFetchFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &msCancellingFetcher{cancel: cancel, err: errors.New("broker down")}
	c, _ := newTestController(t, fetcher)

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start must swallow sync failures, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
	if fetcher.calls != 1 {
		t.Errorf("want exactly one fetch before the cancel was observed, got %d", fetcher.calls)
	}
}

// After a successful sync Start waits for the interval or the context; a cancelled context
// returns nil promptly with the config applied.
func TestMsStartAppliesThenReturnsNilOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fetcher := &msCancellingFetcher{cancel: cancel, resp: fullConfig()}
	c, rec := newRecordingController(t, fetcher)

	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
	if len(rec.patches) != 2 {
		t.Errorf("the initial sync must apply before Start waits, got %v", rec.kinds())
	}
	if c.lastRevision != "rev1" {
		t.Errorf("lastRevision after Start's first sync: want rev1, got %q", c.lastRevision)
	}
}

func TestMsDecodeManifestsEdgeCases(t *testing.T) {
	t.Run("json document", func(t *testing.T) {
		objs, err := decodeManifests(`{"apiVersion":"karpenter.sh/v1","kind":"NodePool","metadata":{"name":"j"}}`)
		if err != nil || len(objs) != 1 || objs[0].GetName() != "j" {
			t.Errorf("JSON is accepted by the YAML-or-JSON decoder: objs=%v err=%v", objs, err)
		}
	})
	t.Run("comment-only document is skipped", func(t *testing.T) {
		objs, err := decodeManifests("# rendered by edge-broker\n---\n" + nodePoolYAML)
		if err != nil || len(objs) != 1 {
			t.Errorf("want the one real document, got %d (err %v)", len(objs), err)
		}
	})
	t.Run("malformed yaml is an error", func(t *testing.T) {
		if _, err := decodeManifests("apiVersion: v1\nkind: [unterminated\n"); err == nil {
			t.Error("syntactically broken YAML must be rejected before any apply")
		}
	})
	t.Run("kind without apiVersion", func(t *testing.T) {
		if _, err := decodeManifests("kind: NodePool\nmetadata:\n  name: x\n"); err == nil {
			t.Error("document without apiVersion should error")
		}
	})
	t.Run("error names the kind missing its name", func(t *testing.T) {
		_, err := decodeManifests("apiVersion: karpenter.rafay.io/v1alpha1\nkind: RafayNodeClass\nspec: {}\n")
		if err == nil || !strings.Contains(err.Error(), "RafayNodeClass") {
			t.Errorf("want the kind in the error, got %v", err)
		}
	})
	t.Run("bad document after a good one fails the whole batch", func(t *testing.T) {
		if _, err := decodeManifests(nodeClassYAML + "---\nfoo: bar\n"); err == nil {
			t.Error("a later invalid document must fail decoding so nothing half-applies")
		}
	})
}

func TestMsEnvParsingTrimsAndRejectsNegative(t *testing.T) {
	t.Setenv("KARPENTER_CONFIG_SYNC_INTERVAL", "  2m ")
	if got := SyncIntervalFromEnv(); got != 2*time.Minute {
		t.Errorf("surrounding whitespace should be trimmed, got %s", got)
	}
	t.Setenv("KARPENTER_CONFIG_SYNC_INTERVAL", "-1m")
	if got := SyncIntervalFromEnv(); got != defaultSyncInterval {
		t.Errorf("negative interval must fall back to the default, got %s", got)
	}
	t.Setenv("KARPENTER_CONFIG_SYNC_INTERVAL", "1.5h")
	if got := SyncIntervalFromEnv(); got != 90*time.Minute {
		t.Errorf("fractional durations are valid Go durations, got %s", got)
	}

	t.Setenv("KARPENTER_CONFIG_BOOTSTRAP", "  false  ")
	if EnabledFromEnv() {
		t.Error("whitespace around a boolean must be trimmed")
	}
	t.Setenv("KARPENTER_CONFIG_BOOTSTRAP", "f")
	if EnabledFromEnv() {
		t.Error("strconv.ParseBool accepts \"f\"")
	}
	t.Setenv("KARPENTER_CONFIG_BOOTSTRAP", "T")
	if !EnabledFromEnv() {
		t.Error("strconv.ParseBool accepts \"T\"")
	}
}
