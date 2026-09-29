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

package headroom

// hr_fixes_test.go — behaviour added by the September 2026 fixes that the parked regression
// tests do not cover: the maxPods ceiling, the fail-closed edge of the unrelated-key skip,
// inert-pool handling around an existing Deployment and NodePool read errors, the
// once-per-transition inert log, and the PriorityClass create race.

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// --- maxPods ---

func TestHrUnmarshalPools_MaxPods(t *testing.T) {
	pools, err := unmarshalPools("pools:\n  - name: p\n    cpu: 30%\n    minPods: 2\n    maxPods: 6\n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pools) != 1 || pools[0].MinPods != 2 || pools[0].MaxPods != 6 {
		t.Fatalf("got %+v, want minPods 2 / maxPods 6", pools)
	}

	cases := map[string]string{
		"negative":       "pools:\n  - name: p\n    maxPods: -1\n",
		"below minPods":  "pools:\n  - name: p\n    minPods: 3\n    maxPods: 2\n",
		"not an integer": "pools:\n  - name: p\n    maxPods: many\n",
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if got, err := unmarshalPools(data); err == nil {
				t.Fatalf("expected an error, got %+v", got)
			} else if name != "not an integer" && !strings.Contains(err.Error(), "maxPods") {
				t.Errorf("error %q does not mention maxPods", err.Error())
			}
		})
	}
}

func TestHrDesiredReplicas_MaxPodsCeiling(t *testing.T) {
	mustQ := resource.MustParse
	base := parsedPoolPolicy{CPU: 0.50, PodCPU: mustQ("500m"), PodMemory: mustQ("512Mi")}
	total := poolAllocatable{cpuMilli: 8000} // 0.5 × 8000 / 500 = 8

	if got := desiredReplicas(base, total, 2); got != 8 {
		t.Fatalf("without maxPods: %d, want 8", got)
	}
	capped := base
	capped.MaxPods = 5
	if got := desiredReplicas(capped, total, 2); got != 5 {
		t.Errorf("maxPods 5: %d, want 5", got)
	}
	// maxPods above the computed count is a no-op.
	capped.MaxPods = 50
	if got := desiredReplicas(capped, total, 2); got != 8 {
		t.Errorf("maxPods 50: %d, want 8", got)
	}
	// The cold-start floor is still honoured (parse-time validation keeps maxPods >= minPods).
	capped.MinPods, capped.MaxPods = 3, 3
	if got := desiredReplicas(capped, poolAllocatable{}, 0); got != 3 {
		t.Errorf("cold start with minPods=maxPods=3: %d, want 3", got)
	}
}

// --- unrelated-key skip stays fail-closed ---

// TestHrParseHeadroomConfig_MalformedUnknownKeyStillErrors: only WELL-FORMED scalars/lists are
// skipped. Malformed YAML under a fallback key is still a broken policy — a typo there must not
// silently read as "no pools" and GC the buffer.
func TestHrParseHeadroomConfig_MalformedUnknownKeyStillErrors(t *testing.T) {
	cases := map[string]map[string]string{
		"malformed fallback key alone":                      {"a-key": "pools: [garbage"},
		"malformed fallback key next to malformed policy":   {"policy": "pools: [garbage", "a-key": "pools: [garbage"},
		"malformed fallback key next to malformed config":   {"policy": "pools: []\n", "config": "pools: [garbage", "a-key": "pools: [garbage"},
		"scalar under a well-known key":                     {"config": "off"},
		"scalar under a well-known key next to clean other": {"policy": "pools: []\n", "config": "off"},
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if pools, err := parseHeadroomConfig(newConfigMap(data)); err == nil {
				t.Fatalf("expected an error, got pools %+v", pools)
			}
		})
	}
	// A malformed fallback key next to a well-known key that parsed cleanly cannot be the policy:
	// it is skipped (warning), so the legitimate `pools: []` still tears the buffer down.
	pools, err := parseHeadroomConfig(newConfigMap(map[string]string{"policy": "pools: []\n", "a-key": "pools: [garbage"}))
	if err != nil || len(pools) != 0 {
		t.Fatalf("malformed fallback key next to a clean policy: got %+v, %v; want no pools and no error", pools, err)
	}
	// A well-formed list is not a mapping: skipped like free text.
	pools, err = parseHeadroomConfig(newConfigMap(map[string]string{"policy": "pools: []\n", "owners": "- alice\n- bob\n"}))
	if err != nil || len(pools) != 0 {
		t.Fatalf("list-valued unrelated key: got %+v, %v; want no pools and no error", pools, err)
	}
}

func TestHrIsNonMappingYAML(t *testing.T) {
	cases := map[string]bool{
		"":                      false, // empty: could be a (nil) mapping
		"pools: []":             false,
		"a: 1\nb: 2":            false,
		"pools: [garbage":       false, // malformed: NOT skipped
		"free text":             true,
		"false":                 true,
		"3":                     true,
		"- a\n- b":              true,
		"\"quoted string\"":     true,
		"disabled during maint": true,
	}
	for in, want := range cases {
		if got := isNonMappingYAML(in); got != want {
			t.Errorf("isNonMappingYAML(%q) = %v, want %v", in, got, want)
		}
	}
}

// --- inert pools ---

// TestHrReconcilePool_InertPoolNeverCreatesDeployment: an inert pool with no Deployment yet gets
// none (a 0-replica Deployment for a pool that cannot launch is clutter).
func TestHrReconcilePool_InertPoolNeverCreatesDeployment(t *testing.T) {
	for name, objs := range map[string][]client.Object{
		"annotated false":  {fupNodePool("p", false)},
		"missing NodePool": {},
		"limits.nodes 0 only": {func() *karpv1.NodePool {
			np := fupNodePool("p", true)
			np.Spec.Limits = karpv1.Limits{"nodes": resource.MustParse("0")}
			return np
		}()},
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestController(append(objs, makeNode("n1", "p", standardAlloc("4", "16Gi"), true))...)
			if err := c.reconcilePool(context.Background(), hrSimplePool("p")); err != nil {
				t.Fatalf("reconcilePool: %v", err)
			}
			if hrExists(t, c, hrDeployment(deploymentName("p"), testNamespace, nil)) {
				t.Fatal("a Deployment was created for an inert pool")
			}
		})
	}
}

// TestHrReconcilePool_InertPoolScalesExistingToZeroAndBack: an existing Deployment is held at 0
// (shape still owned) and resumes its size when the pool is live again.
func TestHrReconcilePool_InertPoolScalesExistingToZeroAndBack(t *testing.T) {
	ctx := context.Background()
	pool := hrSimplePool("p")
	c := newTestController(fupNodePool("p", false),
		hrDeployment(deploymentName("p"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "p"}),
		makeNode("n1", "p", standardAlloc("4", "16Gi"), true))

	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (inert): %v", err)
	}
	dep := hrGetDeployment(t, c, "p")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		t.Fatalf("replicas = %v, want 0 for an inert pool", dep.Spec.Replicas)
	}
	if dep.Spec.Template.Spec.NodeSelector[karpenterNodePoolLabel] != "p" || dep.Spec.Template.Spec.PriorityClassName != priorityClassName {
		t.Errorf("held-at-0 Deployment lost its owned shape: %+v", dep.Spec.Template.Spec)
	}

	var np karpv1.NodePool
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: "p"}, &np); err != nil {
		t.Fatalf("get NodePool: %v", err)
	}
	np.Annotations[fupAutoScalingAnnotation] = "true"
	np.Spec.Limits = karpv1.Limits{"nodes": resource.MustParse("10")}
	if err := c.kubeClient.Update(ctx, &np); err != nil {
		t.Fatalf("update NodePool: %v", err)
	}
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (live again): %v", err)
	}
	if r := hrGetDeployment(t, c, "p").Spec.Replicas; r == nil || *r != 4 {
		t.Errorf("replicas after the pool went live = %v, want 4", r)
	}
}

// TestHrReconcilePool_NodePoolReadErrorIsReturned: a transient NodePool read failure must not be
// mistaken for "missing" (which would scale the buffer to 0); it is returned for retry.
func TestHrReconcilePool_NodePoolReadErrorIsReturned(t *testing.T) {
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*karpv1.NodePool); ok {
				return kerrors.NewServiceUnavailable("apiserver down")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}
	existing := hrDeployment(deploymentName("p"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "p"})
	existing.Spec.Replicas = new(int32)
	*existing.Spec.Replicas = 4
	c := hrNewInterceptedController(funcs, livePool("p"), existing, makeNode("n1", "p", standardAlloc("4", "16Gi"), true))

	err := c.reconcilePool(context.Background(), hrSimplePool("p"))
	if err == nil || !strings.Contains(err.Error(), "get nodepool") {
		t.Fatalf("got %v, want the wrapped NodePool read error", err)
	}
	if r := hrGetDeployment(t, c, "p").Spec.Replicas; r == nil || *r != 4 {
		t.Errorf("replicas = %v, want the Deployment untouched (4) on a read error", r)
	}
}

// TestHrNoteInert_LogsOncePerTransition: the inert reason is reported once, not on every 5-minute
// resync, and again only when the reason changes or the pool goes live.
func TestHrNoteInert_LogsOncePerTransition(t *testing.T) {
	logs := hrCaptureKlog(t)
	// klog writes a WARNING line to the INFO stream too when a single output is set, so one
	// logical message can appear more than once; compare deltas, not absolute counts.
	count := func(marker string) int {
		klog.Flush()
		return strings.Count(logs.String(), marker)
	}
	const (
		missing   = `pool "p" is inert (NodePool does not exist)`
		annotated = `pool "p" is inert (` + autoScalingAnnotationKey + `=false)`
		live      = `pool "p" is live again`
	)
	c := newTestController()

	c.noteInert("p", "NodePool does not exist")
	once := count(missing)
	if once == 0 {
		t.Fatalf("first inert transition was not logged:\n%s", logs.String())
	}
	c.noteInert("p", "NodePool does not exist")
	c.noteInert("p", "NodePool does not exist")
	if n := count(missing); n != once {
		t.Errorf("repeating the same inert reason logged again (%d → %d)", once, n)
	}

	c.noteInert("p", autoScalingAnnotationKey+"=false")
	changed := count(annotated)
	if changed == 0 {
		t.Errorf("a changed inert reason was not logged:\n%s", logs.String())
	}
	c.noteInert("p", autoScalingAnnotationKey+"=false")
	if n := count(annotated); n != changed {
		t.Errorf("repeating the changed reason logged again (%d → %d)", changed, n)
	}

	c.noteInert("p", "")
	back := count(live)
	if back == 0 {
		t.Errorf("the live-again transition was not logged:\n%s", logs.String())
	}
	c.noteInert("p", "")
	c.noteInert("q", "") // never inert: silent
	if n := count(live); n != back {
		t.Errorf("a live pool staying live logged again (%d → %d)", back, n)
	}
	if strings.Contains(logs.String(), `pool "q"`) {
		t.Errorf("a never-inert pool was mentioned:\n%s", logs.String())
	}

	// Forgetting a pool that left the policy makes its next inert state reportable again.
	c.noteInert("p", "NodePool does not exist")
	again := count(missing)
	if again != 2*once {
		t.Errorf("re-entering the inert state after going live should log again (%d → %d)", once, again)
	}
	c.forgetInertExcept(nil)
	c.noteInert("p", "NodePool does not exist")
	if n := count(missing); n != 3*once {
		t.Errorf("after forgetInertExcept the warning should be logged again (%d → %d)", again, n)
	}
}

// TestHrReconcile_InertPoolIsStillConfigured: an inert pool stays in the configured set, so its
// held-at-0 Deployment is not garbage-collected as "stale".
func TestHrReconcile_InertPoolIsStillConfigured(t *testing.T) {
	dep := hrDeployment(deploymentName("p"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "p"})
	c := newTestController(hrPolicyConfigMap("pools:\n  - name: p\n    cpu: 50%\n"), fupNodePool("p", false), dep,
		makeNode("n1", "p", standardAlloc("4", "16Gi"), true))
	if _, err := c.Reconcile(context.Background(), hrSyncRequest()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !hrExists(t, c, dep) {
		t.Fatal("the inert pool's Deployment was garbage-collected")
	}
	if r := hrGetDeployment(t, c, "p").Spec.Replicas; r == nil || *r != 0 {
		t.Errorf("replicas = %v, want 0", r)
	}
}

// --- PriorityClass ---

// TestHrEnsurePriorityClass_AlreadyExistsRaceFallsBackToDriftCheck: a Create that 409s although
// the Get saw nothing (another creator won, or the cache lagged) is not an error; the winner's
// settings are checked for drift instead.
func TestHrEnsurePriorityClass_AlreadyExistsRaceFallsBackToDriftCheck(t *testing.T) {
	never := corev1.PreemptNever
	winner := hrPriorityClass(priorityValue, &never)
	created := false
	funcs := interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*schedulingv1.PriorityClass); ok {
				created = true
				// The class appears between the Get and the Create.
				if err := cl.Create(ctx, winner); err != nil {
					return err
				}
				return kerrors.NewAlreadyExists(schema.GroupResource{Group: "scheduling.k8s.io", Resource: "priorityclasses"}, obj.GetName())
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
	c := hrNewInterceptedController(funcs)
	if err := c.ensurePriorityClass(context.Background()); err != nil {
		t.Fatalf("ensurePriorityClass on a create race: %v", err)
	}
	if !created {
		t.Fatal("expected a Create attempt when the class does not exist")
	}
}

func TestHrEnsurePriorityClass_GetErrorIsWrapped(t *testing.T) {
	funcs := interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*schedulingv1.PriorityClass); ok {
				return errors.New("boom")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}
	c := hrNewInterceptedController(funcs)
	err := c.ensurePriorityClass(context.Background())
	if err == nil || !strings.Contains(err.Error(), "get existing PriorityClass") {
		t.Fatalf("got %v, want the wrapped Get error (a non-NotFound read failure must not fall through to Create)", err)
	}
	// Get is intercepted for PriorityClasses, so check existence through List.
	var pcs schedulingv1.PriorityClassList
	if err := c.kubeClient.List(context.Background(), &pcs); err != nil {
		t.Fatalf("list PriorityClasses: %v", err)
	}
	if len(pcs.Items) != 0 {
		t.Errorf("PriorityClass was created despite the read failure: %+v", pcs.Items)
	}
}

// --- pod template hardening ---

// TestHrReconcilePool_StripsLegacySpreadFromExistingDeployment: a Deployment created by an
// earlier version carries the hostname spread; the mutate fn must remove it, not merely stop
// adding it.
func TestHrReconcilePool_StripsLegacySpreadFromExistingDeployment(t *testing.T) {
	existing := hrDeployment(deploymentName("p"), testNamespace, map[string]string{headroomLabel: "true", headroomPoolLabel: "p"})
	existing.Spec.Template.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}
	c := newTestController(livePool("p"), existing, makeNode("n1", "p", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(context.Background(), hrSimplePool("p")); err != nil {
		t.Fatalf("reconcilePool: %v", err)
	}
	spec := hrGetDeployment(t, c, "p").Spec.Template.Spec
	if len(spec.TopologySpreadConstraints) != 0 {
		t.Errorf("legacy topology spread survived: %+v", spec.TopologySpreadConstraints)
	}
	if spec.Affinity == nil || spec.Affinity.PodAffinity == nil {
		t.Error("pack affinity missing on the updated Deployment")
	}
}

// TestHrReconcilePool_SecurityContextIsStableAcrossReconciles: the hardened template must not
// make the steady-state reconcile issue an Update (the no-spurious-update contract).
func TestHrReconcilePool_SecurityContextIsStableAcrossReconciles(t *testing.T) {
	ctx := context.Background()
	pool := hrSimplePool("p")
	c := newTestController(livePool("p"), makeNode("n1", "p", standardAlloc("4", "16Gi"), true))
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (create): %v", err)
	}
	before := hrGetDeployment(t, c, "p").ResourceVersion
	if err := c.reconcilePool(ctx, pool); err != nil {
		t.Fatalf("reconcilePool (steady state): %v", err)
	}
	after := hrGetDeployment(t, c, "p")
	if after.ResourceVersion != before {
		t.Errorf("steady-state reconcile issued an Update (%s → %s)", before, after.ResourceVersion)
	}
	var dep appsv1.Deployment = *after
	if dep.Spec.Template.Spec.SecurityContext.RunAsUser == nil || *dep.Spec.Template.Spec.SecurityContext.RunAsUser != pauseRunAsUser {
		t.Errorf("runAsUser = %v, want %d", dep.Spec.Template.Spec.SecurityContext.RunAsUser, pauseRunAsUser)
	}
}
