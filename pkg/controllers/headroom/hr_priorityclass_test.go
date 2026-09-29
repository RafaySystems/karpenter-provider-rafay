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

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// hrCaptureKlog routes klog output into a buffer for the duration of the test. klog state is
// process-global, so tests using it must not run in parallel with each other.
func hrCaptureKlog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	klog.LogToStderr(false)
	klog.SetOutput(&buf)
	t.Cleanup(func() {
		klog.Flush()
		klog.LogToStderr(true)
		klog.SetOutput(nil)
	})
	return &buf
}

func hrPriorityClass(value int32, policy *corev1.PreemptionPolicy) *schedulingv1.PriorityClass {
	return &schedulingv1.PriorityClass{
		ObjectMeta:       metav1.ObjectMeta{Name: priorityClassName},
		Value:            value,
		PreemptionPolicy: policy,
	}
}

// TestHrEnsurePriorityClass_NoCreateWhenPresent is the regression test for
// R1-prov-headroom-7: once the PriorityClass exists, a reconcile must not issue a write-path
// Create (expected 409) on every sync — each pool-node status update and each pause-pod
// churn event triggers a reconcile, so this is one audit-logged POST per event.
func TestHrEnsurePriorityClass_NoCreateWhenPresent(t *testing.T) {
	never := corev1.PreemptNever
	var creates atomic.Int32
	funcs := hrCountingCreate(&creates, func(obj client.Object) bool {
		_, ok := obj.(*schedulingv1.PriorityClass)
		return ok
	})
	c := hrNewInterceptedController(funcs, hrPriorityClass(priorityValue, &never))

	for i := 0; i < 2; i++ {
		if _, err := c.Reconcile(context.Background(), hrSyncRequest()); err != nil {
			t.Fatalf("Reconcile %d: %v", i, err)
		}
	}
	if n := creates.Load(); n != 0 {
		t.Fatalf("PriorityClass Create was called %d times although the class already exists, want 0", n)
	}
}

// TestHrEnsurePriorityClass_CreatesOnceThenReuses locks in the part that does hold today: the
// class is created exactly once across reconciles (never re-created or duplicated), and its
// spec is the documented one.
func TestHrEnsurePriorityClass_CreatesOnceThenReuses(t *testing.T) {
	var creates atomic.Int32
	funcs := hrCountingCreate(&creates, func(obj client.Object) bool {
		_, ok := obj.(*schedulingv1.PriorityClass)
		return ok
	})
	c := hrNewInterceptedController(funcs)
	for i := 0; i < 3; i++ {
		if err := c.ensurePriorityClass(context.Background()); err != nil {
			t.Fatalf("ensurePriorityClass %d: %v", i, err)
		}
	}
	var pc schedulingv1.PriorityClass
	if err := c.kubeClient.Get(context.Background(), types.NamespacedName{Name: priorityClassName}, &pc); err != nil {
		t.Fatalf("get PriorityClass: %v", err)
	}
	if pc.Value != priorityValue || pc.GlobalDefault || pc.PreemptionPolicy == nil || *pc.PreemptionPolicy != corev1.PreemptNever {
		t.Errorf("PriorityClass = value %d globalDefault %v preemptionPolicy %v; want %d/false/Never",
			pc.Value, pc.GlobalDefault, pc.PreemptionPolicy, priorityValue)
	}
	if pc.Value >= 0 {
		t.Errorf("value %d is not negative: normal (priority 0) pods could not preempt headroom pods", pc.Value)
	}
	if n := creates.Load(); n < 1 {
		t.Errorf("expected at least one successful Create, got %d", n)
	}
}

// TestHrEnsurePriorityClass_CreateFailureIsToleratedByReconcile: the PriorityClass is only
// re-ensured on each sync; a failure there is logged and must not block the pool sync.
func TestHrEnsurePriorityClass_CreateFailureIsToleratedByReconcile(t *testing.T) {
	funcs := hrCountingCreate(nil, nil)
	funcs.Create = func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if _, ok := obj.(*schedulingv1.PriorityClass); ok {
			return kerrors.NewForbidden(schema.GroupResource{Group: "scheduling.k8s.io", Resource: "priorityclasses"}, obj.GetName(), errors.New("rbac"))
		}
		return cl.Create(ctx, obj, opts...)
	}
	c := hrNewInterceptedController(funcs,
		hrPolicyConfigMap("pools:\n  - name: pool-a\n    cpu: 50%\n"),
		livePool("pool-a"),
		makeNode("n1", "pool-a", standardAlloc("4", "16Gi"), true),
	)
	logs := hrCaptureKlog(t)

	res, err := c.Reconcile(context.Background(), hrSyncRequest())
	if err != nil {
		t.Fatalf("Reconcile must tolerate a PriorityClass failure, got: %v", err)
	}
	if res.RequeueAfter != resyncInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, resyncInterval)
	}
	if d := hrGetDeployment(t, c, "pool-a"); d.Spec.Replicas == nil || *d.Spec.Replicas != 4 {
		t.Errorf("pool-a replicas = %v, want 4 — pools must still be applied", d.Spec.Replicas)
	}
	klog.Flush()
	if !strings.Contains(logs.String(), "could not ensure PriorityClass") {
		t.Errorf("expected a warning about the PriorityClass failure, logs:\n%s", logs.String())
	}

	// ensurePriorityClass itself does surface the error to its caller.
	if err := c.ensurePriorityClass(context.Background()); err == nil {
		t.Error("ensurePriorityClass must return a non-AlreadyExists Create error")
	}
}

func TestHrCheckPriorityClassDrift_Warnings(t *testing.T) {
	never := corev1.PreemptNever
	lower := corev1.PreemptLowerPriority
	cases := []struct {
		name        string
		existing    *schedulingv1.PriorityClass
		wantWarn    []string
		wantNoWarns bool
	}{
		{
			name:        "matching class is silent",
			existing:    hrPriorityClass(priorityValue, &never),
			wantNoWarns: true,
		},
		{
			name:     "drifted value",
			existing: hrPriorityClass(0, &never),
			wantWarn: []string{"has value=0", "expected -1000"},
		},
		{
			name:     "positive value",
			existing: hrPriorityClass(100, &never),
			wantWarn: []string{"has value=100", "may not be preemptable"},
		},
		{
			name:     "preemptionPolicy PreemptLowerPriority",
			existing: hrPriorityClass(priorityValue, &lower),
			wantWarn: []string{`preemptionPolicy="PreemptLowerPriority"`, `expected "Never"`, "may preempt real workloads"},
		},
		{
			name:     "nil preemptionPolicy (server default PreemptLowerPriority)",
			existing: hrPriorityClass(priorityValue, nil),
			wantWarn: []string{`preemptionPolicy=""`, `expected "Never"`},
		},
		{
			name:     "both drifted reports both",
			existing: hrPriorityClass(5, nil),
			wantWarn: []string{"has value=5", `preemptionPolicy=""`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := hrCaptureKlog(t)
			c := newTestController(tc.existing)
			if err := c.checkPriorityClassDrift(context.Background()); err != nil {
				t.Fatalf("checkPriorityClassDrift must never error on drift, got: %v", err)
			}
			klog.Flush()
			out := logs.String()
			if tc.wantNoWarns {
				if strings.Contains(out, "PriorityClass") {
					t.Errorf("unexpected warning for a matching class:\n%s", out)
				}
				return
			}
			for _, want := range tc.wantWarn {
				if !strings.Contains(out, want) {
					t.Errorf("warning does not contain %q; logs:\n%s", want, out)
				}
			}
			// Drift is reported, never repaired.
			var after schedulingv1.PriorityClass
			if err := c.kubeClient.Get(context.Background(), types.NamespacedName{Name: priorityClassName}, &after); err != nil {
				t.Fatalf("get PriorityClass: %v", err)
			}
			if after.Value != tc.existing.Value {
				t.Errorf("value was mutated: %d → %d", tc.existing.Value, after.Value)
			}
		})
	}
}

func TestHrCheckPriorityClassDrift_GetErrorIsReturned(t *testing.T) {
	never := corev1.PreemptNever
	funcs := hrCountingCreate(nil, nil)
	funcs.Get = func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*schedulingv1.PriorityClass); ok {
			return kerrors.NewServiceUnavailable("apiserver down")
		}
		return cl.Get(ctx, key, obj, opts...)
	}
	c := hrNewInterceptedController(funcs, hrPriorityClass(priorityValue, &never))

	err := c.checkPriorityClassDrift(context.Background())
	if err == nil {
		t.Fatal("expected the Get error to be returned")
	}
	if !strings.Contains(err.Error(), "get existing PriorityClass") {
		t.Errorf("error %q should be wrapped with context", err.Error())
	}
	// And ensurePriorityClass propagates it through the AlreadyExists branch.
	if err := c.ensurePriorityClass(context.Background()); err == nil || !strings.Contains(err.Error(), "get existing PriorityClass") {
		t.Errorf("ensurePriorityClass with an existing class and a failing Get: got %v, want the wrapped Get error", err)
	}
}

// hrCountingCreate returns interceptor funcs whose Create increments n for objects matching
// match (when both are non-nil) and otherwise delegates to the real client.
func hrCountingCreate(n *atomic.Int32, match func(client.Object) bool) interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if n != nil && match != nil && match(obj) {
				n.Add(1)
			}
			return cl.Create(ctx, obj, opts...)
		},
	}
}

// --- mapToSyncRequest ---

func TestHrMapToSyncRequest_CollapsesEveryObject(t *testing.T) {
	objs := []client.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1", Labels: map[string]string{karpenterNodePoolLabel: "p"}}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: "somewhere"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "other"}},
		nil,
	}
	want := reconcile.Request{NamespacedName: types.NamespacedName{Name: syncRequestName}}
	for _, obj := range objs {
		got := mapToSyncRequest(context.Background(), obj)
		if len(got) != 1 {
			t.Fatalf("mapToSyncRequest(%T) returned %d requests, want exactly 1", obj, len(got))
		}
		if got[0] != want {
			t.Errorf("mapToSyncRequest(%T) = %v, want %v (cluster-scoped synthetic key)", obj, got[0], want)
		}
	}
}
