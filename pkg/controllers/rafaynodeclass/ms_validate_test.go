/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafaynodeclass

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
)

func msNodeClass(name string, its ...v1alpha1.InstanceTypeSpec) *v1alpha1.RafayNodeClass {
	return &v1alpha1.RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.RafayNodeClassSpec{InstanceTypes: its},
	}
}

// msStatusPatchCounter wraps a fake client and counts (or fails) status patches.
type msStatusPatchCounter struct {
	client.Client
	patches int
	fail    error
}

func msNewStatusPatchCounter(t *testing.T, objs ...client.Object) *msStatusPatchCounter {
	t.Helper()
	pc := &msStatusPatchCounter{}
	pc.Client = interceptor.NewClient(
		fake.NewClientBuilder().
			WithScheme(newTestScheme(t)).
			WithObjects(objs...).
			WithStatusSubresource(&v1alpha1.RafayNodeClass{}).
			Build(),
		interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				pc.patches++
				if pc.fail != nil {
					return pc.fail
				}
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		})
	return pc
}

func TestMsValidateNodeClass(t *testing.T) {
	for _, tc := range []struct {
		name string
		its  []v1alpha1.InstanceTypeSpec
		want string // substring of the message, "" for valid
	}{
		{"valid", []v1alpha1.InstanceTypeSpec{{Name: "a", CPU: "4", Memory: "8Gi"}}, ""},
		{"valid with gpu", []v1alpha1.InstanceTypeSpec{{Name: "g", CPU: "64", Memory: "1024Gi", GPU: "8"}}, ""},
		{"empty list", nil, "at least one entry"},
		{"empty name reports its index", []v1alpha1.InstanceTypeSpec{
			{Name: "ok", CPU: "1", Memory: "1Gi"},
			{Name: "", CPU: "1", Memory: "1Gi"},
		}, "spec.instanceTypes[1] has an empty name"},
		{"invalid cpu", []v1alpha1.InstanceTypeSpec{{Name: "a", CPU: "four", Memory: "8Gi"}}, `instance type "a" has invalid cpu "four"`},
		{"invalid memory", []v1alpha1.InstanceTypeSpec{{Name: "a", CPU: "4", Memory: "8GB?"}}, `instance type "a" has invalid memory "8GB?"`},
		{"invalid gpu", []v1alpha1.InstanceTypeSpec{{Name: "a", CPU: "4", Memory: "8Gi", GPU: "eight"}}, `instance type "a" has invalid nvidia.com/gpu "eight"`},
		{"empty cpu is invalid", []v1alpha1.InstanceTypeSpec{{Name: "a", CPU: "", Memory: "8Gi"}}, "invalid cpu"},
		{"first offending entry wins", []v1alpha1.InstanceTypeSpec{
			{Name: "first", CPU: "x", Memory: "8Gi"},
			{Name: "second", CPU: "4", Memory: "y"},
		}, `"first"`},
		// Pinned current behaviour, not a requirement: the validator only checks parseability.
		{"duplicate names pass", []v1alpha1.InstanceTypeSpec{
			{Name: "dup", CPU: "4", Memory: "8Gi"},
			{Name: "dup", CPU: "8", Memory: "16Gi"},
		}, ""},
		{"zero and negative quantities pass", []v1alpha1.InstanceTypeSpec{{Name: "z", CPU: "0", Memory: "-1Gi"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := validateNodeClass(msNodeClass("nc", tc.its...))
			if tc.want == "" && got != "" {
				t.Fatalf("want valid, got %q", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("want message containing %q, got %q", tc.want, got)
			}
		})
	}
}

// A NodeClass being deleted is left alone: no validation, no status patch.
func TestMsReconcileSkipsDeletingNodeClass(t *testing.T) {
	nc := msNodeClass("going", v1alpha1.InstanceTypeSpec{Name: "bad", CPU: "x", Memory: "8Gi"})
	now := metav1.NewTime(time.Now())
	nc.DeletionTimestamp = &now
	nc.Finalizers = []string{"test/finalizer"} // the fake client rejects a deleting object with none
	pc := msNewStatusPatchCounter(t, nc)

	var current v1alpha1.RafayNodeClass
	if err := pc.Get(context.Background(), client.ObjectKey{Name: nc.Name}, &current); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := NewController(pc).Reconcile(context.Background(), &current); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if pc.patches != 0 {
		t.Errorf("a deleting NodeClass must not be patched, got %d patch(es)", pc.patches)
	}
	if len(current.Status.Conditions) != 0 {
		t.Errorf("no condition may be computed for a deleting NodeClass: %+v", current.Status.Conditions)
	}
}

// A status patch failure is returned so controller-runtime requeues the object.
func TestMsReconcilePropagatesStatusPatchError(t *testing.T) {
	nc := msNodeClass("flaky", v1alpha1.InstanceTypeSpec{Name: "a", CPU: "4", Memory: "8Gi"})
	pc := msNewStatusPatchCounter(t, nc)
	pc.fail = errors.New("status patch refused")

	_, err := NewController(pc).Reconcile(context.Background(), nc)
	if err == nil || !errors.Is(err, pc.fail) {
		t.Fatalf("want the patch error propagated, got %v", err)
	}
	if pc.patches != 1 {
		t.Errorf("exactly one patch attempt expected, got %d", pc.patches)
	}
}

// Covers R1-test-quality-9: the second reconcile of an already-Ready NodeClass must not issue
// a status patch at all (not merely leave the condition True). The message-less SetTrue keeps
// LastTransitionTime from the stored condition, so the recomputed condition is DeepEqual and
// operatorpkg reports no change.
func TestMsReconcileSecondPassIssuesNoPatch(t *testing.T) {
	nc := msNodeClass("steady", v1alpha1.InstanceTypeSpec{Name: "a", CPU: "4", Memory: "8Gi"})
	pc := msNewStatusPatchCounter(t, nc)
	c := NewController(pc)

	for i := 0; i < 3; i++ {
		var current v1alpha1.RafayNodeClass
		if err := pc.Get(context.Background(), client.ObjectKey{Name: nc.Name}, &current); err != nil {
			t.Fatalf("get #%d: %v", i+1, err)
		}
		if _, err := c.Reconcile(context.Background(), &current); err != nil {
			t.Fatalf("Reconcile #%d: %v", i+1, err)
		}
		if pc.patches != 1 {
			t.Fatalf("after reconcile #%d: want exactly 1 status patch in total, got %d", i+1, pc.patches)
		}
	}

	// A spec change that flips validity is still written.
	var current v1alpha1.RafayNodeClass
	if err := pc.Get(context.Background(), client.ObjectKey{Name: nc.Name}, &current); err != nil {
		t.Fatalf("get: %v", err)
	}
	current.Spec.InstanceTypes[0].Memory = "lots"
	if _, err := c.Reconcile(context.Background(), &current); err != nil {
		t.Fatalf("Reconcile after spec change: %v", err)
	}
	if pc.patches != 2 {
		t.Errorf("flipping Ready to False must patch, got %d patches", pc.patches)
	}
	if cond := readyCondition(t, &current); cond.Status != metav1.ConditionFalse || !strings.Contains(cond.Message, "invalid memory") {
		t.Errorf("Ready after spec change: %+v", cond)
	}
}
