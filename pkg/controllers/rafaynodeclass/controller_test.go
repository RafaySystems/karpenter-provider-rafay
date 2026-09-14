/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafaynodeclass

import (
	"context"
	"strings"
	"testing"

	"github.com/awslabs/operatorpkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add v1alpha1 scheme: %v", err)
	}
	return s
}

// reconcileNodeClass runs one Reconcile against a fake client seeded with nc and returns the
// stored object afterwards.
func reconcileNodeClass(t *testing.T, nc *v1alpha1.RafayNodeClass) *v1alpha1.RafayNodeClass {
	t.Helper()
	cl := fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithObjects(nc).
		WithStatusSubresource(&v1alpha1.RafayNodeClass{}).
		Build()
	c := NewController(cl)

	if _, err := c.Reconcile(context.Background(), nc); err != nil {
		t.Fatalf("Reconcile: unexpected error: %v", err)
	}

	var out v1alpha1.RafayNodeClass
	if err := cl.Get(context.Background(), client.ObjectKey{Name: nc.Name}, &out); err != nil {
		t.Fatalf("get RafayNodeClass %q: %v", nc.Name, err)
	}
	return &out
}

func readyCondition(t *testing.T, nc *v1alpha1.RafayNodeClass) *status.Condition {
	t.Helper()
	for i := range nc.Status.Conditions {
		if nc.Status.Conditions[i].Type == status.ConditionReady {
			return &nc.Status.Conditions[i]
		}
	}
	t.Fatalf("RafayNodeClass %q has no %s condition: %+v", nc.Name, status.ConditionReady, nc.Status.Conditions)
	return nil
}

func TestReconcileValidSpecSetsReadyTrue(t *testing.T) {
	nc := &v1alpha1.RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "valid"},
		Spec: v1alpha1.RafayNodeClassSpec{
			InstanceTypes: []v1alpha1.InstanceTypeSpec{
				{Name: "standard-4-8", CPU: "4", Memory: "8Gi"},
				{Name: "standard-8-16", CPU: "8000m", Memory: "16Gi"},
			},
		},
	}
	out := reconcileNodeClass(t, nc)
	cond := readyCondition(t, out)
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %s (reason %q, message %q), want True", cond.Status, cond.Reason, cond.Message)
	}
}

func TestReconcileEmptyInstanceTypesSetsReadyFalse(t *testing.T) {
	nc := &v1alpha1.RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "empty-instance-types"},
		Spec:       v1alpha1.RafayNodeClassSpec{},
	}
	out := reconcileNodeClass(t, nc)
	cond := readyCondition(t, out)
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %s, want False for empty instanceTypes", cond.Status)
	}
	if cond.Reason != "ValidationFailed" {
		t.Errorf("Ready reason = %q, want %q", cond.Reason, "ValidationFailed")
	}
	if !strings.Contains(cond.Message, "at least one entry") {
		t.Errorf("Ready message = %q, want it to mention the empty instanceTypes list", cond.Message)
	}
}

func TestReconcileUnparseableCPUSetsReadyFalse(t *testing.T) {
	nc := &v1alpha1.RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-cpu"},
		Spec: v1alpha1.RafayNodeClassSpec{
			InstanceTypes: []v1alpha1.InstanceTypeSpec{
				{Name: "bad-sku", CPU: "four", Memory: "8Gi"},
			},
		},
	}
	out := reconcileNodeClass(t, nc)
	cond := readyCondition(t, out)
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition = %s, want False for unparseable cpu", cond.Status)
	}
	if cond.Reason != "ValidationFailed" {
		t.Errorf("Ready reason = %q, want %q", cond.Reason, "ValidationFailed")
	}
	if !strings.Contains(cond.Message, "invalid cpu") || !strings.Contains(cond.Message, "bad-sku") {
		t.Errorf("Ready message = %q, want it to name the bad instance type and its invalid cpu", cond.Message)
	}
}

// TestReconcileIsIdempotent re-reconciles an already-Ready NodeClass and verifies no error and
// an unchanged condition (the controller skips patching when nothing changed).
func TestReconcileIsIdempotent(t *testing.T) {
	nc := &v1alpha1.RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "idempotent"},
		Spec: v1alpha1.RafayNodeClassSpec{
			InstanceTypes: []v1alpha1.InstanceTypeSpec{
				{Name: "standard-4-8", CPU: "4", Memory: "8Gi"},
			},
		},
	}
	cl := fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithObjects(nc).
		WithStatusSubresource(&v1alpha1.RafayNodeClass{}).
		Build()
	c := NewController(cl)

	for i := 0; i < 2; i++ {
		var current v1alpha1.RafayNodeClass
		if err := cl.Get(context.Background(), client.ObjectKey{Name: nc.Name}, &current); err != nil {
			t.Fatalf("get RafayNodeClass: %v", err)
		}
		if _, err := c.Reconcile(context.Background(), &current); err != nil {
			t.Fatalf("Reconcile #%d: unexpected error: %v", i+1, err)
		}
	}

	var out v1alpha1.RafayNodeClass
	if err := cl.Get(context.Background(), client.ObjectKey{Name: nc.Name}, &out); err != nil {
		t.Fatalf("get RafayNodeClass: %v", err)
	}
	if cond := readyCondition(t, &out); cond.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %s after repeat reconcile, want True", cond.Status)
	}
}
