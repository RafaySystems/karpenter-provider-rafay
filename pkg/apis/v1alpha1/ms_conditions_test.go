/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package v1alpha1

import (
	"testing"

	"github.com/awslabs/operatorpkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func msNodeClass() *RafayNodeClass {
	return &RafayNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "nc", Generation: 3},
		Spec: RafayNodeClassSpec{InstanceTypes: []InstanceTypeSpec{{
			Name:             "gpu-h100",
			CPU:              "64",
			Memory:           "1024Gi",
			GPU:              "8",
			Zone:             "zone-a",
			Architectures:    []string{"amd64"},
			OperatingSystems: []string{"linux"},
		}}},
		Status: RafayNodeClassStatus{Conditions: []status.Condition{{
			Type: status.ConditionReady, Status: metav1.ConditionTrue, Reason: status.ConditionReady,
		}}},
	}
}

// StatusConditions writes through SetConditions and reads back through GetConditions, which is
// what lets operatorpkg (and Karpenter's nodepool readiness) treat RafayNodeClass as a
// status.Object.
func TestMsStatusConditionsRoundTrip(t *testing.T) {
	nc := &RafayNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "nc", Generation: 2}}
	if got := nc.GetConditions(); len(got) != 0 {
		t.Fatalf("fresh object has conditions: %+v", got)
	}
	// Ready is the root condition: operatorpkg initializes it to Unknown/AwaitingReconciliation
	// on first read and stores that through SetConditions, so even a Get mutates the object.
	if got := nc.StatusConditions().Get(status.ConditionReady); got == nil || got.Status != metav1.ConditionUnknown || got.Reason != "AwaitingReconciliation" {
		t.Fatalf("unset Ready must read as an initialized Unknown, got %+v", got)
	}
	if stored := nc.GetConditions(); len(stored) != 1 || stored[0].Status != metav1.ConditionUnknown {
		t.Fatalf("reading the root condition initializes it on the object: %+v", stored)
	}

	if !nc.StatusConditions().SetTrue(status.ConditionReady) {
		t.Fatal("first SetTrue must report a change")
	}
	got := nc.GetConditions()
	if len(got) != 1 || got[0].Type != status.ConditionReady || got[0].Status != metav1.ConditionTrue {
		t.Fatalf("after SetTrue: %+v", got)
	}
	if got[0].ObservedGeneration != 2 {
		t.Errorf("ObservedGeneration should track metadata.generation, got %d", got[0].ObservedGeneration)
	}
	if nc.StatusConditions().SetTrue(status.ConditionReady) {
		t.Error("repeating SetTrue must report no change")
	}
	if !nc.StatusConditions().IsTrue(status.ConditionReady) {
		t.Error("IsTrue(Ready) after SetTrue")
	}

	if !nc.StatusConditions().SetFalse(status.ConditionReady, "ValidationFailed", "bad cpu") {
		t.Fatal("flipping to False must report a change")
	}
	ready := nc.StatusConditions().Get(status.ConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "ValidationFailed" || ready.Message != "bad cpu" {
		t.Fatalf("after SetFalse: %+v", ready)
	}
	if nc.StatusConditions().SetFalse(status.ConditionReady, "ValidationFailed", "bad cpu") {
		t.Error("repeating an identical SetFalse must report no change")
	}
	if !nc.StatusConditions().SetFalse(status.ConditionReady, "ValidationFailed", "bad memory") {
		t.Error("a new message is a change")
	}

	// SetConditions replaces wholesale and Get sees the replacement.
	nc.SetConditions(nil)
	if len(nc.GetConditions()) != 0 {
		t.Errorf("SetConditions(nil) must clear: %+v", nc.GetConditions())
	}
	if got := nc.StatusConditions().Get(status.ConditionReady); got == nil || got.Status != metav1.ConditionUnknown {
		t.Errorf("after clearing, Ready is re-initialized to Unknown on read, got %+v", got)
	}
	if !nc.StatusConditions().SetTrue(status.ConditionReady) {
		t.Error("Unknown -> True must report a change")
	}
	external := []status.Condition{{Type: status.ConditionReady, Status: metav1.ConditionUnknown, Reason: "Pending"}}
	nc.SetConditions(external)
	if got := nc.StatusConditions().Get(status.ConditionReady); got == nil || got.Status != metav1.ConditionUnknown {
		t.Errorf("conditions set externally must be visible through StatusConditions: %+v", got)
	}
}

// The hand-written deepcopy must not alias slices: mutating a copy must leave the original
// untouched, or a controller's stored/desired diff (client.MergeFrom) silently becomes empty.
func TestMsDeepCopyDoesNotAlias(t *testing.T) {
	orig := msNodeClass()
	cp := orig.DeepCopy()

	cp.Spec.InstanceTypes[0].Architectures[0] = "arm64"
	cp.Spec.InstanceTypes[0].OperatingSystems = append(cp.Spec.InstanceTypes[0].OperatingSystems, "windows")
	cp.Spec.InstanceTypes[0].CPU = "1"
	cp.Status.Conditions[0].Status = metav1.ConditionFalse
	cp.Status.Conditions = append(cp.Status.Conditions, status.Condition{Type: "Extra"})
	cp.Labels = map[string]string{"edited": "yes"}

	it := orig.Spec.InstanceTypes[0]
	if it.Architectures[0] != "amd64" || len(it.OperatingSystems) != 1 || it.CPU != "64" {
		t.Errorf("original spec mutated through the copy: %+v", it)
	}
	if len(orig.Status.Conditions) != 1 || orig.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("original status mutated through the copy: %+v", orig.Status.Conditions)
	}
	if orig.Labels != nil {
		t.Errorf("original metadata mutated through the copy: %v", orig.Labels)
	}

	// Empty-but-non-nil slices stay distinguishable from nil (matters for omitempty diffs).
	empty := &InstanceTypeSpec{Name: "e", Architectures: []string{}}
	if c := empty.DeepCopy(); c.Architectures == nil || c.OperatingSystems != nil {
		t.Errorf("nil-ness not preserved: %+v", c)
	}

	var nilSpec *InstanceTypeSpec
	if nilSpec.DeepCopy() != nil {
		t.Error("DeepCopy of a nil InstanceTypeSpec must be nil")
	}
	var nilNC *RafayNodeClass
	if nilNC.DeepCopy() != nil || nilNC.DeepCopyObject() != nil {
		t.Error("DeepCopy/DeepCopyObject of a nil RafayNodeClass must be nil")
	}
	var nilList *RafayNodeClassList
	if nilList.DeepCopy() != nil || nilList.DeepCopyObject() != nil {
		t.Error("DeepCopy/DeepCopyObject of a nil RafayNodeClassList must be nil")
	}
}

func TestMsListDeepCopyObject(t *testing.T) {
	list := &RafayNodeClassList{Items: []RafayNodeClass{*msNodeClass()}}
	var obj runtime.Object = list.DeepCopyObject()
	cp, ok := obj.(*RafayNodeClassList)
	if !ok {
		t.Fatalf("DeepCopyObject returned %T", obj)
	}
	cp.Items[0].Spec.InstanceTypes[0].Architectures[0] = "arm64"
	cp.Items = append(cp.Items, RafayNodeClass{})
	if len(list.Items) != 1 || list.Items[0].Spec.InstanceTypes[0].Architectures[0] != "amd64" {
		t.Errorf("list copy aliases the original: %+v", list.Items)
	}
}

// The scheme registration must expose both the object and its list under the karpenter.rafay.io
// group; the fake client, the CRD and the informer cache all rely on it.
func TestMsAddToScheme(t *testing.T) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	for _, kind := range []string{"RafayNodeClass", "RafayNodeClassList"} {
		if !s.Recognizes(SchemeGroupVersion.WithKind(kind)) {
			t.Errorf("%s not registered under %s", kind, SchemeGroupVersion)
		}
	}
	if SchemeGroupVersion.Group != Group || Group != "karpenter.rafay.io" {
		t.Errorf("group: %q", SchemeGroupVersion.Group)
	}
}
