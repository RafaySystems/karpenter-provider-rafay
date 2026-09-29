/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package v1alpha1

// ms_crd_schema_test.go — the CRD in config/crd is hand-maintained (there is no controller-gen
// target, see the Makefile), so nothing else keeps it in step with the Go types and their
// kubebuilder markers. This test is that guard (R1-prov-misc-startup-7): every json field of
// RafayNodeClassSpec / InstanceTypeSpec must be a schema property (the spec schema is structural,
// so a missing property is pruned by the API server before the controller sees it), the schema
// must not list properties the Go type lacks, the +required markers must be the schema's
// `required` list, and the `categories=karpenter` marker must be in the CRD's names.

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

const msCRDPath = "../../../config/crd/karpenter.rafay.io_rafaynodeclasses.yaml"

// msJSONFields returns the json property names of a struct type, in declaration order.
func msJSONFields(t *testing.T, typ reflect.Type) []string {
	t.Helper()
	var out []string
	for i := 0; i < typ.NumField(); i++ {
		tag := typ.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			t.Fatalf("%s.%s has no json tag", typ.Name(), typ.Field(i).Name)
		}
		out = append(out, strings.Split(tag, ",")[0])
	}
	return out
}

func msLoadCRD(t *testing.T) map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(msCRDPath))
	if err != nil {
		t.Fatalf("read CRD: %v", err)
	}
	var crd map[string]interface{}
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parse CRD: %v", err)
	}
	return crd
}

// msDig walks nested maps/slices by key or index; it fails the test at the first missing step.
func msDig(t *testing.T, v interface{}, path ...interface{}) interface{} {
	t.Helper()
	for _, step := range path {
		switch s := step.(type) {
		case string:
			m, ok := v.(map[string]interface{})
			if !ok {
				t.Fatalf("CRD: expected a map at %v, got %T", step, v)
			}
			v, ok = m[s]
			if !ok {
				t.Fatalf("CRD: missing key %q (path %v)", s, path)
			}
		case int:
			l, ok := v.([]interface{})
			if !ok || s >= len(l) {
				t.Fatalf("CRD: expected a list with index %d, got %T", s, v)
			}
			v = l[s]
		}
	}
	return v
}

func msStrings(t *testing.T, v interface{}) []string {
	t.Helper()
	l, ok := v.([]interface{})
	if !ok {
		t.Fatalf("expected a list, got %T", v)
	}
	out := make([]string, 0, len(l))
	for _, e := range l {
		out = append(out, e.(string))
	}
	return out
}

func msKeys(t *testing.T, v interface{}) []string {
	t.Helper()
	m, ok := v.(map[string]interface{})
	if !ok {
		t.Fatalf("expected a map, got %T", v)
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestMsCRDSchemaMatchesGoTypes(t *testing.T) {
	crd := msLoadCRD(t)

	// +kubebuilder:resource:...categories=karpenter,shortName={rnc,rncs}
	if got := msStrings(t, msDig(t, crd, "spec", "names", "categories")); !reflect.DeepEqual(got, []string{"karpenter"}) {
		t.Errorf("names.categories = %v, want [karpenter] (the marker on RafayNodeClass; `kubectl get karpenter` relies on it)", got)
	}
	if got := msStrings(t, msDig(t, crd, "spec", "names", "shortNames")); !reflect.DeepEqual(got, []string{"rnc", "rncs"}) {
		t.Errorf("names.shortNames = %v, want [rnc rncs]", got)
	}

	schema := msDig(t, crd, "spec", "versions", 0, "schema", "openAPIV3Schema")

	// spec is structural: its properties must be exactly RafayNodeClassSpec's json fields.
	specProps := msKeys(t, msDig(t, schema, "properties", "spec", "properties"))
	wantSpec := msJSONFields(t, reflect.TypeOf(RafayNodeClassSpec{}))
	sort.Strings(wantSpec)
	if !reflect.DeepEqual(specProps, wantSpec) {
		t.Errorf("spec.properties = %v, want RafayNodeClassSpec's json fields %v", specProps, wantSpec)
	}

	// instanceTypes[] is structural too: same rule against InstanceTypeSpec.
	items := msDig(t, schema, "properties", "spec", "properties", "instanceTypes", "items")
	itemProps := msKeys(t, msDig(t, items, "properties"))
	wantItems := msJSONFields(t, reflect.TypeOf(InstanceTypeSpec{}))
	sort.Strings(wantItems)
	if !reflect.DeepEqual(itemProps, wantItems) {
		t.Errorf("instanceTypes.items.properties = %v, want InstanceTypeSpec's json fields %v (a field missing here is pruned by the API server)", itemProps, wantItems)
	}

	// +required markers on InstanceTypeSpec: name, cpu, memory.
	if got := msStrings(t, msDig(t, items, "required")); !reflect.DeepEqual(got, []string{"name", "cpu", "memory"}) {
		t.Errorf("instanceTypes.items.required = %v, want [name cpu memory] (the +required markers)", got)
	}

	// status must stay permissive (see the CRD header): the readiness controller writes conditions
	// through operatorpkg, and pruning one would break readiness rather than catch a user error.
	if v, ok := msDig(t, schema, "properties", "status").(map[string]interface{})["x-kubernetes-preserve-unknown-fields"]; ok != true || v != true {
		t.Errorf("status must carry x-kubernetes-preserve-unknown-fields: true, got %v", v)
	}
}
