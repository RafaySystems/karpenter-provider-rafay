/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafay

import (
	"strings"
	"testing"
)

// TestRfParseProviderIDEdgeCases pins the parser's behaviour on inputs the platform should never
// produce but that reach it from node labels and NodeClaim status: whitespace, extra slashes,
// a doubled scheme, and a pending ID with no uid. ParseProviderID splits on the first slash only
// and trims nothing, so these are the exact contracts consumers (List/Delete/adoption) rely on.
func TestRfParseProviderIDEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		wantPool string
		wantRest string
		wantErr  bool
	}{
		{name: "leading whitespace is not trimmed", id: " rafay://pool/sku/host", wantErr: true},
		{name: "trailing whitespace stays in the remainder", id: "rafay://pool/sku/host ", wantPool: "pool", wantRest: "sku/host "},
		{name: "trailing slash keeps a non-empty remainder", id: "rafay://pool/sku/", wantPool: "pool", wantRest: "sku/"},
		{name: "remainder may itself start with a slash", id: "rafay://pool//host", wantPool: "pool", wantRest: "/host"},
		{name: "doubled scheme parses the second scheme as a pool", id: "rafay://rafay://pool/host", wantPool: "rafay:", wantRest: "/pool/host"},
		{name: "pending id without a uid is invalid", id: "rafay://pending/", wantErr: true},
		{name: "scheme with a single slash is invalid", id: "rafay:/pool/host", wantErr: true},
		{name: "many segments split on the first slash only", id: "rafay://a/b/c/d/e", wantPool: "a", wantRest: "b/c/d/e"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, rest, err := ParseProviderID(tc.id)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseProviderID(%q) = (%q, %q, nil), want error", tc.id, pool, rest)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseProviderID(%q): %v", tc.id, err)
			}
			if pool != tc.wantPool || rest != tc.wantRest {
				t.Errorf("ParseProviderID(%q) = (%q, %q), want (%q, %q)", tc.id, pool, rest, tc.wantPool, tc.wantRest)
			}
		})
	}
}

// TestRfBuildProviderIDDoesNotEscape pins that BuildProviderID concatenates verbatim: a segment
// containing a slash (or whitespace) is not rejected or escaped, so the built ID parses back with
// a different first segment. Callers must pass the platform's own label values unchanged.
func TestRfBuildProviderIDDoesNotEscape(t *testing.T) {
	tests := []struct {
		name         string
		pool, sku, h string
		want         string
		wantPool     string // what ParseProviderID reads back as the pool
	}{
		{name: "slash in pool name shifts the parsed pool", pool: "team/pool", sku: "sku", h: "host", want: "rafay://team/pool/sku/host", wantPool: "team"},
		{name: "whitespace is preserved", pool: " pool", sku: "sku", h: "host ", want: "rafay:// pool/sku/host ", wantPool: " pool"},
		{name: "prefix already on the pool is not stripped", pool: "rafay://pool", sku: "sku", h: "host", want: "rafay://rafay://pool/sku/host", wantPool: "rafay:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildProviderID(tc.pool, tc.sku, tc.h)
			if got != tc.want {
				t.Fatalf("BuildProviderID = %q, want %q", got, tc.want)
			}
			if !strings.HasPrefix(got, ProviderIDPrefix) {
				t.Fatalf("built ID %q lacks the %q prefix", got, ProviderIDPrefix)
			}
			pool, _, err := ParseProviderID(got)
			if err != nil {
				t.Fatalf("ParseProviderID(%q): %v", got, err)
			}
			if pool != tc.wantPool {
				t.Errorf("parsed pool = %q, want %q", pool, tc.wantPool)
			}
		})
	}
}
