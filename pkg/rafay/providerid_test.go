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

func TestBuildProviderID(t *testing.T) {
	tests := []struct {
		name     string
		pool     string
		sku      string
		hostname string
		want     string
	}{
		{
			// The ID the platform itself writes for a node in a worker pool. Adoption depends on
			// reproducing it byte for byte, since spec.providerID cannot be corrected once set.
			name: "platform format", pool: "pool1", sku: "oci-inst", hostname: "test-auto-2-e-e949w9-w0-af1cc",
			want: "rafay://pool1/oci-inst/test-auto-2-e-e949w9-w0-af1cc",
		},
		{name: "empty pool", sku: "oci-inst", hostname: "host-a"},
		{name: "empty sku", pool: "pool1", hostname: "host-a"},
		{name: "empty hostname", pool: "pool1", sku: "oci-inst"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildProviderID(tt.pool, tt.sku, tt.hostname)
			if got != tt.want {
				t.Fatalf("BuildProviderID(%q, %q, %q) = %q, want %q", tt.pool, tt.sku, tt.hostname, got, tt.want)
			}
			if tt.want == "" {
				return
			}
			// A built ID must round-trip: every consumer reads it back through ParseProviderID.
			pool, rest, err := ParseProviderID(got)
			if err != nil {
				t.Fatalf("ParseProviderID(%q) unexpected error: %v", got, err)
			}
			if pool != tt.pool || rest != tt.sku+"/"+tt.hostname {
				t.Errorf("ParseProviderID(%q) = (%q, %q), want (%q, %q)", got, pool, rest, tt.pool, tt.sku+"/"+tt.hostname)
			}
		})
	}
}

func TestParseProviderID(t *testing.T) {
	tests := []struct {
		name             string
		providerID       string
		wantNodePoolName string
		wantRest         string
		wantErr          bool
	}{
		{
			// The real, platform-stamped format: rafay://<nodepoolname>/<sku_name>/<hostname>.
			name:             "real platform provider ID",
			providerID:       "rafay://worker-pool-amd/oci-inst/host-w1-e6a5c",
			wantNodePoolName: "worker-pool-amd",
			wantRest:         "oci-inst/host-w1-e6a5c",
		},
		{
			name:             "two-segment provider ID",
			providerID:       "rafay://worker-pool-amd/host-w1-e6a5c",
			wantNodePoolName: "worker-pool-amd",
			wantRest:         "host-w1-e6a5c",
		},
		{
			// The synthetic pending ProviderID ("rafay://pending/<uid>") set by Create()
			// is structurally valid: it parses with "pending" as the first segment. Callers
			// that must distinguish pending IDs check the prefix, not the parse result.
			name:             "pending prefix parses with pending as first segment",
			providerID:       "rafay://pending/0e8f9c1d-uid",
			wantNodePoolName: "pending",
			wantRest:         "0e8f9c1d-uid",
		},
		{
			name:       "wrong prefix",
			providerID: "aws://worker-pool-amd/host-w1-e6a5c",
			wantErr:    true,
		},
		{
			name:       "prefix is case-sensitive",
			providerID: "RAFAY://worker-pool-amd/host-w1-e6a5c",
			wantErr:    true,
		},
		{
			name:       "empty string",
			providerID: "",
			wantErr:    true,
		},
		{
			name:       "prefix only",
			providerID: "rafay://",
			wantErr:    true,
		},
		{
			name:       "missing separator after nodepool name",
			providerID: "rafay://worker-pool-amd",
			wantErr:    true,
		},
		{
			name:       "empty nodepool name",
			providerID: "rafay:///host-w1-e6a5c",
			wantErr:    true,
		},
		{
			name:       "empty remainder",
			providerID: "rafay://worker-pool-amd/",
			wantErr:    true,
		},
		{
			name:       "bare path without scheme",
			providerID: "worker-pool-amd/host-w1-e6a5c",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodePoolName, rest, err := ParseProviderID(tt.providerID)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseProviderID(%q) = (%q, %q, nil), want error", tt.providerID, nodePoolName, rest)
				}
				if !strings.Contains(err.Error(), "invalid rafay provider ID") {
					t.Errorf("ParseProviderID(%q) error = %q, want it to contain %q", tt.providerID, err.Error(), "invalid rafay provider ID")
				}
				if nodePoolName != "" || rest != "" {
					t.Errorf("ParseProviderID(%q) returned non-empty components (%q, %q) with error", tt.providerID, nodePoolName, rest)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseProviderID(%q) unexpected error: %v", tt.providerID, err)
			}
			if nodePoolName != tt.wantNodePoolName {
				t.Errorf("ParseProviderID(%q) nodePoolName = %q, want %q", tt.providerID, nodePoolName, tt.wantNodePoolName)
			}
			if rest != tt.wantRest {
				t.Errorf("ParseProviderID(%q) rest = %q, want %q", tt.providerID, rest, tt.wantRest)
			}
		})
	}
}
