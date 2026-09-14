/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafay

import (
	"fmt"
	"strings"
)

// ProviderIDPrefix is the scheme every Rafay provider ID carries.
const ProviderIDPrefix = "rafay://"

// BuildProviderID renders the provider ID the Rafay platform stamps on a worker node:
//
//	rafay://<nodepoolname>/<sku_name>/<hostname>
//
// It exists for node adoption. A node the platform provisioned before this cluster ran Karpenter
// carries the nodepoolname/sku_name labels but may have an empty spec.providerID, and the provider
// ID is the only key that ties a Node to a NodeClaim (and to List()/Delete()) — so a node with an
// empty one has to be given the ID the platform would have written.
//
// An empty segment returns "": such an ID does not round-trip through ParseProviderID, and leaving
// a node unmanaged is better than binding a NodeClaim to an ID nothing else will ever produce.
func BuildProviderID(nodePoolName, skuName, hostname string) string {
	if nodePoolName == "" || skuName == "" || hostname == "" {
		return ""
	}
	return ProviderIDPrefix + nodePoolName + "/" + skuName + "/" + hostname
}

// ParseProviderID splits a Rafay provider ID into its first path segment and the remainder.
//
// The real, platform-stamped format is:
//
//	rafay://<nodepoolname>/<sku_name>/<hostname>
//	e.g. rafay://worker-pool-amd/oci-inst/host-w1-e6a5c
//
// so the first segment is the NODE POOL NAME (not a cluster ID, as an earlier version of this
// doc claimed) and rest carries "<sku_name>/<hostname>".
//
// CloudProvider.Create() also stamps a synthetic pending ID of the form
// "rafay://pending/<nodeclaim-uid>", which parses structurally with nodePoolName == "pending".
// Callers that must distinguish pending IDs check the PendingProviderIDPrefix rather than the
// parse result.
func ParseProviderID(providerID string) (nodePoolName, rest string, err error) {
	if !strings.HasPrefix(providerID, ProviderIDPrefix) {
		return "", "", fmt.Errorf("invalid rafay provider ID %q", providerID)
	}
	trimmed := strings.TrimPrefix(providerID, ProviderIDPrefix)
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid rafay provider ID %q", providerID)
	}
	return parts[0], parts[1], nil
}
