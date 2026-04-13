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

// ParseProviderID splits rafay://clusterID/nodeID into components.
func ParseProviderID(providerID string) (clusterID, nodeID string, err error) {
	const prefix = "rafay://"
	if !strings.HasPrefix(providerID, prefix) {
		return "", "", fmt.Errorf("invalid rafay provider ID %q", providerID)
	}
	rest := strings.TrimPrefix(providerID, prefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid rafay provider ID %q", providerID)
	}
	return parts[0], parts[1], nil
}
