/*
Copyright 2025 Rafay Systems.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package rafay

import "errors"

// ErrNodeNotFound is returned when a node is not found by lookup.
var ErrNodeNotFound = errors.New("rafay: node not found")

// ErrListNodesUnsupported is returned by BrokerClient; CloudProvider lists Kubernetes Nodes instead.
var ErrListNodesUnsupported = errors.New("rafay: ListNodes not implemented for this client")

// ErrGetNodeUnsupported is returned when GetNode has no remote API; CloudProvider falls back to kube.
var ErrGetNodeUnsupported = errors.New("rafay: GetNode not implemented for this client")
