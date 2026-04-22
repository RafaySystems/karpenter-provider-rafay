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

package rafay

import (
	"context"
)

// Client adds and removes nodes via edge-broker (BrokerClient).
type Client interface {
	// AddNodes requests Rafay to add one or more nodes to the cluster.
	// It returns the provider ID(s) of the node(s) being added (e.g. "rafay://cluster-id/node-id").
	// The node may not be registered in the cluster immediately; the returned provider ID
	// is used by Karpenter to track the NodeClaim until the node appears.
	// AddNodesRequest.OperationID is required by the broker.
	AddNodes(ctx context.Context, req AddNodesRequest) (*AddNodesResponse, error)

	// RemoveNode requests Rafay to remove the node identified by providerID from the cluster.
	// operationID is sent to the broker (required); use a stable id per delete (e.g. NodeClaim UID).
	// Returns nil when the node is removed or already gone; returns NodeNotFound when appropriate.
	RemoveNode(ctx context.Context, providerID, operationID string) error

	// GetNode returns info for the node with the given provider ID, or nil if not found.
	GetNode(ctx context.Context, providerID string) (*NodeInfo, error)

	// ListNodes returns all nodes in the cluster that are managed by this provider.
	ListNodes(ctx context.Context, clusterID string) ([]*NodeInfo, error)
}

// AddNodesRequest parameters for adding nodes via Rafay API.
type AddNodesRequest struct {
	ClusterID    string
	ProjectID    string
	InstanceType string
	Count        int
	// NodePoolName can be used by Rafay to target a specific node pool.
	NodePoolName string
	// OperationID is required: sent as KarpenterNodeStreamClientToBroker.operation_id (e.g. NodeClaim UID)
	// for idempotent adds and broker correlation across restarts.
	OperationID string
}

// AddNodesResponse returned after requesting node addition.
type AddNodesResponse struct {
	// ProviderIDs are the provider IDs of the requested nodes (e.g. rafay://cluster/node-id).
	// For a single-node request, one element; Rafay may support adding multiple in one call.
	ProviderIDs []string
}

// NodeInfo describes a node known to Rafay (for Get/List).
type NodeInfo struct {
	ProviderID string
	// Capacity can be set from Rafay if known; otherwise filled from instance type.
	Capacity map[string]string
}
