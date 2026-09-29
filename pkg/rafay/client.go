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

// Client looks up nodes via edge-broker (BrokerClient). Node add/remove go through the
// NodeBatcher (Enqueue/EnqueueRemove), not through this interface.
type Client interface {
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
	// NodePoolName can be used by Rafay to target a specific node pool.
	NodePoolName string
	// OperationID is required: sent as KarpenterBatchNodeAddItem.operation_id (e.g. NodeClaim UID)
	// for idempotent adds and broker correlation across restarts.
	OperationID string
}

// RemoveNodesRequest parameters for removing a node via Rafay API. The platform's worker-node
// catalog is declarative (per pool+SKU counts), so removal is a count decrement targeted by
// InstanceType + NodePoolName; ProviderID is carried for future targeted removal.
type RemoveNodesRequest struct {
	ClusterID    string
	ProjectID    string
	InstanceType string
	// NodePoolName identifies the node pool the node belongs to.
	NodePoolName string
	// ProviderID identifies the node to remove, in the platform's format
	// rafay://<nodepoolname>/<sku_name>/<hostname> (see ParseProviderID — the first segment is the
	// node pool, never a cluster ID). Empty when the machine has not registered yet (the add
	// succeeded but no Node carries an ID): the removal is then untargeted. Brokers so far ignore
	// it and let the catalog decide which machine is retired.
	ProviderID string
}

// NodeInfo describes a node known to Rafay (for Get/List).
type NodeInfo struct {
	ProviderID string
	// Capacity is the node's reported capacity, as resource.Quantity strings keyed by resource
	// name. The broker client has no node API, so CloudProvider fills it from the Kubernetes
	// Node's status.capacity (kubeNodeToNodeInfo); nothing falls back to the instance type — a
	// NodeInfo with no capacity yields a NodeClaim with an empty status.capacity.
	Capacity map[string]string
}
