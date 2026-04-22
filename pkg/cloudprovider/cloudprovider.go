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

package cloudprovider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// CloudProvider implements Karpenter's cloudprovider.CloudProvider by calling Rafay APIs to add/remove nodes (private cloud).
type CloudProvider struct {
	kubeClient client.Client
	client     rafay.Client
	clusterID  string
	projectID  string
	mu         sync.Mutex
}

// NewCloudProvider returns a Rafay cloud provider that uses the given Rafay client.
func NewCloudProvider(kubeClient client.Client, rafayClient rafay.Client, clusterID, projectID string) *CloudProvider {
	return &CloudProvider{
		kubeClient: kubeClient,
		client:     rafayClient,
		clusterID:  clusterID,
		projectID:  projectID,
	}
}

// Create requests Rafay to add a node via its API and returns the NodeClaim with provider ID set.
func (c *CloudProvider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if nodeClaim == nil {
		return nil, fmt.Errorf("nodeclaim is nil")
	}

	nodeClass, err := c.resolveNodeClassFromNodeClaim(ctx, nodeClaim)
	if err != nil {
		return nil, fmt.Errorf("resolve NodeClass: %w", err)
	}

	instanceTypes, err := c.getInstanceTypes(ctx, nodeClass)
	if err != nil {
		return nil, fmt.Errorf("get instance types: %w", err)
	}

	compatible := filterCompatibleInstanceTypes(instanceTypes, nodeClaim)
	if len(compatible) == 0 {
		return nil, cloudprovider.NewInsufficientCapacityError(fmt.Errorf("no compatible instance type for requirements"))
	}
	// Pick first compatible (could add preference by cost/size).
	selected := compatible[0]

	clusterID := c.clusterID
	if nodeClass.Spec.ClusterID != "" {
		clusterID = nodeClass.Spec.ClusterID
	}
	projectID := c.projectID
	if nodeClass.Spec.ProjectID != "" {
		projectID = nodeClass.Spec.ProjectID
	}

	resp, err := c.client.AddNodes(ctx, rafay.AddNodesRequest{
		ClusterID:    clusterID,
		ProjectID:    projectID,
		InstanceType: selected.Name,
		Count:        1,
		NodePoolName: nodeClaim.Labels[karpv1.NodePoolLabelKey],
		OperationID:  string(nodeClaim.UID),
	})
	if err != nil {
		return nil, cloudprovider.NewCreateError(err, "AddNodesFailed", err.Error())
	}
	if len(resp.ProviderIDs) == 0 {
		return nil, cloudprovider.NewCreateError(fmt.Errorf("no provider ID returned"), "AddNodesFailed", "Rafay API returned no provider ID")
	}
	providerID := resp.ProviderIDs[0]

	// Hydrate NodeClaim with resolved labels and capacity.
	out := nodeClaim.DeepCopy()
	out.Status.ProviderID = providerID
	out.Status.Capacity = selected.Capacity
	out.Labels = lo.Assign(out.Labels, requirementsToLabels(selected.Requirements))
	return out, nil
}

// Delete asks Rafay to remove the node.
func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *karpv1.NodeClaim) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if nodeClaim == nil || nodeClaim.Status.ProviderID == "" {
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("nodeclaim has no provider ID"))
	}
	err := c.client.RemoveNode(ctx, nodeClaim.Status.ProviderID, string(nodeClaim.UID))
	if err != nil {
		if errors.Is(err, rafay.ErrNodeNotFound) {
			return cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return err
	}
	return nil
}

// Get returns the NodeClaim for the given provider ID.
func (c *CloudProvider) Get(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	if providerID == "" {
		return nil, fmt.Errorf("providerID is empty")
	}
	info, err := c.client.GetNode(ctx, providerID)
	if err != nil {
		if errors.Is(err, rafay.ErrGetNodeUnsupported) {
			return c.getNodeFromKube(ctx, providerID)
		}
		if errors.Is(err, rafay.ErrNodeNotFound) {
			return nil, cloudprovider.NewNodeClaimNotFoundError(err)
		}
		return nil, err
	}
	return nodeInfoToNodeClaim(info), nil
}

// List returns all NodeClaims known to Rafay for the cluster.
func (c *CloudProvider) List(ctx context.Context) ([]*karpv1.NodeClaim, error) {
	nodes, err := c.client.ListNodes(ctx, c.clusterID)
	if err != nil {
		if errors.Is(err, rafay.ErrListNodesUnsupported) {
			return c.listNodesFromKube(ctx)
		}
		return nil, err
	}
	return lo.Map(nodes, func(n *rafay.NodeInfo, _ int) *karpv1.NodeClaim {
		return nodeInfoToNodeClaim(n)
	}), nil
}

const rafayProviderPrefix = "rafay://"

// listNodesFromKube builds a synthetic cloud list from spec.providerID on Nodes (broker mode has no list API).
func (c *CloudProvider) listNodesFromKube(ctx context.Context) ([]*karpv1.NodeClaim, error) {
	var list corev1.NodeList
	if err := c.kubeClient.List(ctx, &list); err != nil {
		return nil, err
	}
	out := make([]*karpv1.NodeClaim, 0)
	for i := range list.Items {
		pid := list.Items[i].Spec.ProviderID
		if pid == "" || !strings.HasPrefix(pid, rafayProviderPrefix) {
			continue
		}
		if c.clusterID != "" {
			cid, _, err := rafay.ParseProviderID(pid)
			if err != nil || cid != c.clusterID {
				continue
			}
		}
		info := kubeNodeToNodeInfo(&list.Items[i])
		out = append(out, nodeInfoToNodeClaim(info))
	}
	return out, nil
}

func (c *CloudProvider) getNodeFromKube(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	var list corev1.NodeList
	if err := c.kubeClient.List(ctx, &list); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Spec.ProviderID == providerID {
			return nodeInfoToNodeClaim(kubeNodeToNodeInfo(&list.Items[i])), nil
		}
	}
	return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("no node with provider ID %q", providerID))
}

func kubeNodeToNodeInfo(n *corev1.Node) *rafay.NodeInfo {
	cap := make(map[string]string)
	for k, v := range n.Status.Capacity {
		cap[string(k)] = v.String()
	}
	return &rafay.NodeInfo{ProviderID: n.Spec.ProviderID, Capacity: cap}
}

// GetInstanceTypes returns instance types from the NodeClass or defaults for private cloud.
func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *karpv1.NodePool) ([]*cloudprovider.InstanceType, error) {
	if nodePool == nil {
		return nil, fmt.Errorf("node pool is nil")
	}
	nodeClass, err := c.resolveNodeClassFromNodePool(ctx, nodePool)
	if err != nil {
		return nil, err
	}
	return c.getInstanceTypes(ctx, nodeClass)
}

func (c *CloudProvider) getInstanceTypes(ctx context.Context, nodeClass *v1alpha1.RafayNodeClass) ([]*cloudprovider.InstanceType, error) {
	if len(nodeClass.Spec.InstanceTypes) > 0 {
		return rafayInstanceTypesToKarpenter(nodeClass.Spec.InstanceTypes), nil
	}
	// Default instance types for private cloud when none specified.
	return defaultInstanceTypes(), nil
}

// IsDrifted reports no drift (Rafay manages node lifecycle).
func (c *CloudProvider) IsDrifted(ctx context.Context, nodeClaim *karpv1.NodeClaim) (cloudprovider.DriftReason, error) {
	return "", nil
}

// RepairPolicies returns no custom repair policies.
func (c *CloudProvider) RepairPolicies() []cloudprovider.RepairPolicy {
	return nil
}

// Name returns the provider name.
func (c *CloudProvider) Name() string {
	return "rafay"
}

// GetSupportedNodeClasses returns the Rafay NodeClass.
func (c *CloudProvider) GetSupportedNodeClasses() []status.Object {
	return []status.Object{&v1alpha1.RafayNodeClass{}}
}

func (c *CloudProvider) resolveNodeClassFromNodeClaim(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*v1alpha1.RafayNodeClass, error) {
	if nodeClaim.Spec.NodeClassRef == nil {
		return nil, fmt.Errorf("nodeclaim %q has no nodeClassRef", nodeClaim.Name)
	}
	key := client.ObjectKey{Name: nodeClaim.Spec.NodeClassRef.Name}
	var nc v1alpha1.RafayNodeClass
	if err := c.kubeClient.Get(ctx, key, &nc); err != nil {
		return nil, fmt.Errorf("get RafayNodeClass %q: %w", nodeClaim.Spec.NodeClassRef.Name, err)
	}
	return &nc, nil
}

func (c *CloudProvider) resolveNodeClassFromNodePool(ctx context.Context, nodePool *karpv1.NodePool) (*v1alpha1.RafayNodeClass, error) {
	if nodePool.Spec.Template.Spec.NodeClassRef == nil {
		return nil, fmt.Errorf("node pool %q has no nodeClassRef", nodePool.Name)
	}
	key := client.ObjectKey{Name: nodePool.Spec.Template.Spec.NodeClassRef.Name}
	var nc v1alpha1.RafayNodeClass
	if err := c.kubeClient.Get(ctx, key, &nc); err != nil {
		return nil, fmt.Errorf("get RafayNodeClass %q: %w", nodePool.Spec.Template.Spec.NodeClassRef.Name, err)
	}
	return &nc, nil
}

func nodeInfoToNodeClaim(n *rafay.NodeInfo) *karpv1.NodeClaim {
	nc := &karpv1.NodeClaim{}
	nc.Status.ProviderID = n.ProviderID
	if len(n.Capacity) > 0 {
		nc.Status.Capacity = make(corev1.ResourceList)
		for k, v := range n.Capacity {
			q, err := resource.ParseQuantity(v)
			if err == nil {
				nc.Status.Capacity[corev1.ResourceName(k)] = q
			}
		}
	}
	return nc
}

func rafayInstanceTypesToKarpenter(specs []v1alpha1.InstanceTypeSpec) []*cloudprovider.InstanceType {
	out := make([]*cloudprovider.InstanceType, 0, len(specs))
	for _, s := range specs {
		cpu := resource.MustParse(s.CPU)
		mem := resource.MustParse(s.Memory)
		capacity := corev1.ResourceList{
			corev1.ResourceCPU:    cpu,
			corev1.ResourceMemory: mem,
			corev1.ResourcePods:   resource.MustParse("110"),
		}
		zone := s.Zone
		if zone == "" {
			zone = "default"
		}
		reqs := scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, s.Name),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
			scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
		)
		offerings := cloudprovider.Offerings{
			{
				Requirements: reqs,
				Price:        0,
				Available:    true,
			},
		}
		out = append(out, &cloudprovider.InstanceType{
			Name:         s.Name,
			Requirements: reqs,
			Offerings:    offerings,
			Capacity:     capacity,
			Overhead:     &cloudprovider.InstanceTypeOverhead{},
		})
	}
	return out
}

func defaultInstanceTypes() []*cloudprovider.InstanceType {
	specs := []v1alpha1.InstanceTypeSpec{
		{Name: "standard-2-4", CPU: "2", Memory: "4Gi", Zone: "default"},
		{Name: "standard-4-8", CPU: "4", Memory: "8Gi", Zone: "default"},
		{Name: "standard-8-16", CPU: "8", Memory: "16Gi", Zone: "default"},
		{Name: "standard-16-32", CPU: "16", Memory: "32Gi", Zone: "default"},
		{Name: "standard-32-64", CPU: "32", Memory: "64Gi", Zone: "default"},
		{Name: "standard-48-96", CPU: "48", Memory: "96Gi", Zone: "default"},
	}
	return rafayInstanceTypesToKarpenter(specs)
}

// requirementsToLabels converts scheduling requirements to a label map (In-operator values only).
func requirementsToLabels(reqs scheduling.Requirements) map[string]string {
	labels := make(map[string]string)
	for _, r := range reqs.Values() {
		if r != nil && r.Operator() == corev1.NodeSelectorOpIn {
			vals := r.Values()
			if len(vals) > 0 {
				labels[r.Key] = vals[0]
			}
		}
	}
	return labels
}

func filterCompatibleInstanceTypes(instanceTypes []*cloudprovider.InstanceType, nodeClaim *karpv1.NodeClaim) []*cloudprovider.InstanceType {
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(nodeClaim.Spec.Requirements...)
	return lo.Filter(instanceTypes, func(it *cloudprovider.InstanceType, _ int) bool {
		return reqs.Compatible(it.Requirements, scheduling.AllowUndefinedWellKnownLabels) == nil &&
			resources.Fits(nodeClaim.Spec.Resources.Requests, it.Allocatable())
	})
}
