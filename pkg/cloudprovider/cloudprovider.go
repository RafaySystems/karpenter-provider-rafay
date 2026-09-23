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
	"sort"
	"strings"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const (
	rafayProviderPrefix = "rafay://"

	// PendingProviderIDPrefix marks a NodeClaim whose real node ProviderID has not yet been
	// resolved. The NodeProviderIDController watches for new nodes and patches the real value.
	PendingProviderIDPrefix = "rafay://pending/"

	// AdoptedProviderIDAnnotationKey carries the ProviderID of a node that already existed in the
	// cluster when its NodeClaim was created. The NodeAdoptionController stamps it on the
	// NodeClaims it creates for pre-existing worker nodes, and Create() treats it as "this machine
	// is already running": it returns that ProviderID instead of asking the broker for a node.
	//
	// The annotation — rather than a status pre-patch by the adoption controller — is what makes
	// adoption safe. Karpenter's lifecycle controller calls Create() for every NodeClaim whose
	// Launched condition is not yet True, so a NodeClaim created without this marker would
	// provision a *second* machine for a node the cluster already has. Being on the object at
	// creation time, the marker is visible on the very first Launch reconcile and survives
	// controller restarts.
	AdoptedProviderIDAnnotationKey = "karpenter.rafay.io/adopted-provider-id"

	// BatchIDAnnotationKey records the broker batch a NodeClaim's add was sent in. Create()
	// stamps it at ACK and Karpenter merges it onto the stored NodeClaim (lifecycle
	// PopulateNodeClaimDetails copies returned annotations). The batchresume controller reads it
	// on startup so a restarted provider resumes polling that batch — otherwise the batch ID
	// lives only in the old process's memory, and the failure signal for a pending add is lost
	// until Karpenter's 60-minute registration timeout.
	BatchIDAnnotationKey = "karpenter.rafay.io/batch-id"

	nodepoolNameLabel = "nodepoolname"
	skuNameLabel      = "sku_name"

	// temporaryGPUResourceName is the extended resource InstanceTypeSpec.GPU is advertised under.
	// TEMPORARY, and hardcoded on purpose — see InstanceTypeSpec.GPU. Real support carries the
	// per-SKU name the broker already resolves (amd.com/gpu on AMD shapes) instead of assuming
	// NVIDIA for every SKU.
	temporaryGPUResourceName = corev1.ResourceName("nvidia.com/gpu")
)

// CloudProvider implements Karpenter's cloudprovider.CloudProvider by calling Rafay APIs to add/remove nodes (private cloud).
type CloudProvider struct {
	kubeClient client.Client
	// apiReader reads directly from the API server (no cache). Used where a stale cache could
	// cause a wrong decision, e.g. matching a freshly joined node to a pending NodeClaim.
	apiReader client.Reader
	client    rafay.Client
	clusterID string
	projectID string
	batcher   *rafay.NodeBatcher
}

// NewCloudProvider returns a Rafay cloud provider that uses the given Rafay client.
// The batcher must already be started (call BrokerClient.StartBatcher) before Create is called.
func NewCloudProvider(kubeClient client.Client, apiReader client.Reader, rafayClient rafay.Client, clusterID, projectID string, batcher *rafay.NodeBatcher) *CloudProvider {
	return &CloudProvider{
		kubeClient: kubeClient,
		apiReader:  apiReader,
		client:     rafayClient,
		clusterID:  clusterID,
		projectID:  projectID,
		batcher:    batcher,
	}
}

// Create enqueues a node-add request with the NodeBatcher and returns as soon as the broker
// acknowledges the request. A synthetic pending ProviderID is set on the returned NodeClaim;
// the NodeProviderIDController will patch it with the real value once the node joins.
//
// A NodeClaim carrying AdoptedProviderIDAnnotationKey is the exception: its machine is already
// running and registered in the cluster, so no broker call is made and the annotated ProviderID is
// returned directly. See the annotation's doc for why the marker has to be on the object.
func (c *CloudProvider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
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
	// Smallest fit first: pick the cheapest compatible offering (synthetic price, see
	// rafayInstanceTypesToKarpenter). Stable sort keeps NodeClass spec order on ties.
	sort.SliceStable(compatible, func(i, j int) bool {
		return cheapestOffering(compatible[i]) < cheapestOffering(compatible[j])
	})
	selected := compatible[0]

	// Adopted NodeClaim: the machine already exists, so skip the broker entirely and report it as
	// launched. Capacity comes from the selected instance type exactly as on the provisioning path;
	// the node's real capacity takes over in cluster state once the NodeClaim is initialized.
	if adoptedID := nodeClaim.Annotations[AdoptedProviderIDAnnotationKey]; adoptedID != "" {
		out := nodeClaim.DeepCopy()
		out.Status.ProviderID = adoptedID
		out.Status.Capacity = selected.Capacity
		out.Status.Allocatable = selected.Allocatable()
		// Labels are deliberately NOT overlaid from the instance type here, unlike the provisioning
		// path below. The adoption controller derived this NodeClaim's well-known labels from the
		// node itself, and Karpenter merges whatever Create() returns into the stored NodeClaim —
		// from where the Registration reconciler copies them onto the Node. A label the SKU implies
		// but the node does not carry (topology zone, most notably: a RafayNodeClass with no zone
		// yields "default") would therefore overwrite the real value on a node already running
		// workloads, breaking topology-aware scheduling for the pods on it.
		klog.Infof("Create: nodeclaim %s adopts existing node providerID=%s (no broker call)", nodeClaim.Name, adoptedID)
		return out, nil
	}

	req := rafay.AddNodesRequest{
		ClusterID:    c.clusterID,
		ProjectID:    c.projectID,
		InstanceType: selected.Name,
		NodePoolName: nodeClaim.Labels[karpv1.NodePoolLabelKey],
		OperationID:  string(nodeClaim.UID),
	}

	resultCh := c.batcher.Enqueue(string(nodeClaim.UID), req)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, cloudprovider.NewCreateError(result.Err, "AddNodesFailed", result.Err.Error())
		}

		// Return immediately with a synthetic ProviderID. The NodeProviderIDController will
		// resolve and patch the real ProviderID once the node joins the cluster (~12 min).
		out := nodeClaim.DeepCopy()
		out.Status.ProviderID = PendingProviderIDPrefix + string(nodeClaim.UID)
		out.Status.Capacity = selected.Capacity
		out.Status.Allocatable = selected.Allocatable()
		out.Labels = lo.Assign(out.Labels, requirementsToLabels(selected.Requirements))
		if result.BatchID != "" {
			out.Annotations = lo.Assign(out.Annotations, map[string]string{BatchIDAnnotationKey: result.BatchID})
		}
		klog.Infof("Create: broker ACK for nodeclaim=%s, set pending providerID=%s batchID=%s", nodeClaim.Name, out.Status.ProviderID, result.BatchID)
		return out, nil
	}
}

// Delete drives a node removal to completion. Karpenter's termination controller calls Delete on
// every reconcile (every ~5s) and only releases the node's termination finalizer once Delete
// reports the instance is gone with a NodeClaimNotFoundError — so Delete must eventually return
// that, or the NodeClaim and Node stay Terminating forever.
//
// The completion signal is the broker reporting the remove operation SUCCEEDED (observed by the
// batcher's status poller). Node existence cannot be used instead: during termination Karpenter
// holds the Node object alive with its own finalizer, and it only drops that finalizer once Delete
// says the instance is gone — so "does a Node still carry this providerID" never converges.
//
// Repeated calls are cheap: while the removal is in flight at the broker the batcher suppresses
// duplicate sends, so the 5s retry loop does not flood the broker's queue.
//
// If the ProviderID is still pending (node not yet joined), the node is looked up by labels first;
// if it has not appeared, a best-effort cancellation of the queued add is sent so provisioning does
// not leave an orphaned node.
func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *karpv1.NodeClaim) error {
	if nodeClaim == nil || nodeClaim.Status.ProviderID == "" {
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("nodeclaim has no provider ID"))
	}

	// The remove operationID is derived from the NodeClaim UID so retries of Delete dedup at
	// the batcher and at the broker.
	operationID := string(nodeClaim.UID) + "-remove"

	// The broker already finished this removal: report the instance as gone so Karpenter releases
	// the finalizer and finishes terminating the NodeClaim.
	if c.batcher.Succeeded(operationID) {
		klog.Infof("Delete: remove operation %s completed at broker — reporting instance gone for nodeclaim=%s", operationID, nodeClaim.Name)
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("node removed for nodeclaim %s", nodeClaim.Name))
	}

	providerID := nodeClaim.Status.ProviderID
	if strings.HasPrefix(providerID, PendingProviderIDPrefix) {
		// Node may not have joined yet; try to find it by labels.
		realID, findErr := c.findNodeProviderID(ctx, nodeClaim)
		if findErr != nil {
			klog.Warningf("Delete: error finding node for pending nodeclaim %s: %v", nodeClaim.Name, findErr)
		}
		if realID == "" {
			// Node has not joined yet. Fire a best-effort cancel of the queued add operation
			// (NodeClaim UID) at the broker; ops already RUNNING or finished are untouched.
			klog.Warningf("Delete: nodeclaim %s still pending — sending best-effort broker cancellation to avoid orphaned node", nodeClaim.Name)
			c.batcher.Cancel(ctx, string(nodeClaim.UID))
			return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("node not yet provisioned for nodeclaim %s", nodeClaim.Name))
		}
		providerID = realID
	}

	req := rafay.RemoveNodesRequest{
		ClusterID:    c.clusterID,
		ProjectID:    c.projectID,
		InstanceType: nodeClaim.Labels[corev1.LabelInstanceTypeStable],
		NodePoolName: nodeClaim.Labels[karpv1.NodePoolLabelKey],
		ProviderID:   providerID,
	}

	resultCh := c.batcher.EnqueueRemove(operationID, req)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			// Karpenter retries Delete; the deterministic operationID dedups the retry.
			return result.Err
		}
		// Queued at the broker. Return nil so Karpenter requeues and calls Delete again; once the
		// poller sees the operation SUCCEEDED, the call above reports the instance gone.
		klog.Infof("Delete: broker ACK for nodeclaim=%s providerID=%s — awaiting removal", nodeClaim.Name, providerID)
		return nil
	}
}

// Get returns the NodeClaim for the given provider ID.
// For synthetic pending IDs the instance is considered to exist (provisioning in progress).
func (c *CloudProvider) Get(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	if providerID == "" {
		return nil, fmt.Errorf("providerID is empty")
	}

	// Synthetic pending ProviderID: the node is being provisioned; report it as existing.
	if strings.HasPrefix(providerID, PendingProviderIDPrefix) {
		nc := &karpv1.NodeClaim{}
		nc.Status.ProviderID = providerID
		return nc, nil
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

// NodeClaimSKUs returns the sku_name node-label values that identify a node belonging to this
// NodeClaim. The Rafay platform stamps sku_name with the SKU (instance type) it provisioned, so the
// primary match is the NodeClaim's instance-type label — Create() stamps that from the instance type
// it actually selected. The NodeClass name is also accepted for the legacy single-SKU convention
// where a RafayNodeClass is named after its only SKU; matching on the NodeClass name alone would
// never resolve a node for any NodeClass that lists several instanceTypes.
func NodeClaimSKUs(nodeClaim *karpv1.NodeClaim) map[string]bool {
	skus := make(map[string]bool, 2)
	if it := nodeClaim.Labels[corev1.LabelInstanceTypeStable]; it != "" {
		skus[it] = true
	}
	if nodeClaim.Spec.NodeClassRef != nil && nodeClaim.Spec.NodeClassRef.Name != "" {
		skus[nodeClaim.Spec.NodeClassRef.Name] = true
	}
	return skus
}

// findNodeProviderID finds the real ProviderID of a node that:
//   - has a nodepoolname label matching the NodeClaim and an acceptable sku_name (see NodeClaimSKUs)
//   - was created after the NodeClaim (guards against claiming pre-existing nodes)
//   - is not already owned by a different non-pending NodeClaim
//
// The usedIDs check prevents Delete() from selecting a node that the NodeProviderIDController
// has already assigned to another NodeClaim, which would cause that node to be incorrectly removed.
// Lists go through apiReader (uncached) — a stale cache here could match the wrong node.
func (c *CloudProvider) findNodeProviderID(ctx context.Context, nodeClaim *karpv1.NodeClaim) (string, error) {
	nodePoolName := nodeClaim.Labels[karpv1.NodePoolLabelKey]
	skus := NodeClaimSKUs(nodeClaim)
	if nodePoolName == "" || len(skus) == 0 {
		return "", nil
	}

	// Build the set of ProviderIDs already owned by other non-pending NodeClaims.
	var claimList karpv1.NodeClaimList
	if err := c.apiReader.List(ctx, &claimList); err != nil {
		return "", fmt.Errorf("list nodeclaims: %w", err)
	}
	usedIDs := make(map[string]bool, len(claimList.Items))
	for i := range claimList.Items {
		if claimList.Items[i].UID == nodeClaim.UID {
			continue // skip self
		}
		pid := claimList.Items[i].Status.ProviderID
		if pid != "" && !strings.HasPrefix(pid, PendingProviderIDPrefix) {
			usedIDs[pid] = true
		}
	}

	// A NodeClaim can accept more than one sku_name, so filter that label in code rather than
	// pushing it into the server-side selector.
	var nodeList corev1.NodeList
	if err := c.apiReader.List(ctx, &nodeList, client.MatchingLabels{
		nodepoolNameLabel: nodePoolName,
	}); err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}

	for i := range nodeList.Items {
		n := &nodeList.Items[i]
		if !skus[n.Labels[skuNameLabel]] {
			continue
		}
		pid := n.Spec.ProviderID
		if pid == "" || usedIDs[pid] {
			continue
		}
		if !n.CreationTimestamp.After(nodeClaim.CreationTimestamp.Time) {
			continue
		}
		return pid, nil
	}
	return "", nil
}

// listNodesFromKube builds a synthetic cloud list from spec.providerID on Nodes (broker mode has no
// list API). Every Node in this cluster carrying a rafay:// ProviderID is an instance this provider
// manages.
//
// Note there is deliberately no cluster-ID filter here. A real ProviderID is stamped by the Rafay
// platform as rafay://<nodepoolname>/<sku_name>/<hostname> — its first segment is the node pool, not
// a cluster ID — so comparing it against RAFAY_CLUSTER_ID never matches and would make List() return
// nothing. An empty List() is dangerous: the core garbage-collection controller deletes any
// Registered NodeClaim whose ProviderID is absent from it, so every managed node would be torn down
// the moment it briefly went NotReady.
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
	if len(nodeClass.Spec.InstanceTypes) == 0 {
		return nil, fmt.Errorf("RafayNodeClass %q has no instanceTypes defined; add at least one entry to spec.instanceTypes", nodeClass.Name)
	}
	return rafayInstanceTypesToKarpenter(nodeClass.Spec.InstanceTypes)
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

func rafayInstanceTypesToKarpenter(specs []v1alpha1.InstanceTypeSpec) ([]*cloudprovider.InstanceType, error) {
	out := make([]*cloudprovider.InstanceType, 0, len(specs))
	for _, s := range specs {
		cpu, err := resource.ParseQuantity(s.CPU)
		if err != nil {
			return nil, fmt.Errorf("instance type %q: invalid cpu %q: %w", s.Name, s.CPU, err)
		}
		mem, err := resource.ParseQuantity(s.Memory)
		if err != nil {
			return nil, fmt.Errorf("instance type %q: invalid memory %q: %w", s.Name, s.Memory, err)
		}
		capacity := corev1.ResourceList{
			corev1.ResourceCPU:    cpu,
			corev1.ResourceMemory: mem,
			corev1.ResourcePods:   resource.MustParse("110"),
		}
		// TEMPORARY accelerator capacity — see InstanceTypeSpec.GPU for what has to be removed
		// with it. A zero count is dropped rather than advertised: Karpenter copies a NodeClaim's
		// requests into spec.resources.requests and Initialization then blocks until the device
		// plugin reports that resource in the node's allocatable, so advertising a GPU the SKU does
		// not have would strand the NodeClaim at Initialized=Unknown with nothing to reap it.
		if s.GPU != "" {
			gpu, err := resource.ParseQuantity(s.GPU)
			if err != nil {
				return nil, fmt.Errorf("instance type %q: invalid %s %q: %w", s.Name, temporaryGPUResourceName, s.GPU, err)
			}
			if gpu.Sign() > 0 {
				capacity[temporaryGPUResourceName] = gpu
			}
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
		if len(s.Architectures) > 0 {
			archVals := make([]string, 0, len(s.Architectures))
			for _, a := range s.Architectures {
				a = strings.TrimSpace(a)
				if a != "" {
					archVals = append(archVals, a)
				}
			}
			if len(archVals) > 0 {
				reqs.Add(scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, archVals...))
			}
		}
		if len(s.OperatingSystems) > 0 {
			osVals := make([]string, 0, len(s.OperatingSystems))
			for _, o := range s.OperatingSystems {
				o = strings.TrimSpace(o)
				if o != "" {
					osVals = append(osVals, o)
				}
			}
			if len(osVals) > 0 {
				reqs.Add(scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, osVals...))
			}
		}
		// Synthetic relative cost, not real currency: 1.0 per vCPU + 0.125 per GiB of memory.
		// The private cloud has no price list; this gives consolidation a gradient so smaller
		// instance types are preferred and underutilized nodes can be replaced by cheaper ones.
		price := cpu.AsApproximateFloat64()*1.0 + mem.AsApproximateFloat64()/(1024*1024*1024)*0.125
		offerings := cloudprovider.Offerings{
			{
				Requirements: reqs,
				Price:        price,
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
	return out, nil
}

// cheapestOffering returns the price of the instance type's first offering (each Rafay instance
// type has exactly one offering; see rafayInstanceTypesToKarpenter).
func cheapestOffering(it *cloudprovider.InstanceType) float64 {
	if len(it.Offerings) == 0 {
		return 0
	}
	return it.Offerings[0].Price
}

// requirementsToLabels converts scheduling requirements to a label map (In-operator values only).
func requirementsToLabels(reqs scheduling.Requirements) map[string]string {
	labels := make(map[string]string)
	for _, r := range reqs.Values() {
		if r != nil && r.Operator() == corev1.NodeSelectorOpIn {
			vals := r.Values()
			if len(vals) == 0 {
				continue
			}
			// Avoid pinning a single value when this SKU allows multiple (misleading on NodeClaim labels).
			if (r.Key == corev1.LabelArchStable || r.Key == corev1.LabelOSStable) && len(vals) > 1 {
				continue
			}
			labels[r.Key] = vals[0]
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

// NewBatchFailureHandler returns a rafay.FailureHandler that deletes the NodeClaim whose UID
// matches a FAILED add operation, so Karpenter reprovisions immediately instead of waiting
// out the 60-minute registration timeout. Only NodeClaims still carrying a pending
// ProviderID are deleted. Remove-operation failures are logged only (Karpenter retries
// Delete as long as the node object exists).
func NewBatchFailureHandler(kubeClient client.Client, apiReader client.Reader) rafay.FailureHandler {
	return func(ctx context.Context, operationID, kind, detail string) {
		if kind != "add" {
			klog.Warningf("batch %s operation failed operationID=%s: %s", kind, operationID, detail)
			return
		}
		var claimList karpv1.NodeClaimList
		if err := apiReader.List(ctx, &claimList); err != nil {
			klog.Warningf("batch add failed operationID=%s (%s): list nodeclaims: %v", operationID, detail, err)
			return
		}
		for i := range claimList.Items {
			nc := &claimList.Items[i]
			if string(nc.UID) != operationID {
				continue
			}
			// Only delete NodeClaims still waiting for their node: a resolved ProviderID means
			// a real node exists, and a deletion timestamp means removal is already underway.
			pid := nc.Status.ProviderID
			if (pid != "" && !strings.HasPrefix(pid, PendingProviderIDPrefix)) || nc.DeletionTimestamp != nil {
				klog.Warningf("batch add failed operationID=%s (%s): nodeclaim %s not pending (providerID=%q deleting=%t), leaving it alone", operationID, detail, nc.Name, pid, nc.DeletionTimestamp != nil)
				return
			}
			klog.Warningf("batch add failed operationID=%s (%s): deleting nodeclaim %s so Karpenter reprovisions immediately", operationID, detail, nc.Name)
			if err := kubeClient.Delete(ctx, nc); err != nil {
				klog.Warningf("batch add failed operationID=%s: delete nodeclaim %s: %v", operationID, nc.Name, err)
			}
			return
		}
		klog.Warningf("batch add failed operationID=%s (%s): no matching nodeclaim found", operationID, detail)
	}
}
