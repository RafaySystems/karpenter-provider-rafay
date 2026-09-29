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
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/apis/v1alpha1"
	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
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

	// RemoveRetiredOtherAnnotationKey persists Delete()'s verdict that the platform retired a
	// different machine than this NodeClaim's (see deleteAfterSucceeded): its value is the
	// provider IDs the broker named, or "unknown" for an untargeted remove whose machine simply
	// never stopped. The in-process memo of the same verdict dies with the process; without the
	// annotation a restarted provider finds no memo, no batcher result and a Node still carrying
	// the ID, re-sends <uid>-remove, and once the broker's SUCCEEDED tombstone has expired that
	// is accepted as a new operation and retires a third machine. Delete() never sends a remove
	// for a NodeClaim carrying it. Operators clear it (or remove the Node's finalizer) once the
	// machine is retired or re-registered.
	RemoveRetiredOtherAnnotationKey = "karpenter.rafay.io/remove-retired-other"

	// DefaultRemoveSettleWindow is how long Delete() waits, after the broker reports an
	// untargeted remove SUCCEEDED, for the machine behind the NodeClaim to actually stop (its
	// kubelet to stop heartbeating) before concluding the platform retired a different machine.
	// It equals the broker's own remove processing timeout (karpenterBatchRemoveTimeout, 60 min).
	// Overridable with RAFAY_REMOVE_SETTLE_WINDOW but never shorter than this.
	DefaultRemoveSettleWindow = 60 * time.Minute

	// retiredOtherUnknown is the RemoveRetiredOtherAnnotationKey value when the broker named no
	// machine.
	retiredOtherUnknown = "unknown"

	nodepoolNameLabel = "nodepoolname"
	skuNameLabel      = "sku_name"

	// nodeProviderIDIndex is the field index the Karpenter operator registers on Nodes
	// (karpenter-rafay pkg/operator/operator.go, IndexField(&corev1.Node{}, "spec.providerID")).
	// Listing through the cached client with client.MatchingFields on it returns the one Node
	// carrying a providerID instead of deep-copying every Node in the cluster.
	nodeProviderIDIndex = "spec.providerID"

	// defaultArchitecture and defaultOperatingSystem are what an instance type advertises when
	// its RafayNodeClass lists no architectures / operatingSystems. Karpenter requires every
	// well-known label to be defined on an instance type (cloudprovider.InstanceType.Requirements);
	// left undefined, a pod selecting kubernetes.io/arch=arm64 would be matched to an amd64 SKU.
	// The broker always renders both; these defaults only apply to hand-written classes.
	defaultArchitecture    = "amd64"
	defaultOperatingSystem = "linux"

	// temporaryGPUResourceName is the extended resource InstanceTypeSpec.GPU is advertised under.
	// TEMPORARY, and hardcoded on purpose — see InstanceTypeSpec.GPU. Real support carries the
	// per-SKU name the broker already resolves (amd.com/gpu on AMD shapes) instead of assuming
	// NVIDIA for every SKU.
	temporaryGPUResourceName = corev1.ResourceName("nvidia.com/gpu")
)

// NodeOwnershipMu serialises every read-decide-write cycle that binds a Node to a NodeClaim:
// NodeAdoptionController (a NodeClaim for an unowned node), NodeProviderIDController (a joined
// node bound to a pending NodeClaim) and Delete's resolvePendingRemoval (a joined node reserved
// for a removal). Each computes "which provider IDs are spoken for" from an uncached NodeClaim
// LIST, and that answer is only valid until the matching write lands. The two controllers
// serialise within themselves (MaxConcurrentReconciles=1) but not against each other, and all
// three run in this one manager, so one process-wide lock is enough. Without it: adoption LISTs
// while a scale-out's NodeClaim P is still empty (Create() awaiting the broker ACK) and finds an
// operator-added node N unowned; Create() returns and P turns pending; NodeProviderIDController
// LISTs (no claim for N yet) and binds N to P; adoption then creates a second NodeClaim for N.
var NodeOwnershipMu sync.Mutex

// nodeBatcher is the subset of *rafay.NodeBatcher that Create and Delete call. It exists solely so
// tests can substitute a fake; production code always passes the concrete *rafay.NodeBatcher via
// NewCloudProvider.
type nodeBatcher interface {
	Succeeded(operationID string) bool
	Enqueue(operationID string, req rafay.AddNodesRequest) <-chan rafay.BatchResult
	EnqueueRemove(operationID string, req rafay.RemoveNodesRequest) <-chan rafay.BatchResult
	// Cancel reports whether the broker cancelled the still-queued operation (applied) — false
	// means it was already running or finished, so its platform write must be assumed committed.
	Cancel(ctx context.Context, operationID string) (applied bool, err error)
	// SucceededResult exposes the broker's detail and provider IDs for a SUCCEEDED operation.
	SucceededResult(operationID string) (providerIDs []string, detail string, ok bool)
}

// CloudProvider implements Karpenter's cloudprovider.CloudProvider by calling Rafay APIs to add/remove nodes (private cloud).
type CloudProvider struct {
	kubeClient client.Client
	// apiReader reads directly from the API server (no cache). Used where a stale cache could
	// cause a wrong decision, e.g. matching a freshly joined node to a pending NodeClaim.
	apiReader client.Reader
	client    rafay.Client
	clusterID string
	projectID string
	batcher   nodeBatcher
	// poolBackoff holds back NodePools the broker refused to grow (permanent refusals such as
	// "pool at maximum"); GetInstanceTypes consults it. Shared with the batch failure handler,
	// which marks pools there and also records every terminal FAILED operation for Delete()
	// (PoolBackoff.FailedOp). May be nil (no backoff, no records).
	poolBackoff *PoolBackoff
	// recorder publishes the Warning events Delete() raises on a NodePool (NodeNotRetired,
	// RemoveRetiredOtherMachine). Nil means log only. Set with WithEventRecorder.
	recorder events.Recorder

	// sentRemoves memoises, per NodeClaim UID, the providerID the <uid>-remove was ACKed with in
	// this process. For a NodeClaim still carrying a pending providerID it saves the two uncached
	// LISTs of findNodeProviderID on each 5-second Delete() retry (the NodeClaim keeps its pending
	// ID until the remove SUCCEEDs); for a real one it skips the "does a Node still carry this
	// ID" check once the removal is under way. Lost on restart, which costs one lookup.
	sentRemoves sync.Map
	// uncancellable memoises the NodeClaim UIDs whose add the broker reported it could no longer
	// cancel (RUNNING or finished): the answer never changes, so the 5-second retries do not
	// repeat the RPC while waiting for the poller to report the add's terminal state.
	uncancellable sync.Map
	// retiredOther memoises the remove operationIDs whose SUCCEEDED turned out to have retired a
	// machine that is not the NodeClaim's own. Delete() keeps such a NodeClaim terminating (see
	// deleteAfterSucceeded) and must not re-send the remove once the batcher forgets the result.
	// The same verdict is persisted as RemoveRetiredOtherAnnotationKey; the memo covers the
	// window in which that patch has not landed.
	retiredOther sync.Map
	// succeededSeen memoises, per remove operationID, when this process first saw the broker
	// report it SUCCEEDED; deleteAfterSucceeded measures the settle window from it.
	succeededSeen sync.Map
	// settleWindow is the remove settle window (DefaultRemoveSettleWindow when zero). Set with
	// WithRemoveSettleWindow.
	settleWindow time.Duration
	// warned dedups the once-per-operation warnings and events Delete() raises.
	warned sync.Map
}

// Option customises a CloudProvider built by NewCloudProvider.
type Option func(*CloudProvider)

// WithEventRecorder lets Delete() record its Warning events (NodeNotRetiredEventReason,
// RemoveRetiredOtherMachineEventReason) on the NodePool. Without it they are only logged.
func WithEventRecorder(recorder events.Recorder) Option {
	return func(c *CloudProvider) { c.recorder = recorder }
}

// WithRemoveSettleWindow sets how long Delete() waits for the machine behind a NodeClaim to stop
// after its untargeted remove SUCCEEDED (see deleteAfterSucceeded). A value below
// DefaultRemoveSettleWindow is raised to it: the platform may take up to the broker's remove
// timeout to retire a machine, and a shorter window would call a slow retirement "wrong machine".
func WithRemoveSettleWindow(d time.Duration) Option {
	return func(c *CloudProvider) {
		if d < DefaultRemoveSettleWindow {
			klog.Warningf("remove settle window %s is shorter than the platform's retirement bound; using %s", d, DefaultRemoveSettleWindow)
			d = DefaultRemoveSettleWindow
		}
		c.settleWindow = d
	}
}

// removeSettleWindow is the configured settle window, defaulting when unset.
func (c *CloudProvider) removeSettleWindow() time.Duration {
	if c.settleWindow <= 0 {
		return DefaultRemoveSettleWindow
	}
	return c.settleWindow
}

// NewCloudProvider returns a Rafay cloud provider that uses the given Rafay client.
// The batcher must already be started (call BrokerClient.StartBatcher) before Create is called.
// poolBackoff is the store NewBatchFailureHandler marks; nil disables the pool backoff and the
// per-operation failure records Delete() consults.
func NewCloudProvider(kubeClient client.Client, apiReader client.Reader, rafayClient rafay.Client, clusterID, projectID string, batcher *rafay.NodeBatcher, poolBackoff *PoolBackoff, opts ...Option) *CloudProvider {
	c := &CloudProvider{
		kubeClient:  kubeClient,
		apiReader:   apiReader,
		client:      rafayClient,
		clusterID:   clusterID,
		projectID:   projectID,
		batcher:     batcher,
		poolBackoff: poolBackoff,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Create enqueues a node-add request with the NodeBatcher and returns as soon as the broker
// acknowledges the request. A synthetic pending ProviderID is set on the returned NodeClaim;
// the NodeProviderIDController will patch it with the real value once the node joins.
//
// A NodeClaim carrying AdoptedProviderIDAnnotationKey is the exception: its machine is already
// running and registered in the cluster, so no broker call is made and the annotated ProviderID is
// returned directly. See the annotation's doc for why the marker has to be on the object.
//
// A RafayNodeClass that is gone, not Ready, or has no usable instanceTypes is reported as
// cloudprovider.NodeClassNotReadyError: Karpenter's launch reconciler then drops the NodeClaim at
// once instead of retrying with backoff for the 5-minute launch timeout and counting a launch
// failure against the NodePool. Other errors (API server, batcher) stay plain so the launch is
// retried.
func (c *CloudProvider) Create(ctx context.Context, nodeClaim *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	if nodeClaim == nil {
		return nil, fmt.Errorf("nodeclaim is nil")
	}

	nodeClass, err := c.resolveNodeClassFromNodeClaim(ctx, nodeClaim)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, cloudprovider.NewNodeClassNotReadyError(fmt.Errorf("resolve NodeClass: %w", err))
		}
		return nil, fmt.Errorf("resolve NodeClass: %w", err)
	}
	if ready := nodeClass.StatusConditions().Get(status.ConditionReady); ready != nil && ready.IsFalse() {
		return nil, cloudprovider.NewNodeClassNotReadyError(fmt.Errorf("RafayNodeClass %q is not ready: %s: %s", nodeClass.Name, ready.Reason, ready.Message))
	}

	instanceTypes, err := c.getInstanceTypes(ctx, nodeClass)
	if err != nil {
		// Empty or unparseable instanceTypes: nothing a retry can launch until the class is fixed.
		return nil, cloudprovider.NewNodeClassNotReadyError(fmt.Errorf("get instance types: %w", err))
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
		out.Status.Capacity = nodeClaimResources(selected.Capacity)
		out.Status.Allocatable = nodeClaimResources(selected.Allocatable())
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
		out.Status.Capacity = nodeClaimResources(selected.Capacity)
		out.Status.Allocatable = nodeClaimResources(selected.Allocatable())
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
// batcher's status poller). Node existence cannot be used as the completion signal: during
// termination Karpenter holds the Node object alive with its own finalizer, and it only drops that
// finalizer once Delete says the instance is gone — so "does a Node still carry this providerID"
// never converges. It is, however, a valid signal for the opposite question. Removal is untargeted
// (the broker lowers the pool's count and the platform picks the machine), so a NodeClaim whose
// Node object has already vanished — the platform retired the machine, or retired a different one
// than Karpenter asked for and the garbage collector deleted the orphaned NodeClaim — must NOT
// send a fresh remove: every such remove retires one more healthy machine, one per cycle, down to
// the pool floor. On the normal termination path the Node object always exists when the first
// Delete runs (the lifecycle controller deletes Nodes first and Karpenter's node finalizer keeps
// the object until Delete reports NotFound), so the check only bites on the orphan path. The
// Node object rarely vanishes outright, though: every registered Node carries Karpenter's
// termination finalizer, so a machine the platform retired and whose Node it deleted (or
// drained) leaves a Terminating Node whose kubelet has stopped; the node termination controller
// then deletes the NodeClaim itself and ends up here. retiredExternally recognises that shape
// (Node deleted no later than the NodeClaim, not by Karpenter's disruption queue, kubelet not
// Ready) and Delete reports the instance gone without a remove for it as well.
//
// Repeated calls are cheap: while the removal is in flight at the broker the batcher suppresses
// duplicate sends, so the 5s retry loop does not flood the broker's queue.
//
// A NodeClaim whose ProviderID is still pending is handled by resolvePendingRemoval: the add
// operation's own state decides whether a machine has to be removed (add SUCCEEDED), nothing has
// to be done (add FAILED or cancelled at the broker) or the outcome is still open (add RUNNING:
// keep the NodeClaim so the machine that lands has an owner).
func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *karpv1.NodeClaim) error {
	if nodeClaim == nil || nodeClaim.Status.ProviderID == "" {
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("nodeclaim has no provider ID"))
	}

	uid := string(nodeClaim.UID)
	// The remove operationID is derived from the NodeClaim UID so retries of Delete dedup at
	// the batcher and at the broker.
	operationID := uid + "-remove"
	providerID := nodeClaim.Status.ProviderID

	// An earlier SUCCEEDED told us the platform retired another machine (see deleteAfterSucceeded).
	// The verdict lives on the NodeClaim (it survives a restart of this process) and in the memo
	// (in case that patch has not landed); the batcher may have forgotten the result and the
	// broker's tombstone may have expired meanwhile, so the remove must never be re-sent.
	if _, ok := c.retiredOther.Load(operationID); ok || nodeClaim.Annotations[RemoveRetiredOtherAnnotationKey] != "" {
		return c.holdForRetiredOtherMachine(ctx, nodeClaim, operationID)
	}
	// The broker already finished this removal.
	if pids, detail, ok := c.batcher.SucceededResult(operationID); ok {
		return c.deleteAfterSucceeded(ctx, nodeClaim, operationID, pids, detail)
	}
	// The broker refused the remove for a reason a retry cannot fix (an old broker FAILs the op
	// with a "pool at minimum" detail; the batch failure handler recorded it). Re-sending it every
	// 5s would only be refused again while the drained node stays cordoned: converge instead —
	// the machine keeps running (see warnNodeNotRetired for how it comes back).
	if detail, failed := c.poolBackoff.FailedOp(operationID); failed && IsPermanentRefusalDetail(detail) {
		c.warnNodeNotRetired(ctx, nodeClaim, operationID, detail)
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("remove refused permanently for nodeclaim %s: %s", nodeClaim.Name, detail))
	}

	if strings.HasPrefix(providerID, PendingProviderIDPrefix) {
		resolved, send, err := c.resolvePendingRemoval(ctx, nodeClaim)
		if !send {
			return err
		}
		providerID = resolved
	} else if _, sent := c.sentRemoves.Load(uid); !sent {
		node, err := c.nodeByProviderID(ctx, providerID)
		if err != nil {
			return fmt.Errorf("look up node for nodeclaim %s: %w", nodeClaim.Name, err)
		}
		if node == nil {
			klog.Warningf("Delete: no Node carries providerID=%s for nodeclaim=%s and no removal was sent — the machine is already gone; reporting instance gone without a remove (a remove here would retire another machine)", providerID, nodeClaim.Name)
			return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("no node carries provider ID %s for nodeclaim %s", providerID, nodeClaim.Name))
		}
		if retiredExternally(node, nodeClaim) {
			c.warnNodeRetiredExternally(ctx, nodeClaim, node, operationID)
			return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("node %s carrying provider ID %s for nodeclaim %s was retired outside Karpenter", node.Name, providerID, nodeClaim.Name))
		}
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
		c.sentRemoves.Store(uid, providerID)
		// Queued at the broker. Return nil so Karpenter requeues and calls Delete again; once the
		// poller sees the operation SUCCEEDED, the call above reports the instance gone. Only
		// the first ACK is worth a line: the 5-second re-invocations that follow are resolved
		// from the in-flight set without a send (a 15-minute retire is ~180 of them).
		if result.Duplicate {
			klog.V(2).Infof("Delete: removal already in flight for nodeclaim=%s providerID=%s — awaiting removal", nodeClaim.Name, providerID)
		} else {
			klog.Infof("Delete: broker ACK for nodeclaim=%s providerID=%s — awaiting removal", nodeClaim.Name, providerID)
		}
		return nil
	}
}

// deleteAfterSucceeded is Delete once the broker reported <uid>-remove SUCCEEDED with the given
// provider IDs and detail.
//
//   - A "node not retired" detail means the broker applied no catalog change (pool at its
//     minimum, unknown pool, SKU mismatch, pool not auto-scaling): the machine is still running.
//     Delete still converges — NotFound releases the finalizer and Karpenter drops the Node
//     object — but warns the operator once (see warnNodeNotRetired for how the machine comes
//     back).
//   - Provider IDs that name other machines but not this NodeClaim's mean the platform retired a
//     different machine (removal is untargeted). Reporting NotFound would drop the finalizer of a
//     Node whose kubelet is alive; the API server deletes the Node object and a kubelet that has
//     completed registration never re-creates it, leaving a machine the platform counts but no
//     scheduler can see. So the NodeClaim is kept terminating (nil) for as long as a Node carries
//     its ID, with a Warning event; re-sending the remove is not an option (it would retire a
//     third machine).
//   - No provider IDs — every broker so far: the remove SUCCEEDs the moment the catalog
//     decrement is published, names no machine, and the platform retires one of its choosing
//     minutes later — is the same question with one signal to answer it: whether the machine
//     behind THIS NodeClaim stopped. See deleteAfterUntargetedSucceeded.
//   - IDs that include this NodeClaim's, or a still-pending providerID (the machine never
//     registered, so no Node can tell): the instance is gone.
func (c *CloudProvider) deleteAfterSucceeded(ctx context.Context, nodeClaim *karpv1.NodeClaim, operationID string, providerIDs []string, detail string) error {
	if IsNotRetiredDetail(detail) {
		c.warnNodeNotRetired(ctx, nodeClaim, operationID, detail)
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("remove completed without retiring a node for nodeclaim %s: %s", nodeClaim.Name, detail))
	}
	providerID := nodeClaim.Status.ProviderID
	if !strings.HasPrefix(providerID, PendingProviderIDPrefix) {
		if len(providerIDs) == 0 {
			return c.deleteAfterUntargetedSucceeded(ctx, nodeClaim, operationID)
		}
		if !lo.Contains(providerIDs, providerID) {
			return c.holdRetiredOther(ctx, nodeClaim, operationID, providerIDs)
		}
	}
	klog.Infof("Delete: remove operation %s completed at broker — reporting instance gone for nodeclaim=%s", operationID, nodeClaim.Name)
	return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("node removed for nodeclaim %s", nodeClaim.Name))
}

// deleteAfterUntargetedSucceeded is deleteAfterSucceeded for a SUCCEEDED remove that named no
// machine. The kubelet heartbeat is the one signal about which machine the platform retired: a
// retired machine's Node goes NotReady within the node monitor grace period (~40 s) and stays so,
// while the finalizer only keeps the *object*. So, while a Node still carries the NodeClaim's ID
// and reports Ready, the NodeClaim is kept terminating (nil) for up to the settle window measured
// from the first time this process saw the SUCCEEDED (the platform may need that long to retire
// a machine; a restart of this process restarts the window). NotFound follows as soon as the Node
// is gone or not Ready. A Node still Ready after the window means the platform retired another
// machine: the retired-other verdict applies (event, annotation, held until no Node carries the
// ID), exactly as if the broker had named it.
//
// This deliberately makes a normal termination take as long as the platform's real retirement
// rather than the broker's publish, so a drained, cordoned Node stays visible until its machine
// is actually gone — the alternative deletes a Node object whose kubelet is alive, and a v1.31
// kubelet never re-registers on its own.
func (c *CloudProvider) deleteAfterUntargetedSucceeded(ctx context.Context, nodeClaim *karpv1.NodeClaim, operationID string) error {
	providerID := nodeClaim.Status.ProviderID
	node, err := c.nodeByProviderID(ctx, providerID)
	if err != nil {
		return fmt.Errorf("look up node for nodeclaim %s: %w", nodeClaim.Name, err)
	}
	if node == nil {
		klog.Infof("Delete: remove operation %s completed at broker and no Node carries providerID=%s — reporting instance gone for nodeclaim=%s", operationID, providerID, nodeClaim.Name)
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("node removed for nodeclaim %s", nodeClaim.Name))
	}
	if !nodeReady(node) {
		klog.Infof("Delete: remove operation %s completed at broker and Node %s (providerID=%s) is no longer Ready — reporting instance gone for nodeclaim=%s", operationID, node.Name, providerID, nodeClaim.Name)
		return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("node removed for nodeclaim %s", nodeClaim.Name))
	}
	firstSeen, _ := c.succeededSeen.LoadOrStore(operationID, time.Now())
	elapsed := time.Since(firstSeen.(time.Time))
	if elapsed < c.removeSettleWindow() {
		klog.V(2).Infof("Delete: remove operation %s completed at broker %s ago but Node %s (providerID=%s) is still Ready — keeping nodeclaim=%s terminating until the platform retires the machine (settle window %s)", operationID, elapsed.Round(time.Second), node.Name, providerID, nodeClaim.Name, c.removeSettleWindow())
		return nil
	}
	return c.holdRetiredOther(ctx, nodeClaim, operationID, nil)
}

// holdRetiredOther records the verdict that remove operationID retired a machine other than the
// NodeClaim's (providerIDs names it when the broker did; nil when the NodeClaim's machine simply
// never stopped) — in memory, on the NodeClaim and as a Warning event, once — and keeps the
// NodeClaim terminating while a Node carries its ID.
func (c *CloudProvider) holdRetiredOther(ctx context.Context, nodeClaim *karpv1.NodeClaim, operationID string, providerIDs []string) error {
	c.retiredOther.Store(operationID, struct{}{})
	providerID := nodeClaim.Status.ProviderID
	if c.warnOnce(operationID + "/retired-other") {
		var msg string
		if len(providerIDs) > 0 {
			msg = fmt.Sprintf("remove operation %s completed but the platform retired %s, not this NodeClaim's machine %s; keeping NodeClaim %s terminating until no Node carries its ID (remove the Node's karpenter.sh/termination finalizer once the machine is retired or re-registered)", operationID, strings.Join(providerIDs, ","), providerID, nodeClaim.Name)
		} else {
			msg = fmt.Sprintf("remove operation %s completed at the broker %s ago but the machine behind NodeClaim %s (%s) is still Ready: the platform retired a different machine; keeping the NodeClaim terminating until no Node carries its ID (remove the Node's karpenter.sh/termination finalizer once the machine is retired or re-registered)", operationID, c.removeSettleWindow(), nodeClaim.Name, providerID)
		}
		klog.Warning("Delete: " + msg)
		publishNodePoolEvent(ctx, c.kubeClient, c.recorder, nodeClaim.Labels[karpv1.NodePoolLabelKey], RemoveRetiredOtherMachineEventReason, msg, operationID)
	}
	if nodeClaim.Annotations[RemoveRetiredOtherAnnotationKey] == "" {
		value := retiredOtherUnknown
		if len(providerIDs) > 0 {
			value = strings.Join(providerIDs, ",")
		}
		c.annotateRetiredOther(ctx, nodeClaim, value)
	}
	return c.holdForRetiredOtherMachine(ctx, nodeClaim, operationID)
}

// annotateRetiredOther patches RemoveRetiredOtherAnnotationKey=value onto the stored NodeClaim.
// Best effort, like reserveProviderID: a failure is logged and retried on the next Delete (the
// in-memory memo holds the verdict meanwhile).
func (c *CloudProvider) annotateRetiredOther(ctx context.Context, nodeClaim *karpv1.NodeClaim, value string) {
	var stored karpv1.NodeClaim
	if err := c.apiReader.Get(ctx, client.ObjectKeyFromObject(nodeClaim), &stored); err != nil {
		klog.V(2).Infof("Delete: annotate nodeclaim %s with %s: get: %v", nodeClaim.Name, RemoveRetiredOtherAnnotationKey, err)
		return
	}
	if stored.Annotations[RemoveRetiredOtherAnnotationKey] != "" {
		return
	}
	patched := stored.DeepCopy()
	patched.Annotations = lo.Assign(patched.Annotations, map[string]string{RemoveRetiredOtherAnnotationKey: value})
	if err := c.kubeClient.Patch(ctx, patched, client.MergeFrom(&stored)); err != nil {
		klog.Warningf("Delete: annotate nodeclaim %s with %s=%s: %v", nodeClaim.Name, RemoveRetiredOtherAnnotationKey, value, err)
		return
	}
	klog.Infof("Delete: recorded %s=%s on nodeclaim %s", RemoveRetiredOtherAnnotationKey, value, nodeClaim.Name)
}

// holdForRetiredOtherMachine keeps a NodeClaim whose remove retired another machine terminating
// (nil) while a Node still carries its providerID, and reports the instance gone once none does.
func (c *CloudProvider) holdForRetiredOtherMachine(ctx context.Context, nodeClaim *karpv1.NodeClaim, operationID string) error {
	node, err := c.nodeByProviderID(ctx, nodeClaim.Status.ProviderID)
	if err != nil {
		return fmt.Errorf("look up node for nodeclaim %s: %w", nodeClaim.Name, err)
	}
	if node != nil {
		klog.V(2).Infof("Delete: nodeclaim=%s kept terminating — remove %s retired another machine and Node %s still carries providerID=%s", nodeClaim.Name, operationID, node.Name, nodeClaim.Status.ProviderID)
		return nil
	}
	return cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("no node carries provider ID %s for nodeclaim %s", nodeClaim.Status.ProviderID, nodeClaim.Name))
}

// retiredExternally reports whether node — the Node carrying nodeClaim's providerID, found while
// no remove has been sent for it in this process — is what a machine the platform (or an
// operator) already retired leaves behind, so that sending <uid>-remove would retire a second
// machine. The shape: the Node was deleted no later than the NodeClaim, the NodeClaim was not
// chosen by Karpenter's disruption queue, and the kubelet no longer heartbeats.
//
// Karpenter's own path deletes the NodeClaim first and the Node after (lifecycle controller); the
// external path deletes the Node first and the node termination controller then deletes the
// NodeClaim. Both steps usually land within one second, and deletion timestamps have second
// granularity, so the comparison is "not after" rather than "before": the ambiguous same-second
// case is resolved towards not sending a remove, the error that cannot cascade. A NodeClaim the
// disruption queue chose (consolidation, drift, ...) carries the DisruptionReason condition and is
// Karpenter's own whatever the timestamps say. A Ready kubelet means the machine is alive (an
// operator deleted a healthy Node): the remove is still sent.
func retiredExternally(node *corev1.Node, nodeClaim *karpv1.NodeClaim) bool {
	if node.DeletionTimestamp.IsZero() || nodeClaim.DeletionTimestamp.IsZero() {
		return false
	}
	if node.DeletionTimestamp.After(nodeClaim.DeletionTimestamp.Time) {
		return false
	}
	if nodeClaim.StatusConditions().Get(karpv1.ConditionTypeDisruptionReason).IsTrue() {
		return false
	}
	return !nodeReady(node)
}

// nodeReady reports whether the Node's Ready condition is True (its kubelet is heartbeating).
func nodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// warnNodeRetiredExternally tells the operator, once per operation, that Delete found the
// NodeClaim's Node already retired outside Karpenter and sent no remove for it.
func (c *CloudProvider) warnNodeRetiredExternally(ctx context.Context, nodeClaim *karpv1.NodeClaim, node *corev1.Node, operationID string) {
	if !c.warnOnce(operationID + "/retired-externally") {
		return
	}
	msg := fmt.Sprintf("Node %s (providerID %s) was deleted outside Karpenter at %s, before NodeClaim %s, and its kubelet is not Ready: the platform (or an operator) already retired the machine, so no remove is sent for it — one would retire another machine", node.Name, nodeClaim.Status.ProviderID, node.DeletionTimestamp.Format(time.RFC3339), nodeClaim.Name)
	klog.Warning("Delete: " + msg)
	publishNodePoolEvent(ctx, c.kubeClient, c.recorder, nodeClaim.Labels[karpv1.NodePoolLabelKey], NodeRetiredExternallyEventReason, msg, operationID)
}

// resolvePendingRemoval decides what Delete does for a NodeClaim whose providerID is still the
// synthetic pending one. It returns the providerID the remove should carry (empty: untargeted, the
// machine has not registered) with send == true when a remove must be sent; otherwise err is what
// Delete returns — a NodeClaimNotFoundError when no machine exists for this NodeClaim, nil when
// the outcome is still open and Karpenter should call again in 5s, or a real error.
//
// Order matters:
//
//  1. A remove already ACKed for this NodeClaim in this process is re-sent as is (the batcher
//     dedups it) without re-running the two uncached LISTs of findNodeProviderID.
//  2. The add SUCCEEDED at the broker: a machine exists or is about to register, whether or not a
//     Node carries a resolvable ID yet. If one does, the NodeClaim is given that ID first (a
//     reservation, so NodeProviderIDController cannot bind the same node to another pending
//     NodeClaim while the removal is in flight); either way the remove is sent. A cancel cannot
//     undo a finished add, and NotFound would finalize the NodeClaim while its machine lands with
//     no owner and the catalog keeps counting it.
//  3. The add FAILED (the batch failure handler recorded it): no machine is coming.
//  4. Otherwise the add is queued or running at the broker and only a synchronous cancel can tell
//     which: applied means the broker dropped it before it ran (NotFound); not applied means it
//     is RUNNING or already finished, so the NodeClaim is kept until the poller reports the add's
//     terminal state (then 2 or 3 applies); a failed cancel RPC leaves the fate unknown and is
//     returned so Karpenter retries.
//
// Only the add's own state is consulted, never a node that merely matches the NodeClaim's
// pool/SKU: such a node may belong to another pending NodeClaim or to an operator, and removing it
// for an add that never applied retires a healthy machine.
func (c *CloudProvider) resolvePendingRemoval(ctx context.Context, nodeClaim *karpv1.NodeClaim) (providerID string, send bool, err error) {
	uid := string(nodeClaim.UID)
	if v, ok := c.sentRemoves.Load(uid); ok {
		return v.(string), true, nil
	}
	if c.batcher.Succeeded(uid) {
		// The lookup and the reservation are one ownership decision (see NodeOwnershipMu).
		NodeOwnershipMu.Lock()
		realID, findErr := c.findNodeProviderID(ctx, nodeClaim)
		if findErr == nil && realID != "" {
			c.reserveProviderID(ctx, nodeClaim, realID)
		}
		NodeOwnershipMu.Unlock()
		if findErr != nil {
			// "Could not look" is not "nothing there": returning NotFound here would release the
			// finalizer with the machine alive and no remove sent. Karpenter retries in 5s.
			return "", false, fmt.Errorf("find node for pending nodeclaim %s: %w", nodeClaim.Name, findErr)
		}
		if realID == "" {
			klog.Warningf("Delete: add for nodeclaim %s succeeded at the broker but its node has not registered — sending an untargeted remove so the catalog count is undone", nodeClaim.Name)
		}
		return realID, true, nil
	}
	if detail, failed := c.poolBackoff.FailedOp(uid); failed {
		klog.Infof("Delete: add for nodeclaim %s failed at the broker (%s) — no machine to remove", nodeClaim.Name, detail)
		return "", false, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("add failed for nodeclaim %s: %s", nodeClaim.Name, detail))
	}
	if _, ok := c.uncancellable.Load(uid); ok {
		klog.V(2).Infof("Delete: nodeclaim %s still pending and its add is running at the broker — waiting for its outcome", nodeClaim.Name)
		return "", false, nil
	}
	applied, cancelErr := c.batcher.Cancel(ctx, uid)
	if cancelErr != nil {
		return "", false, fmt.Errorf("cancel add operation for pending nodeclaim %s: %w", nodeClaim.Name, cancelErr)
	}
	if applied {
		klog.Infof("Delete: nodeclaim %s still pending — broker cancelled the queued add; no machine will be built", nodeClaim.Name)
		return "", false, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("add cancelled for nodeclaim %s", nodeClaim.Name))
	}
	c.uncancellable.Store(uid, struct{}{})
	klog.Warningf("Delete: nodeclaim %s still pending and its add can no longer be cancelled (running or finished at the broker) — keeping the NodeClaim until the poller reports the add's outcome", nodeClaim.Name)
	return "", false, nil
}

// reserveProviderID patches providerID onto the stored NodeClaim's status.providerID (with an
// optimistic lock, like NodeProviderIDController) so the node is spoken for while its removal is
// in flight. Best effort: a conflict, a NodeClaim already carrying a real ID, or a missing
// NodeClaim is logged and the removal proceeds with the resolved ID regardless.
func (c *CloudProvider) reserveProviderID(ctx context.Context, nodeClaim *karpv1.NodeClaim, providerID string) {
	var stored karpv1.NodeClaim
	if err := c.apiReader.Get(ctx, client.ObjectKeyFromObject(nodeClaim), &stored); err != nil {
		klog.V(2).Infof("Delete: reserve providerID=%s for nodeclaim %s: get: %v", providerID, nodeClaim.Name, err)
		return
	}
	if !strings.HasPrefix(stored.Status.ProviderID, PendingProviderIDPrefix) {
		klog.V(2).Infof("Delete: nodeclaim %s already carries providerID=%s; not reserving %s", nodeClaim.Name, stored.Status.ProviderID, providerID)
		return
	}
	patched := stored.DeepCopy()
	patched.Status.ProviderID = providerID
	if err := c.kubeClient.Status().Patch(ctx, patched, client.MergeFromWithOptions(&stored, client.MergeFromWithOptimisticLock{})); err != nil {
		klog.Warningf("Delete: reserve providerID=%s for nodeclaim %s: %v", providerID, nodeClaim.Name, err)
		return
	}
	klog.Infof("Delete: reserved providerID=%s for pending nodeclaim %s before removal", providerID, nodeClaim.Name)
}

// warnOnce reports whether key has not been warned about yet, recording it.
func (c *CloudProvider) warnOnce(key string) bool {
	_, loaded := c.warned.LoadOrStore(key, struct{}{})
	return !loaded
}

// warnNodeNotRetired tells the operator, once per operation, that a remove converged without the
// platform retiring a machine. The machine behind the NodeClaim keeps running, but Karpenter drops
// its Node object and a kubelet that has completed registration does not re-create it on its own:
// the kubelet must be restarted (or the platform must re-register the node) before the machine
// reappears as a Node, and the node adoption controller then adopts it back.
func (c *CloudProvider) warnNodeNotRetired(ctx context.Context, nodeClaim *karpv1.NodeClaim, operationID, detail string) {
	if !c.warnOnce(operationID + "/not-retired") {
		return
	}
	msg := fmt.Sprintf("remove operation %s for NodeClaim %s did not retire its machine (%s): the platform refused the count change; the machine %s keeps running but its Node object is dropped and the kubelet does not re-register on its own — restart the kubelet (or re-register the node from the platform) and the node is adopted back", operationID, nodeClaim.Name, detail, nodeClaim.Status.ProviderID)
	klog.Warning("Delete: " + msg)
	publishNodePoolEvent(ctx, c.kubeClient, c.recorder, nodeClaim.Labels[karpv1.NodePoolLabelKey], NodeNotRetiredEventReason, msg, operationID)
}

// Get returns the NodeClaim for the given provider ID.
// For synthetic pending IDs the instance is considered to exist (provisioning in progress).
//
// In broker mode (no GetNode API) the answer comes from the Kubernetes Node carrying the ID. Note
// what that means for Karpenter's node termination controller: it calls Get for a NotReady node
// and skips the drain when the instance is gone (NodeClaimNotFound) — but the Node it asks about
// is the one it is finalizing, pinned by karpenter.sh/termination, so Get always finds it and
// that short-circuit never fires here. Get must not guess from readiness either: a transiently
// NotReady machine is alive, and dropping its finalizer orphans it. A dead machine's drain is
// bounded by the NodePool's terminationGracePeriod instead, which the broker renders.
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
// match is the NodeClaim's instance-type label — Create() stamps that from the instance type it
// actually selected. Only a NodeClaim that has no instance-type label yet (not through Create,
// or the legacy single-SKU convention where a RafayNodeClass is named after its only SKU) falls
// back to the NodeClass name. The name is not accepted alongside the label: a RafayNodeClass named
// after one of several instanceTypes would otherwise let a NodeClaim that selected SKU B be bound
// to — or remove — a machine of SKU A.
func NodeClaimSKUs(nodeClaim *karpv1.NodeClaim) map[string]bool {
	skus := make(map[string]bool, 1)
	if it := nodeClaim.Labels[corev1.LabelInstanceTypeStable]; it != "" {
		skus[it] = true
		return skus
	}
	if nodeClaim.Spec.NodeClassRef != nil && nodeClaim.Spec.NodeClassRef.Name != "" {
		skus[nodeClaim.Spec.NodeClassRef.Name] = true
	}
	return skus
}

// findNodeProviderID finds the real ProviderID of a node that:
//   - has a nodepoolname label matching the NodeClaim and an acceptable sku_name (see NodeClaimSKUs)
//   - was created after the NodeClaim (guards against claiming pre-existing nodes)
//   - is not already spoken for by a different NodeClaim: a non-pending status.providerID
//     (bound by NodeProviderIDController or reserved by Delete) or the adoption controller's
//     AdoptedProviderIDAnnotationKey (an existing node whose NodeClaim has not been through
//     Create yet and so has an empty status.providerID)
//
// The usedIDs check prevents Delete() from selecting a node that belongs to another NodeClaim,
// which would cause that node to be incorrectly removed.
// Lists go through apiReader (uncached) — a stale cache here could match the wrong node.
func (c *CloudProvider) findNodeProviderID(ctx context.Context, nodeClaim *karpv1.NodeClaim) (string, error) {
	nodePoolName := nodeClaim.Labels[karpv1.NodePoolLabelKey]
	skus := NodeClaimSKUs(nodeClaim)
	if nodePoolName == "" || len(skus) == 0 {
		return "", nil
	}

	// Build the set of ProviderIDs already spoken for by other NodeClaims.
	var claimList karpv1.NodeClaimList
	if err := c.apiReader.List(ctx, &claimList); err != nil {
		return "", fmt.Errorf("list nodeclaims: %w", err)
	}
	usedIDs := make(map[string]bool, len(claimList.Items))
	for i := range claimList.Items {
		other := &claimList.Items[i]
		if other.UID == nodeClaim.UID {
			continue // skip self
		}
		if pid := other.Status.ProviderID; pid != "" && !strings.HasPrefix(pid, PendingProviderIDPrefix) {
			usedIDs[pid] = true
		}
		if pid := other.Annotations[AdoptedProviderIDAnnotationKey]; pid != "" {
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

// getNodeFromKube answers Get from the Node carrying providerID. The termination controller calls
// Get on every reconcile of a NotReady terminating node (1s while draining), so the lookup goes
// through the cache's spec.providerID index rather than a full Node list.
func (c *CloudProvider) getNodeFromKube(ctx context.Context, providerID string) (*karpv1.NodeClaim, error) {
	node, err := c.nodeByProviderID(ctx, providerID)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, cloudprovider.NewNodeClaimNotFoundError(fmt.Errorf("no node with provider ID %q", providerID))
	}
	return nodeInfoToNodeClaim(kubeNodeToNodeInfo(node)), nil
}

// nodeByProviderID returns the Node whose spec.providerID is providerID, or nil when none carries
// it (a Terminating Node still counts). It lists through the cached client on the operator's
// nodeProviderIDIndex, so the cache answers from its index with at most one object.
func (c *CloudProvider) nodeByProviderID(ctx context.Context, providerID string) (*corev1.Node, error) {
	var list corev1.NodeList
	if err := c.kubeClient.List(ctx, &list, client.MatchingFields{nodeProviderIDIndex: providerID}); err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Spec.ProviderID == providerID {
			return &list.Items[i], nil
		}
	}
	return nil, nil
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
	its, err := c.getInstanceTypes(ctx, nodeClass)
	if err != nil {
		return nil, err
	}
	// A pool the broker reported at its platform maximum gets no available offerings until the
	// cooldown ends: the scheduler then leaves its pods pending ("no instance type has the
	// required offering") instead of creating NodeClaims the broker would refuse again. Create()
	// resolves instance types by NodeClass, not through here, so a launch already under way is
	// unaffected.
	if until, held := c.poolBackoff.Until(nodePool.Name); held {
		markOfferingsUnavailable(its)
		klog.V(2).Infof("GetInstanceTypes: pool %q is at its platform maximum; offerings unavailable until %s", nodePool.Name, until.Format(time.RFC3339))
	}
	return its, nil
}

// Reasons of the Warning events this package records on a NodePool, so `kubectl describe
// nodepool` (and anything watching events) explains what the platform did.
const (
	// PoolAtMaxEventReason: the batch failure handler holds the pool back because the broker
	// reported it at its platform maximum; the pool's pods stay pending during the hold.
	PoolAtMaxEventReason = "PoolAtPlatformMaximum"
	// PoolRefusedEventReason: same hold for every other permanent refusal of an add (pool not on
	// the cluster, SKU mismatch, pool not auto-scaling, precondition); the message is the broker's
	// detail.
	PoolRefusedEventReason = "PoolRefusedByPlatform"
	// NodeNotRetiredEventReason: a remove converged (the NodeClaim is finalized) although the
	// platform refused the count change (pool at its minimum, unknown pool, ...); the machine keeps
	// running without a Node object until its kubelet is restarted or the platform re-registers
	// it, and is then adopted back.
	NodeNotRetiredEventReason = "NodeNotRetired"
	// NodeRetiredExternallyEventReason: Delete found the NodeClaim's Node already deleted outside
	// Karpenter with its kubelet stopped — the platform (or an operator) retired the machine — and
	// reported the instance gone without sending a remove (one would retire another machine).
	NodeRetiredExternallyEventReason = "NodeRetiredExternally"
	// RemoveRetiredOtherMachineEventReason: the broker reported the remove complete but named a
	// different machine than the NodeClaim's; the NodeClaim is kept terminating (see Delete).
	RemoveRetiredOtherMachineEventReason = "RemoveRetiredOtherMachine"
)

// publishPoolAtMaxEvent records a pool-at-maximum hold on the NodePool object.
func publishPoolAtMaxEvent(ctx context.Context, reader client.Reader, recorder events.Recorder, pool, detail string, until time.Time) {
	publishPoolRefusedEvent(ctx, reader, recorder, pool, PoolAtMaxEventReason, detail, until)
}

// publishPoolRefusedEvent records a hold caused by a permanent refusal on the NodePool object.
func publishPoolRefusedEvent(ctx context.Context, reader client.Reader, recorder events.Recorder, pool, reason, detail string, until time.Time) {
	publishNodePoolEvent(ctx, reader, recorder, pool, reason,
		fmt.Sprintf("The platform refused a node for this pool (%s); not provisioning into it until %s", detail, until.Format(time.RFC3339)))
}

// publishNodePoolEvent records a Warning event on the NodePool object. Best effort: a pool that
// cannot be read (renamed, deleted, or the config sync has not applied it yet) gets no event. The
// dedupe values default to the pool name.
func publishNodePoolEvent(ctx context.Context, reader client.Reader, recorder events.Recorder, pool, reason, message string, dedupe ...string) {
	if recorder == nil || reader == nil || pool == "" {
		return
	}
	var np karpv1.NodePool
	if err := reader.Get(ctx, client.ObjectKey{Name: pool}, &np); err != nil {
		klog.V(2).Infof("pool %q %s: no event recorded, get NodePool: %v", pool, reason, err)
		return
	}
	recorder.Publish(events.Event{
		InvolvedObject: &np,
		Type:           corev1.EventTypeWarning,
		Reason:         reason,
		Message:        message,
		DedupeValues:   append([]string{pool}, dedupe...),
	})
}

// nodeClaimResources returns the instance type resource list as it is reported on a NodeClaim's
// status: without the scheduler-only "nodes" entry, so the status reads like the node's own
// capacity. Karpenter's cluster state adds the node count itself (StateNode.Capacity), so nothing
// is lost.
func nodeClaimResources(rl corev1.ResourceList) corev1.ResourceList {
	out := make(corev1.ResourceList, len(rl))
	for k, v := range rl {
		if k == resources.Node {
			continue
		}
		out[k] = v
	}
	return out
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

// instanceTypeOverhead models what a registered node does not offer to pods, so Karpenter
// bin-packs a planned node against a realistic allocatable instead of the SKU's nominal size. A
// real node reports capacity below nominal (kernel and firmware reservations) and allocatable
// below that (kube-reserved, system-reserved, the eviction threshold); advertising nominal memory
// as allocatable over-packs the planned node, the last pod does not fit once the node registers,
// and the next round provisions one node more than needed.
//
// Capacity stays nominal (it is what NodePool limits and the pool's catalog count are written
// against); the overhead only lowers InstanceType.Allocatable(). Every value is an estimate the
// deployment tunes to its kubelet configuration:
//
//	RAFAY_VM_MEMORY_OVERHEAD_PERCENT  memory the OS/kernel/firmware keep from the nominal size
//	                                  (SystemReserved; default 7.5, as other Karpenter providers use)
//	RAFAY_KUBE_RESERVED_CPU           kube-reserved cpu (default 80m)
//	RAFAY_KUBE_RESERVED_MEMORY        kube-reserved memory (default 255Mi)
//
// plus the kubelet's default hard eviction threshold (memory.available < 100Mi). The environment
// is read once; an unparseable value keeps the default with a warning.
type instanceTypeOverhead struct {
	vmMemoryPercent    float64
	kubeReservedCPU    resource.Quantity
	kubeReservedMemory resource.Quantity
	evictionMemory     resource.Quantity
}

const (
	defaultVMMemoryOverheadPercent = 7.5
	defaultKubeReservedCPU         = "80m"
	defaultKubeReservedMemory      = "255Mi"
	defaultEvictionMemory          = "100Mi"
)

var (
	overheadOnce   sync.Once
	overheadConfig instanceTypeOverhead
)

// overheadFromEnv builds the overhead model from the environment (see instanceTypeOverhead).
func overheadFromEnv() instanceTypeOverhead {
	oh := instanceTypeOverhead{
		vmMemoryPercent:    defaultVMMemoryOverheadPercent,
		kubeReservedCPU:    resource.MustParse(defaultKubeReservedCPU),
		kubeReservedMemory: resource.MustParse(defaultKubeReservedMemory),
		evictionMemory:     resource.MustParse(defaultEvictionMemory),
	}
	if v := strings.TrimSpace(os.Getenv("RAFAY_VM_MEMORY_OVERHEAD_PERCENT")); v != "" {
		if pct, err := strconv.ParseFloat(v, 64); err == nil && pct >= 0 && pct < 100 {
			oh.vmMemoryPercent = pct
		} else {
			klog.Warningf("RAFAY_VM_MEMORY_OVERHEAD_PERCENT=%q is not a percentage in [0,100); using %v", v, defaultVMMemoryOverheadPercent)
		}
	}
	for _, e := range []struct {
		name string
		dst  *resource.Quantity
	}{
		{"RAFAY_KUBE_RESERVED_CPU", &oh.kubeReservedCPU},
		{"RAFAY_KUBE_RESERVED_MEMORY", &oh.kubeReservedMemory},
	} {
		v := strings.TrimSpace(os.Getenv(e.name))
		if v == "" {
			continue
		}
		if q, err := resource.ParseQuantity(v); err == nil && q.Sign() >= 0 {
			*e.dst = q
		} else {
			klog.Warningf("%s=%q is not a non-negative quantity; using %s", e.name, v, e.dst.String())
		}
	}
	return oh
}

func currentOverhead() instanceTypeOverhead {
	overheadOnce.Do(func() { overheadConfig = overheadFromEnv() })
	return overheadConfig
}

// overheadFor renders the overhead of an instance type with the given nominal memory.
func (oh instanceTypeOverhead) overheadFor(mem resource.Quantity) *cloudprovider.InstanceTypeOverhead {
	vm := resource.NewQuantity(int64(float64(mem.Value())*oh.vmMemoryPercent/100), resource.BinarySI)
	return &cloudprovider.InstanceTypeOverhead{
		KubeReserved: corev1.ResourceList{
			corev1.ResourceCPU:    oh.kubeReservedCPU.DeepCopy(),
			corev1.ResourceMemory: oh.kubeReservedMemory.DeepCopy(),
		},
		SystemReserved: corev1.ResourceList{
			corev1.ResourceMemory: *vm,
		},
		EvictionThreshold: corev1.ResourceList{
			corev1.ResourceMemory: oh.evictionMemory.DeepCopy(),
		},
	}
}

// trimmedValues drops blank entries and surrounding whitespace from a label-value list.
func trimmedValues(vals []string) []string {
	out := make([]string, 0, len(vals))
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func rafayInstanceTypesToKarpenter(specs []v1alpha1.InstanceTypeSpec) ([]*cloudprovider.InstanceType, error) {
	overhead := currentOverhead()
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
			// One machine per instance type, so NodePool.spec.limits.nodes (the broker renders the
			// pool's maxNodeCount there) holds WITHIN a scheduling round, not only between rounds.
			// Karpenter's scheduler subtracts an existing node's capacity from the pool's remaining
			// limits with "nodes: 1" added by the cluster state, but subtracts a NodeClaim it has
			// just decided to create using the instance type's capacity as declared here. Without
			// this entry a pool with one slot left and two pending pods gets two NodeClaims, the
			// broker refuses the batch, and Karpenter re-asks every few minutes (see docs/
			// architecture.md, "Pool maximum: three layers"). Stripped again before the value is
			// copied onto a NodeClaim's status (nodeClaimResources).
			resources.Node: resource.MustParse("1"),
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
		// Every well-known label is defined (Karpenter's InstanceType contract): the scheduler
		// checks compatibility with AllowUndefinedWellKnownLabels, so an undefined arch would let a
		// pod selecting arm64 land on this SKU. Arch and os default when the class omits them; the
		// region has no platform value, so it is declared as Exists (defined, no label stamped).
		archVals := trimmedValues(s.Architectures)
		if len(archVals) == 0 {
			archVals = []string{defaultArchitecture}
		}
		osVals := trimmedValues(s.OperatingSystems)
		if len(osVals) == 0 {
			osVals = []string{defaultOperatingSystem}
		}
		reqs := scheduling.NewRequirements(
			scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, s.Name),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
			scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpExists),
			scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
			scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, archVals...),
			scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, osVals...),
		)
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
			Overhead:     overhead.overheadFor(mem),
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
// ProviderID are deleted.
//
// Every terminal FAILED operation is first recorded in poolBackoff (PoolBackoff.MarkFailedOp) —
// for adds unconditionally, for removes only when the broker refused them permanently
// (IsPermanentRefusalDetail). Delete() reads those records: a pending NodeClaim whose add FAILED
// has no machine to remove, and a remove the broker will refuse again must not be re-sent every
// 5s. Other remove failures are logged only (Karpenter retries Delete as long as the node object
// exists).
//
// Some add failures are not transient: the broker refusing the node because the pool is at its
// platform maximum, is not on the cluster, has another SKU, is not auto-scaling or fails a
// precondition (IsPermanentRefusalDetail). Reprovisioning right away would be refused again, so
// before deleting the NodeClaim the handler marks its NodePool in poolBackoff; GetInstanceTypes
// then withholds the pool's offerings for the cooldown and Karpenter leaves the pods pending
// instead of re-asking every few minutes. The NodeClaim is still deleted — kept, it would be an
// in-flight node the scheduler expects the pods to land on, for the full 60-minute registration
// timeout. A nil poolBackoff keeps the plain delete-and-reprovision behaviour. The hold is also
// published as a Warning event on the NodePool (PoolAtMaxEventReason for the maximum,
// PoolRefusedEventReason otherwise, message = the broker's detail) when recorder is non-nil: the
// pods' own scheduling message during the hold ("nodepool requirements filtered out all
// available instance types") does not say why.
func NewBatchFailureHandler(kubeClient client.Client, apiReader client.Reader, poolBackoff *PoolBackoff, recorder events.Recorder) rafay.FailureHandler {
	return func(ctx context.Context, operationID, kind, detail string) {
		if kind != "add" {
			if IsPermanentRefusalDetail(detail) {
				poolBackoff.MarkFailedOp(operationID, detail)
				klog.Warningf("batch %s operation failed operationID=%s: %s — refused permanently; Delete() will converge without re-sending it", kind, operationID, detail)
				return
			}
			klog.Warningf("batch %s operation failed operationID=%s: %s", kind, operationID, detail)
			return
		}
		// Recorded before anything else: the NodeClaim may already be terminating (left alone
		// below) and its Delete() must learn that no machine is coming for this add.
		poolBackoff.MarkFailedOp(operationID, detail)
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
			if IsPermanentRefusalDetail(detail) {
				pool := nc.Labels[karpv1.NodePoolLabelKey]
				if until := poolBackoff.Mark(pool); !until.IsZero() {
					reason := PoolRefusedEventReason
					if IsPoolAtMaxDetail(detail) {
						reason = PoolAtMaxEventReason
					}
					klog.Warningf("batch add failed operationID=%s (%s): the platform refused pool %q permanently; holding back provisioning for it until %s", operationID, detail, pool, until.Format(time.RFC3339))
					publishPoolRefusedEvent(ctx, apiReader, recorder, pool, reason, detail, until)
				}
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
