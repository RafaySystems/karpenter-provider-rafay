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

package nodeadoption

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"

	"github.com/RafaySystems/karpenter-provider-rafay/pkg/rafay"
)

// adNewZonedInstanceType is newInstanceType for a RafayNodeClass entry that declares a zone.
func adNewZonedInstanceType(name, arch, zone string) *cloudprovider.InstanceType {
	reqs := scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, name),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
		scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, arch),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, "linux"),
	)
	return &cloudprovider.InstanceType{
		Name:         name,
		Requirements: reqs,
		Offerings:    cloudprovider.Offerings{{Requirements: reqs, Price: 1, Available: true}},
		Capacity:     corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")},
		Overhead:     &cloudprovider.InstanceTypeOverhead{},
	}
}

// TestAdCapacityTypeOf: the capacity type is read off the first offering, falling back to
// on-demand when the instance type has no offerings. (An offering whose requirements lack the
// capacity-type key is not covered: Requirements.Get on an absent key yields an Exists
// requirement whose Any() is a random value, so the `ct != ""` fallback never fires for it. Every
// Rafay offering states its capacity type, so that path is unreachable in practice.)
func TestAdCapacityTypeOf(t *testing.T) {
	spot := scheduling.NewRequirements(
		scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeSpot),
	)
	tests := []struct {
		name string
		it   *cloudprovider.InstanceType
		want string
	}{
		{name: "on-demand offering", it: newInstanceType(testSKU, "amd64"), want: karpv1.CapacityTypeOnDemand},
		{name: "spot offering", it: &cloudprovider.InstanceType{Offerings: cloudprovider.Offerings{{Requirements: spot}}}, want: karpv1.CapacityTypeSpot},
		{name: "no offerings", it: &cloudprovider.InstanceType{Name: testSKU}, want: karpv1.CapacityTypeOnDemand},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := capacityTypeOf(tt.it); got != tt.want {
				t.Errorf("capacityTypeOf = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestAdEqualNodes: only spec.providerID and the karpenter.sh/registered label decide whether
// prepareNode has anything to write; any other difference is not this controller's business.
func TestAdEqualNodes(t *testing.T) {
	base := func() *corev1.Node {
		return newNode("worker-a", withProviderID("rafay://pool1/oci-inst/worker-a"), withLabel(karpv1.NodeRegisteredLabelKey, "true"))
	}
	tests := []struct {
		name string
		b    *corev1.Node
		want bool
	}{
		{name: "identical", b: base(), want: true},
		{name: "different providerID", b: newNode("worker-a", withProviderID("rafay://pool1/oci-inst/other"), withLabel(karpv1.NodeRegisteredLabelKey, "true")), want: false},
		{name: "empty providerID", b: newNode("worker-a", withLabel(karpv1.NodeRegisteredLabelKey, "true")), want: false},
		{name: "registered label missing", b: newNode("worker-a", withProviderID("rafay://pool1/oci-inst/worker-a")), want: false},
		{name: "registered label false", b: newNode("worker-a", withProviderID("rafay://pool1/oci-inst/worker-a"), withLabel(karpv1.NodeRegisteredLabelKey, "false")), want: false},
		{name: "unrelated label differs", b: newNode("worker-a", withProviderID("rafay://pool1/oci-inst/worker-a"), withLabel(karpv1.NodeRegisteredLabelKey, "true"), withLabel("extra", "x")), want: true},
		{name: "nil labels on both sides", b: func() *corev1.Node { n := base(); n.Labels = nil; return n }(), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := equalNodes(base(), tt.b); got != tt.want {
				t.Errorf("equalNodes = %t, want %t", got, tt.want)
			}
		})
	}
	// Symmetric and reflexive on a node with no labels at all.
	bare := &corev1.Node{}
	if !equalNodes(bare, bare) {
		t.Error("equalNodes(bare, bare) = false, want true")
	}
}

// TestAdPrepareNode covers each write prepareNode may or may not make: nothing when the node is
// already prepared, only the label when the platform stamped the ID, both when it did not, and an
// error when no ID can be built.
func TestAdPrepareNode(t *testing.T) {
	const platformID = "rafay://pool1/oci-inst/host-w0-af1cc"
	builtID := rafay.BuildProviderID(testPool, testSKU, "worker-a")
	tests := []struct {
		name        string
		node        *corev1.Node
		sku         string
		wantID      string
		wantPatches int
		wantErr     string
	}{
		{
			name:        "already prepared is a no-op",
			node:        newNode("worker-a", withProviderID(platformID), withLabel(karpv1.NodeRegisteredLabelKey, "true")),
			sku:         testSKU,
			wantID:      platformID,
			wantPatches: 0,
		},
		{
			name:        "platform ID kept, label added",
			node:        newNode("worker-a", withProviderID(platformID)),
			sku:         testSKU,
			wantID:      platformID,
			wantPatches: 1,
		},
		{
			name:        "ID built and label added",
			node:        newNode("worker-a"),
			sku:         testSKU,
			wantID:      builtID,
			wantPatches: 1,
		},
		{
			name:        "label present but ID missing still builds the ID",
			node:        newNode("worker-a", withLabel(karpv1.NodeRegisteredLabelKey, "true")),
			sku:         testSKU,
			wantID:      builtID,
			wantPatches: 1,
		},
		{
			name:    "unbuildable ID is an error before any write",
			node:    newNode("worker-a"),
			sku:     "",
			wantErr: "cannot build a provider ID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := 0
			c := adNewInterceptedClient(interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches++
					return cl.Patch(ctx, obj, patch, opts...)
				},
			}, tt.node)
			ctrl, _ := newTestController(c)

			got, err := ctrl.prepareNode(context.Background(), tt.node.DeepCopy(), testPool, tt.sku)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("prepareNode err = %v, want %q", err, tt.wantErr)
				}
				if patches != 0 {
					t.Errorf("patches = %d, want 0", patches)
				}
				return
			}
			if err != nil {
				t.Fatalf("prepareNode: %v", err)
			}
			if got != tt.wantID {
				t.Errorf("providerID = %q, want %q", got, tt.wantID)
			}
			if patches != tt.wantPatches {
				t.Errorf("patches = %d, want %d", patches, tt.wantPatches)
			}
			stored := getNode(t, c, tt.node.Name)
			if stored.Spec.ProviderID != tt.wantID {
				t.Errorf("stored providerID = %q, want %q", stored.Spec.ProviderID, tt.wantID)
			}
			if stored.Labels[karpv1.NodeRegisteredLabelKey] != "true" {
				t.Errorf("stored %s = %q, want \"true\"", karpv1.NodeRegisteredLabelKey, stored.Labels[karpv1.NodeRegisteredLabelKey])
			}
		})
	}
}

// TestAdAdoptedNodeClaimCarriesTemplateMetadata: an adopted NodeClaim is the pool's template plus
// the adoption markers — its labels, annotations, nodeClassRef and expiry come from the template so
// the claim is indistinguishable from a provisioned one for accounting and disruption.
func TestAdAdoptedNodeClaimCarriesTemplateMetadata(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	pool.Spec.Template.Labels["team"] = "platform"
	pool.Spec.Template.Annotations = map[string]string{"catalog/row": "42"}
	pool.Spec.Template.Spec.ExpireAfter = karpv1.MustParseNillableDuration("Never")
	pool.Spec.Template.Spec.TerminationGracePeriod = &metav1.Duration{Duration: 90 * time.Second}
	node := newNode("worker-a", withLabel(corev1.LabelTopologyRegion, "us-ashburn-1"))
	it := newInstanceType(testSKU, "amd64")

	nc := adoptedNodeClaim(pool, node, testSKU, it)

	if got := nc.Labels["team"]; got != "platform" {
		t.Errorf("template label team = %q, want platform", got)
	}
	if got := nc.Annotations["catalog/row"]; got != "42" {
		t.Errorf("template annotation catalog/row = %q, want 42", got)
	}
	if got := nc.Annotations[adoptedNodeAnnotationKey]; got != "worker-a" {
		t.Errorf("%s = %q, want worker-a", adoptedNodeAnnotationKey, got)
	}
	if nc.Spec.NodeClassRef == nil || *nc.Spec.NodeClassRef != *pool.Spec.Template.Spec.NodeClassRef {
		t.Errorf("nodeClassRef = %+v, want the template's %+v", nc.Spec.NodeClassRef, pool.Spec.Template.Spec.NodeClassRef)
	}
	if nc.Spec.ExpireAfter.Duration != nil {
		t.Errorf("expireAfter = %v, want Never/nil (from the template)", *nc.Spec.ExpireAfter.Duration)
	}
	if nc.Spec.TerminationGracePeriod == nil || nc.Spec.TerminationGracePeriod.Duration != 90*time.Second {
		t.Errorf("terminationGracePeriod = %v, want 90s (from the template)", nc.Spec.TerminationGracePeriod)
	}
	if got := nc.Labels[corev1.LabelTopologyRegion]; got != "us-ashburn-1" {
		t.Errorf("region label = %q, want the node's us-ashburn-1", got)
	}
	if got := nc.Labels[corev1.LabelInstanceTypeStable]; got != testSKU {
		t.Errorf("instance-type label = %q, want %q", got, testSKU)
	}
	if len(nc.OwnerReferences) != 1 {
		t.Fatalf("owner references = %+v, want exactly one", nc.OwnerReferences)
	}
	or := nc.OwnerReferences[0]
	if or.Kind != "NodePool" || or.Name != testPool || or.UID != pool.UID || or.BlockOwnerDeletion == nil || !*or.BlockOwnerDeletion {
		t.Errorf("owner reference = %+v, want NodePool %s with BlockOwnerDeletion", or, testPool)
	}
	// The requirement pinning must reflect the node's SKU, and the instance-type label must
	// satisfy it (otherwise validateAdoptable rejects the claim it just built).
	var pinned []string
	for _, r := range nc.Spec.Requirements {
		if r.Key == corev1.LabelInstanceTypeStable {
			pinned = append(pinned, r.Values...)
		}
	}
	if len(pinned) != 1 || pinned[0] != testSKU {
		t.Errorf("instance-type requirement values = %v, want [%s]", pinned, testSKU)
	}
	if err := validateAdoptable(pool, it, nc.Labels); err != nil {
		t.Errorf("validateAdoptable on the built claim = %v, want nil", err)
	}
	// The template must not be mutated by building a claim from it.
	if pool.Spec.Template.Annotations[adoptedNodeAnnotationKey] != "" {
		t.Error("adoptedNodeClaim wrote the adoption annotation back onto the pool template")
	}
}

// TestAdWellKnownNodeLabels: the SKU supplies only instance-type and capacity-type; every other
// well-known label is copied from the node when present and absent otherwise, never inferred.
func TestAdWellKnownNodeLabels(t *testing.T) {
	it := newInstanceType(testSKU, "amd64")
	t.Run("node with full topology", func(t *testing.T) {
		node := newNode("worker-a",
			withLabel(corev1.LabelTopologyZone, "zone-a"),
			withLabel(corev1.LabelTopologyRegion, "region-1"),
		)
		got := wellKnownNodeLabels(node, testSKU, it)
		want := map[string]string{
			corev1.LabelInstanceTypeStable: testSKU,
			karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
			corev1.LabelArchStable:         "amd64",
			corev1.LabelOSStable:           "linux",
			corev1.LabelTopologyZone:       "zone-a",
			corev1.LabelTopologyRegion:     "region-1",
		}
		if len(got) != len(want) {
			t.Errorf("labels = %v, want %v", got, want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("label %s = %q, want %q", k, got[k], v)
			}
		}
	})
	t.Run("node without arch or os", func(t *testing.T) {
		node := newNode("worker-a", withoutLabel(corev1.LabelArchStable), withoutLabel(corev1.LabelOSStable))
		got := wellKnownNodeLabels(node, testSKU, it)
		for _, k := range []string{corev1.LabelArchStable, corev1.LabelOSStable, corev1.LabelTopologyZone, corev1.LabelTopologyRegion} {
			if v, ok := got[k]; ok {
				t.Errorf("label %s = %q, want it absent (the SKU's value must not be inferred)", k, v)
			}
		}
		if len(got) != 2 {
			t.Errorf("labels = %v, want only instance-type and capacity-type", got)
		}
	})
}

// TestAdPinInstanceTypeNoExistingRequirement: a template with no instance-type requirement (a
// multi-SKU class left open) gains exactly one, and unrelated requirements keep their MinValues.
func TestAdPinInstanceTypeNoExistingRequirement(t *testing.T) {
	two := 2
	reqs := []karpv1.NodeSelectorRequirementWithMinValues{
		{Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"linux"}, MinValues: &two},
	}
	out := pinInstanceType(reqs, testSKU)
	if len(out) != 2 {
		t.Fatalf("got %d requirements, want 2", len(out))
	}
	if out[0].Key != corev1.LabelOSStable || out[0].MinValues == nil || *out[0].MinValues != 2 {
		t.Errorf("first requirement = %+v, want the OS requirement with MinValues intact", out[0])
	}
	last := out[1]
	if last.Key != corev1.LabelInstanceTypeStable || last.Operator != corev1.NodeSelectorOpIn || len(last.Values) != 1 || last.Values[0] != testSKU {
		t.Errorf("appended requirement = %+v, want instance-type In [%s]", last, testSKU)
	}
	if pinned := pinInstanceType(nil, testSKU); len(pinned) != 1 {
		t.Errorf("pinInstanceType(nil) = %+v, want exactly the pin", pinned)
	}
}

// TestAdNodeToNodePoolNonNode: the map function ignores objects that are not nodes.
func TestAdNodeToNodePoolNonNode(t *testing.T) {
	if got := nodeToNodePool(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p"}}); got != nil {
		t.Errorf("nodeToNodePool(pod) = %+v, want nil", got)
	}
}

// TestAdNodePredicatePassesRelevantEvents pins the Node watch filter: a node carrying both pool
// labels reaches nodeToNodePool on create, delete and on the updates adoption acts on (Ready flip,
// providerID stamped, relabel); a node without them, or a non-node, never does. Unlike the
// NodeProviderIDController's predicate, an empty spec.providerID must pass.
func TestAdNodePredicatePassesRelevantEvents(t *testing.T) {
	labelled := newNode("worker-a")
	notReady := newNode("worker-a", withReady(corev1.ConditionFalse))
	withID := newNode("worker-a", withProviderID("rafay://pool1/oci-inst/worker-a"))
	unlabelled := newNode("worker-a", withoutLabel(nodepoolNameLabel), withoutLabel(skuNameLabel))
	noSKU := newNode("worker-a", withoutLabel(skuNameLabel))

	if !rafayNodePredicate.Create(event.CreateEvent{Object: labelled}) {
		t.Error("Create(labelled node without providerID) = false, want true")
	}
	if rafayNodePredicate.Create(event.CreateEvent{Object: unlabelled}) {
		t.Error("Create(unlabelled node) = true, want false")
	}
	if rafayNodePredicate.Create(event.CreateEvent{Object: noSKU}) {
		t.Error("Create(node without sku_name) = true, want false")
	}
	if rafayNodePredicate.Create(event.CreateEvent{Object: &corev1.Pod{}}) {
		t.Error("Create(pod) = true, want false")
	}
	if !rafayNodePredicate.Delete(event.DeleteEvent{Object: labelled}) {
		t.Error("Delete(labelled node) = false, want true")
	}
	if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: notReady, ObjectNew: labelled}) {
		t.Error("Update(NotReady -> Ready) = false, want true")
	}
	if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: labelled, ObjectNew: withID}) {
		t.Error("Update(providerID stamped) = false, want true")
	}
	if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: unlabelled, ObjectNew: labelled}) {
		t.Error("Update(pool labels added) = false, want true")
	}
	if rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: unlabelled, ObjectNew: unlabelled}) {
		t.Error("Update(unlabelled node) = true, want false")
	}
}

// TestAdNodePredicateIgnoresStatusHeartbeat: a kubelet status heartbeat changes nothing adoption
// decides on, and each pass it triggers costs an uncached NodeClaim LIST. An Update whose only
// difference is LastHeartbeatTime must not enqueue the pool.
func TestAdNodePredicateIgnoresStatusHeartbeat(t *testing.T) {

	old := newNode("worker-a")
	old.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(baseTime)
	updated := old.DeepCopy()
	updated.Status.Conditions[0].LastHeartbeatTime = metav1.NewTime(baseTime.Add(10 * time.Second))
	updated.ResourceVersion = "2"

	if rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: updated}) {
		t.Error("Update(heartbeat only) = true, want false")
	}
	// The changes that matter still pass.
	ready := old.DeepCopy()
	ready.Status.Conditions[0].Status = corev1.ConditionFalse
	if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: ready, ObjectNew: updated}) {
		t.Error("Update(Ready False -> True) = false, want true")
	}
}

// TestAdAdoptedClaimCarriesHashVersion: the NodeClaim is created at the current
// karpenter.sh/nodepool-hash-version (and without karpenter.sh/nodepool-hash), which is what makes
// Karpenter's nodepool hash controller skip it when it back-fills hashes onto stale claims.
func TestAdAdoptedClaimCarriesHashVersion(t *testing.T) {
	pool := newNodePool(testPool, testSKU)
	nc := adoptedNodeClaim(pool, newNode("worker-a"), testSKU, newInstanceType(testSKU, "amd64"))
	if got := nc.Annotations[karpv1.NodePoolHashVersionAnnotationKey]; got != karpv1.NodePoolHashVersion {
		t.Errorf("%s = %q, want %q", karpv1.NodePoolHashVersionAnnotationKey, got, karpv1.NodePoolHashVersion)
	}
	if v, ok := nc.Annotations[karpv1.NodePoolHashAnnotationKey]; ok {
		t.Errorf("%s = %q, want it omitted", karpv1.NodePoolHashAnnotationKey, v)
	}
}

// TestAdNodePredicateUpdateEdges pins the remaining Update branches: an old object that is not a
// node is not compared (the event passes on the new node alone); a role label appearing is a
// label change; a node that lost its pool labels is dropped even though the old one had them.
func TestAdNodePredicateUpdateEdges(t *testing.T) {
	labelled := newNode("worker-a")
	if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: &corev1.Pod{}, ObjectNew: labelled}) {
		t.Error("Update(non-node old, labelled new) = false, want true")
	}
	roled := newNode("worker-a", withLabel("node-role.kubernetes.io/control-plane", ""))
	if !rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: labelled, ObjectNew: roled}) {
		t.Error("Update(role label added) = false, want true")
	}
	unlabelled := newNode("worker-a", withoutLabel(nodepoolNameLabel))
	if rafayNodePredicate.Update(event.UpdateEvent{ObjectOld: labelled, ObjectNew: unlabelled}) {
		t.Error("Update(pool label removed) = true, want false")
	}
	if rafayNodePredicate.Generic(event.GenericEvent{Object: unlabelled}) {
		t.Error("Generic(unlabelled node) = true, want false")
	}
	if !rafayNodePredicate.Generic(event.GenericEvent{Object: labelled}) {
		t.Error("Generic(labelled node) = false, want true")
	}
}
