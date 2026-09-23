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

package v1alpha1

import (
	"github.com/awslabs/operatorpkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RafayNodeClassSpec defines configuration for provisioning nodes via edge-broker (gRPC).
//
// instanceTypes is the only field: a RafayNodeClass is a pure, cluster-agnostic instance
// catalog, so the same manifest applies unchanged to every cluster. Rafay cluster/project
// identity is deliberately NOT expressed here — it comes solely from the controller's
// RAFAY_CLUSTER_ID / RAFAY_PROJECT_ID env, captured into CloudProvider.clusterID/.projectID
// by NewCloudProvider. Do not reintroduce per-NodeClass overrides.
//
// The CRD's spec is a structural schema listing exactly these fields, so a stray
// spec.clusterID is rejected by kubectl rather than silently ignored. Adding a field here
// without adding it to config/crd/karpenter.rafay.io_rafaynodeclasses.yaml means the API
// server prunes it before the controller ever sees it.
type RafayNodeClassSpec struct {
	// InstanceTypes define the node shapes available for provisioning.
	// At least one entry is required; the provider returns an error if this list is empty.
	// +optional
	InstanceTypes []InstanceTypeSpec `json:"instanceTypes,omitempty"`
}

// InstanceTypeSpec describes a node size available via Rafay.
type InstanceTypeSpec struct {
	// Name must match node.kubernetes.io/instance-type (e.g. standard-4-8).
	// +required
	Name string `json:"name"`
	// CPU capacity (e.g. "4", "8000m").
	// +required
	CPU string `json:"cpu"`
	// Memory capacity (e.g. "8Gi").
	// +required
	Memory string `json:"memory"`
	// GPU is the nvidia.com/gpu capacity this SKU advertises (e.g. "8"). Without it no instance
	// type declares an accelerator, so Karpenter's scheduler filters every one of them out for a
	// pod requesting nvidia.com/gpu ("no instance type has enough resources") and never creates a
	// NodeClaim — the pod just stays Pending.
	//
	// TEMPORARY — remove this field together with edge-broker's temporaryGPUCapacity /
	// instanceTypeGPUCapacity / yamlInstanceType.GPU, the CRD property, and the Capacity entry in
	// rafayInstanceTypesToKarpenter. Two things make it a stopgap rather than real GPU support:
	//
	//   - The resource name is hardcoded to nvidia.com/gpu, so an amd.com/gpu SKU is advertised
	//     under the wrong key. The broker already resolves the real name per SKU
	//     (KarpenterNodeSku.GPUResourceName) — the replacement should carry that through instead.
	//   - The broker falls back to a fixed count for any SKU whose ComputeProfile declares no
	//     gpu_count, which includes SKUs that have no accelerator at all. Such a node registers
	//     but never reports nvidia.com/gpu in allocatable, so its NodeClaim is stuck
	//     Initialized=Unknown forever (there is no Initialized timeout — see
	//     nodeclaim/lifecycle/liveness.go, which only reaps NodeClaims that fail to Register).
	// +optional
	GPU string `json:"nvidia.com/gpu,omitempty"`
	// Zone for this offering (e.g. zone-a). Used for topology.kubernetes.io/zone.
	// +optional
	Zone string `json:"zone,omitempty"`
	// Architectures lists CPU architectures this SKU supports (well-known label kubernetes.io/arch),
	// e.g. ["amd64"], ["arm64"], or ["amd64","arm64"]. When set, Karpenter only matches NodeClaims whose
	// requirements intersect these values. When empty, this instance type does not constrain architecture
	// (legacy behavior: any arch allowed on the NodeClaim still matches).
	// +optional
	Architectures []string `json:"architectures,omitempty"`
	// OperatingSystems lists OS values this SKU supports (well-known label kubernetes.io/os),
	// e.g. ["linux"]. When set, NodePool/NodeClaim requirements such as kubernetes.io/os In [linux] must
	// intersect these values. When empty, this instance type does not constrain OS.
	// +optional
	OperatingSystems []string `json:"operatingSystems,omitempty"`
}

// RafayNodeClassStatus is the status for RafayNodeClass.
type RafayNodeClassStatus struct {
	// Conditions contain readiness and health signals.
	// +optional
	Conditions []status.Condition `json:"conditions,omitempty"`
}

// RafayNodeClass is the Schema for the RafayNodeClass API (private cloud).
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=rafaynodeclasses,scope=Cluster,categories=karpenter,shortName={rnc,rncs}
// +kubebuilder:subresource:status
type RafayNodeClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RafayNodeClassSpec   `json:"spec,omitempty"`
	Status RafayNodeClassStatus `json:"status,omitempty"`
}

// RafayNodeClassList contains a list of RafayNodeClass.
// +kubebuilder:object:root=true
type RafayNodeClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RafayNodeClass `json:"items"`
}

// StatusConditions / GetConditions / SetConditions for operatorpkg status.Object.
func (nc *RafayNodeClass) StatusConditions(opts ...status.ForOption) status.ConditionSet {
	return status.NewReadyConditions().For(nc, opts...)
}

func (nc *RafayNodeClass) GetConditions() []status.Condition {
	return nc.Status.Conditions
}

func (nc *RafayNodeClass) SetConditions(conditions []status.Condition) {
	nc.Status.Conditions = conditions
}
