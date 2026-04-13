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
type RafayNodeClassSpec struct {
	// ClusterID is the Rafay cluster identifier to add/remove nodes in.
	// +optional
	ClusterID string `json:"clusterID,omitempty"`

	// ProjectID is the Rafay project identifier when required by the platform.
	// +optional
	ProjectID string `json:"projectID,omitempty"`

	// InstanceTypes define the node shapes available for provisioning.
	// If empty, a default set for private cloud is used.
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
	// Zone for this offering (e.g. zone-a). Used for topology.kubernetes.io/zone.
	// +optional
	Zone string `json:"zone,omitempty"`
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
func (nc *RafayNodeClass) StatusConditions() status.ConditionSet {
	return status.NewReadyConditions().For(nc)
}

func (nc *RafayNodeClass) GetConditions() []status.Condition {
	return nc.Status.Conditions
}

func (nc *RafayNodeClass) SetConditions(conditions []status.Condition) {
	nc.Status.Conditions = conditions
}
