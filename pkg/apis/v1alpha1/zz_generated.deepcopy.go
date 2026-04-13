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
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto copies the receiver into out.
func (in *InstanceTypeSpec) DeepCopyInto(out *InstanceTypeSpec) {
	*out = *in
}

// DeepCopy returns a copy of the receiver.
func (in *InstanceTypeSpec) DeepCopy() *InstanceTypeSpec {
	if in == nil {
		return nil
	}
	out := new(InstanceTypeSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *RafayNodeClassSpec) DeepCopyInto(out *RafayNodeClassSpec) {
	*out = *in
	if in.InstanceTypes != nil {
		in, out := &in.InstanceTypes, &out.InstanceTypes
		*out = make([]InstanceTypeSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a copy of the receiver.
func (in *RafayNodeClassSpec) DeepCopy() *RafayNodeClassSpec {
	if in == nil {
		return nil
	}
	out := new(RafayNodeClassSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *RafayNodeClassStatus) DeepCopyInto(out *RafayNodeClassStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]status.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a copy of the receiver.
func (in *RafayNodeClassStatus) DeepCopy() *RafayNodeClassStatus {
	if in == nil {
		return nil
	}
	out := new(RafayNodeClassStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *RafayNodeClass) DeepCopyInto(out *RafayNodeClass) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy returns a copy of the receiver.
func (in *RafayNodeClass) DeepCopy() *RafayNodeClass {
	if in == nil {
		return nil
	}
	out := new(RafayNodeClass)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *RafayNodeClass) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out.
func (in *RafayNodeClassList) DeepCopyInto(out *RafayNodeClassList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]RafayNodeClass, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy returns a copy of the receiver.
func (in *RafayNodeClassList) DeepCopy() *RafayNodeClassList {
	if in == nil {
		return nil
	}
	out := new(RafayNodeClassList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject implements runtime.Object.
func (in *RafayNodeClassList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
