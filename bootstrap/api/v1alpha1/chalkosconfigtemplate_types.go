/*
Copyright 2026 Niklas Voss.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// ChalkosConfigTemplateSpec is the desired state of ChalkosConfigTemplate.
type ChalkosConfigTemplateSpec struct {
	// Template is the ChalkosConfig that MachineDeployments and MachineSets clone for each
	// Machine.
	// +required
	Template ChalkosConfigTemplateResource `json:"template,omitempty,omitzero"`
}

// ChalkosConfigTemplateResource is the template for a cloned ChalkosConfig.
// +kubebuilder:validation:MinProperties=1
type ChalkosConfigTemplateResource struct {
	// ObjectMeta is the metadata the cloned ChalkosConfig starts with.
	// +optional
	ObjectMeta clusterv1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// Spec is the cloned ChalkosConfig's spec.
	// +optional
	Spec ChalkosConfigTemplateResourceSpec `json:"spec,omitempty,omitzero"`
}

// ChalkosConfigTemplateResourceSpec is the cloned ChalkosConfig's spec. It is ChalkosConfigSpec
// with Image optional: a MachineDeployment's or MachineSet's rollout may compute it for each
// Machine that uses it, instead.
type ChalkosConfigTemplateResourceSpec struct {
	// Image is the chalkos image the machine installs, by digest: repository@sha256:….
	// +optional
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-f0-9]{64}$`
	Image string `json:"image,omitempty"`

	// ImagePullSecretRef names a Secret of type kubernetes.io/dockerconfigjson in the
	// ChalkosConfig's namespace whose credentials the controller reads the image with. The node
	// pulls with the Registry documents among Files.
	// +optional
	ImagePullSecretRef *corev1.LocalObjectReference `json:"imagePullSecretRef,omitempty"`

	// Files are chalkos config documents of the machine's pool, such as a Node's labels and taints.
	// A Cluster document belongs in the ChalkosControlPlane's files and is refused here. Fields the
	// controllers own are refused: the Node's type and providerID, the Cluster's endpoint, and its
	// ipFamilies, podCIDRs and serviceCIDRs when the Cluster's clusterNetwork gives ranges.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Files []runtime.RawExtension `json:"files,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=chalkosconfigtemplates,scope=Namespaced,categories=cluster-api
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation of ChalkosConfigTemplate"

// ChalkosConfigTemplate is the ChalkosConfig a MachineDeployment or MachineSet clones for each
// Machine it creates.
type ChalkosConfigTemplate struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired state of ChalkosConfigTemplate.
	// +optional
	Spec ChalkosConfigTemplateSpec `json:"spec,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// ChalkosConfigTemplateList contains a list of ChalkosConfigTemplate.
type ChalkosConfigTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ChalkosConfigTemplate `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &ChalkosConfigTemplate{}, &ChalkosConfigTemplateList{})
}
