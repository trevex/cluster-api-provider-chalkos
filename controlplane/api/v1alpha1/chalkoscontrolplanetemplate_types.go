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

// ChalkosControlPlaneTemplateSpec is the desired state of ChalkosControlPlaneTemplate.
type ChalkosControlPlaneTemplateSpec struct {
	// Template is the ChalkosControlPlane a ClusterClass's control plane is instantiated from.
	// +required
	Template ChalkosControlPlaneTemplateResource `json:"template,omitempty,omitzero"`
}

// ChalkosControlPlaneTemplateResource is the template a ClusterClass's control plane is
// instantiated from.
// +kubebuilder:validation:MinProperties=1
type ChalkosControlPlaneTemplateResource struct {
	// ObjectMeta is the metadata the instantiated ChalkosControlPlane starts with.
	// +optional
	ObjectMeta clusterv1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// Spec is the instantiated ChalkosControlPlane's spec.
	// +optional
	Spec ChalkosControlPlaneTemplateResourceSpec `json:"spec,omitempty,omitzero"`
}

// ChalkosControlPlaneTemplateResourceSpec is the instantiated ChalkosControlPlane's spec. It is
// ChalkosControlPlaneSpec without Replicas, Version and MachineTemplate.Spec.InfrastructureRef: a
// ClusterClass's topology computes those for every Cluster that uses it.
type ChalkosControlPlaneTemplateResourceSpec struct {
	// Image is the chalkos image the control planes install, by digest: repository@sha256:….
	// A ClusterClass's topology may compute this for each Cluster that uses it, instead.
	// +optional
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-f0-9]{64}$`
	Image string `json:"image,omitempty"`

	// ImagePullSecretRef names a Secret of type kubernetes.io/dockerconfigjson in the
	// ChalkosControlPlane's namespace whose credentials the controller reads the image with. The
	// node pulls with the Registry documents among Files.
	// +optional
	ImagePullSecretRef *corev1.LocalObjectReference `json:"imagePullSecretRef,omitempty"`

	// MachineTemplate is what the control planes' Machines are made from, without the
	// infrastructure reference that a ClusterClass's topology computes for each Cluster.
	// +optional
	MachineTemplate ChalkosControlPlaneTemplateMachineTemplate `json:"machineTemplate,omitempty,omitzero"`

	// Files are the cluster-wide chalkos config documents every node of the cluster gets (such as
	// Flannel, Addon and Registry documents), together with the control planes' own Node fields.
	// They must hold the Cluster document, which names the etcd, coredns, pause and flannel images;
	// the controllers derive the API server's, controller-manager's, scheduler's and kube-proxy's
	// from the version where it names none. Fields the controllers own are refused: the Node's type
	// and providerID, the Cluster's endpoint, and its ipFamilies, podCIDRs and serviceCIDRs when the
	// Cluster's clusterNetwork gives ranges.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Files []runtime.RawExtension `json:"files,omitempty"`

	// Kubeconfig says how the cluster's kubeconfig Secret's certificate is issued.
	// +optional
	// +kubebuilder:default={}
	Kubeconfig KubeconfigSpec `json:"kubeconfig,omitempty,omitzero"`
}

// ChalkosControlPlaneTemplateMachineTemplate is what a ClusterClass's control planes' Machines
// are made from. It is ChalkosControlPlaneMachineTemplate without Spec.InfrastructureRef: a
// ClusterClass's topology computes that for each Cluster that uses it.
// +kubebuilder:validation:MinProperties=1
type ChalkosControlPlaneTemplateMachineTemplate struct {
	// ObjectMeta is the metadata the control planes' Machines start with.
	// +optional
	ObjectMeta clusterv1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// Spec is the control planes' Machines' spec.
	// +optional
	Spec ChalkosControlPlaneTemplateMachineTemplateSpec `json:"spec,omitempty,omitzero"`
}

// ChalkosControlPlaneTemplateMachineTemplateSpec is a ClusterClass's control planes' Machines'
// spec. It is ChalkosControlPlaneMachineTemplateSpec without InfrastructureRef.
// +kubebuilder:validation:MinProperties=1
type ChalkosControlPlaneTemplateMachineTemplateSpec struct {
	// Deletion contains configuration options for a control plane's Machine's deletion.
	// +optional
	Deletion ChalkosControlPlaneMachineTemplateDeletionSpec `json:"deletion,omitempty,omitzero"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=chalkoscontrolplanetemplates,scope=Namespaced,categories=cluster-api
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation of ChalkosControlPlaneTemplate"

// ChalkosControlPlaneTemplate is the ChalkosControlPlane a ClusterClass's control plane is
// instantiated from.
type ChalkosControlPlaneTemplate struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired state of ChalkosControlPlaneTemplate.
	// +optional
	Spec ChalkosControlPlaneTemplateSpec `json:"spec,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// ChalkosControlPlaneTemplateList contains a list of ChalkosControlPlaneTemplate.
type ChalkosControlPlaneTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ChalkosControlPlaneTemplate `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &ChalkosControlPlaneTemplate{}, &ChalkosControlPlaneTemplateList{})
}
