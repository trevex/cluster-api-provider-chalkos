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
)

// ChalkosConfigSpec is the chalkos part of one Machine: the image it installs and the documents of
// its pool.
type ChalkosConfigSpec struct {
	// Image is the chalkos image the machine installs, by digest: repository@sha256:….
	// +required
	// +kubebuilder:validation:Pattern=`^.+@sha256:[a-f0-9]{64}$`
	Image string `json:"image"`

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

// ChalkosConfigStatus is how far the machine's install got.
type ChalkosConfigStatus struct {
	// Initialization reports the bootstrap data's creation, as the Cluster API contract reads it.
	// +optional
	Initialization ChalkosConfigInitialization `json:"initialization,omitempty,omitzero"`

	// DataSecretName names the Secret with the machine's bootstrap data: a marker, since the
	// machine is installed over chalkd's API instead.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	DataSecretName string `json:"dataSecretName,omitempty"`

	// MaintenanceFingerprints are the certificate fingerprints the machine presented in
	// maintenance mode, newest last, for checks after the fact.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=10
	MaintenanceFingerprints []Fingerprint `json:"maintenanceFingerprints,omitempty"`

	// Installed is set once the machine answered in normal mode as its node.
	// +optional
	Installed *bool `json:"installed,omitempty"`

	// Conditions are the ChalkosConfig's current observations. Known condition types are Ready,
	// Installed and Paused.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +kubebuilder:validation:MaxItems=32
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation the status describes.
	// +optional
	// +kubebuilder:validation:Minimum=1
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// ChalkosConfigInitialization reports the bootstrap data's creation, as the Cluster API contract
// reads it.
// +kubebuilder:validation:MinProperties=1
type ChalkosConfigInitialization struct {
	// DataSecretCreated is true once the Machine's bootstrap data Secret exists.
	// +optional
	DataSecretCreated *bool `json:"dataSecretCreated,omitempty"`
}

// Fingerprint is a maintenance-mode certificate the machine presented, and when.
type Fingerprint struct {
	// SHA256 is the certificate's fingerprint: lower-case hex, as chalkos's pki.Fingerprint
	// formats it.
	// +required
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	SHA256 string `json:"sha256"`

	// SeenAt is when the machine presented the certificate.
	// +required
	SeenAt metav1.Time `json:"seenAt"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=chalkosconfigs,scope=Namespaced,categories=cluster-api
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Installed",type="boolean",JSONPath=".status.installed",description="The machine answered as its node"
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=`.status.conditions[?(@.type=="Ready")].status`,description="Install completed"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation of ChalkosConfig"

// ChalkosConfig is the bootstrap provider's contribution to one Machine: the chalkos image it
// installs and the config documents of the node's pool.
type ChalkosConfig struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired state of ChalkosConfig.
	// +required
	Spec ChalkosConfigSpec `json:"spec,omitempty,omitzero"`

	// Status is the observed state of ChalkosConfig.
	// +optional
	Status ChalkosConfigStatus `json:"status,omitempty,omitzero"`
}

// GetConditions returns the ChalkosConfig's conditions.
func (c *ChalkosConfig) GetConditions() []metav1.Condition {
	return c.Status.Conditions
}

// SetConditions sets the ChalkosConfig's conditions.
func (c *ChalkosConfig) SetConditions(conditions []metav1.Condition) {
	c.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// ChalkosConfigList contains a list of ChalkosConfig.
type ChalkosConfigList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ChalkosConfig `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &ChalkosConfig{}, &ChalkosConfigList{})
}
