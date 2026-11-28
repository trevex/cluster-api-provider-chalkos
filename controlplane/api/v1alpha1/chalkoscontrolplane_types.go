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

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
)

// ChalkosControlPlaneFinalizer keeps a ChalkosControlPlane until its Machines are deleted: the
// Cluster API core controllers do not wait for a control plane's Machines when they delete a
// Cluster.
const ChalkosControlPlaneFinalizer = "controlplane.cluster.x-k8s.io/chalkoscontrolplane"

// SpecHashAnnotation holds, on each control-plane Machine and its ChalkosConfig, the SHA-256 of
// what the Machine was made from: the ChalkosControlPlane's version, image, files and
// infrastructure template. A Machine whose hash differs from the ChalkosControlPlane's is not up
// to date and is replaced.
const SpecHashAnnotation = "controlplane.cluster.x-k8s.io/chalkos-spec-hash"

// ChalkosControlPlaneSpec is the chalkos part of a cluster's control plane: the image its
// Machines install, how many there are, and the documents of the cluster's node pool.
// +kubebuilder:validation:XValidation:rule="self.?rollout.?strategy.?rollingUpdate.?maxSurge.orValue(1) == 1 || self.?replicas.orValue(1) >= 3",message="rollout.strategy.rollingUpdate.maxSurge 0 needs at least 3 replicas: with fewer, removing a control plane first loses etcd's quorum"
type ChalkosControlPlaneSpec struct {
	// Replicas is the number of control planes to run. It must be odd, so the control plane's
	// etcd can form a quorum: an even count tolerates no more failures than the odd count below
	// it, while using one more Machine. It defaults to 1.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:XValidation:rule="self % 2 == 1",message="replicas must be odd"
	Replicas *int32 `json:"replicas,omitempty"`

	// Version is the Kubernetes version the control planes run. The chalkos image they install
	// must carry a kubelet of this same release.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:XValidation:rule="self.startsWith('v')",message="version must start with v"
	Version string `json:"version,omitempty"`

	// Image is the chalkos image the control planes install, by digest: repository@sha256:….
	// +required
	Image bootstrapv1.ImageReference `json:"image"`

	// ImagePullSecretRef names a Secret of type kubernetes.io/dockerconfigjson in the
	// ChalkosControlPlane's namespace whose credentials the controller reads the image with. The
	// node pulls with the Registry documents among Files.
	// +optional
	ImagePullSecretRef *corev1.LocalObjectReference `json:"imagePullSecretRef,omitempty"`

	// MachineTemplate is what the control planes' Machines are made from.
	// +required
	MachineTemplate ChalkosControlPlaneMachineTemplate `json:"machineTemplate,omitempty,omitzero"`

	// Rollout says when the control planes' Machines are replaced and how. A Machine is replaced
	// when the version, image, files or infrastructure template it was made from changed, or it
	// was created before rollout.after.
	// +optional
	Rollout ChalkosControlPlaneRolloutSpec `json:"rollout,omitempty,omitzero"`

	// Files are the cluster-wide chalkos config documents every node of the cluster gets (such as
	// Flannel, Addon and Registry documents), together with the control planes' own Node fields.
	// They must hold the Cluster document, which names the etcd, coredns, pause and flannel images;
	// the controllers derive the API server's, controller-manager's, scheduler's and kube-proxy's
	// from the version where it names none. Fields the controllers own are refused: the Node's type
	// and providerID, the Cluster's endpoint, and its ipFamilies, podCIDRs and serviceCIDRs when the
	// Cluster's clusterNetwork gives ranges. The cluster-wide documents other than Secrets are also
	// recorded in status.observedFiles, so very large files count about twice against the
	// object's size limit.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	// +kubebuilder:validation:MaxItems=256
	Files []runtime.RawExtension `json:"files,omitempty"`

	// Kubeconfig says how the cluster's kubeconfig Secret's certificate is issued.
	// +optional
	// +kubebuilder:default={}
	Kubeconfig KubeconfigSpec `json:"kubeconfig,omitempty,omitzero"`
}

// ChalkosControlPlaneMachineTemplate is what the control planes' Machines are made from.
type ChalkosControlPlaneMachineTemplate struct {
	// ObjectMeta is the metadata the control planes' Machines start with.
	// +optional
	ObjectMeta clusterv1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// Spec is the control planes' Machines' spec.
	// +required
	Spec ChalkosControlPlaneMachineTemplateSpec `json:"spec,omitempty,omitzero"`
}

// ChalkosControlPlaneMachineTemplateSpec is the control planes' Machines' spec.
type ChalkosControlPlaneMachineTemplateSpec struct {
	// InfrastructureRef is a required reference to the infrastructure resource backing each
	// control plane's Machine, offered by an infrastructure provider.
	// +required
	InfrastructureRef clusterv1.ContractVersionedObjectReference `json:"infrastructureRef,omitempty,omitzero"`

	// Deletion contains configuration options for a control plane's Machine's deletion.
	// +optional
	Deletion ChalkosControlPlaneMachineTemplateDeletionSpec `json:"deletion,omitempty,omitzero"`
}

// ChalkosControlPlaneMachineTemplateDeletionSpec contains configuration options for a control
// plane's Machine's deletion.
// +kubebuilder:validation:MinProperties=1
type ChalkosControlPlaneMachineTemplateDeletionSpec struct {
	// NodeDrainTimeoutSeconds is the total amount of time the controller spends draining a
	// control plane's node. Its default, 0, means the node is drained without a time limit.
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeDrainTimeoutSeconds *int32 `json:"nodeDrainTimeoutSeconds,omitempty"`

	// NodeVolumeDetachTimeoutSeconds is the total amount of time the controller waits for all of
	// a control plane's volumes to detach. Its default, 0, means it waits without a time limit.
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeVolumeDetachTimeoutSeconds *int32 `json:"nodeVolumeDetachTimeoutSeconds,omitempty"`

	// NodeDeletionTimeoutSeconds is how long the controller attempts to delete a control plane
	// Machine's Node, measured from when that deletion started. A value of 0 retries
	// indefinitely.
	// +optional
	// +kubebuilder:validation:Minimum=0
	NodeDeletionTimeoutSeconds *int32 `json:"nodeDeletionTimeoutSeconds,omitempty"`
}

// KubeconfigSpec says how a cluster's kubeconfig Secret's certificate is issued.
type KubeconfigSpec struct {
	// TTLSeconds is how long, in seconds, the kubeconfig Secret's client certificate is valid.
	// The certificate is issued again once two thirds of the TTL have passed.
	// +optional
	// +kubebuilder:default=86400
	// +kubebuilder:validation:Minimum=600
	TTLSeconds *int32 `json:"ttlSeconds,omitempty"`
}

// ChalkosControlPlaneRolloutSpec says when the control planes' Machines are replaced and how.
type ChalkosControlPlaneRolloutSpec struct {
	// After replaces every control-plane Machine created before it, once it has passed, even when
	// nothing else changed. Written in RFC 3339, such as 2026-11-28T09:00:00Z.
	// +optional
	After metav1.Time `json:"after,omitempty,omitzero"`

	// Strategy says how a Machine that is not up to date is replaced.
	// +optional
	Strategy ChalkosControlPlaneRolloutStrategy `json:"strategy,omitempty,omitzero"`
}

// ChalkosControlPlaneRolloutStrategy says how the control planes' Machines are replaced.
type ChalkosControlPlaneRolloutStrategy struct {
	// RollingUpdate replaces the Machines one at a time, the oldest first.
	// +optional
	RollingUpdate ChalkosControlPlaneRollingUpdate `json:"rollingUpdate,omitempty,omitzero"`
}

// ChalkosControlPlaneRollingUpdate replaces the control planes' Machines one at a time.
type ChalkosControlPlaneRollingUpdate struct {
	// MaxSurge is how many Machines above replicas the control plane runs while it replaces one.
	// 1, the default, creates the replacement first and deletes the old Machine once the new one
	// has joined etcd. 0 deletes the old Machine first and creates the replacement once it is
	// gone, which needs at least three replicas.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	MaxSurge *int32 `json:"maxSurge,omitempty"`
}

// ChalkosControlPlaneStatus is how far the cluster's control plane got.
type ChalkosControlPlaneStatus struct {
	// Selector is the label selector, in string format, that the scale subresource and tools
	// such as kubectl use to find the control planes' Machines. The string is in the same format
	// as the query-param syntax.
	// +optional
	Selector string `json:"selector,omitempty"`

	// Replicas is the total number of non-terminated Machines targeted by this control plane.
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of those Machines whose Ready condition is true.
	// +optional
	ReadyReplicas *int32 `json:"readyReplicas,omitempty"`

	// AvailableReplicas is the number of those Machines whose Available condition is true.
	// +optional
	AvailableReplicas *int32 `json:"availableReplicas,omitempty"`

	// UpToDateReplicas is the number of those Machines whose UpToDate condition is true.
	// +optional
	UpToDateReplicas *int32 `json:"upToDateReplicas,omitempty"`

	// Versions is the aggregated Kubernetes versions of the control plane's Machines.
	// +optional
	// +listType=map
	// +listMapKey=version
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=100
	Versions []clusterv1.StatusVersion `json:"versions,omitempty"`

	// Version is the Kubernetes version of the control plane's Machines.
	//
	// Deprecated: This field is deprecated and is going to be removed in a future API version.
	// Please use status.versions instead.
	//
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	Version string `json:"version,omitempty"`

	// Initialization reports the control plane's initialization, as the Cluster API contract
	// reads it.
	// +optional
	Initialization ChalkosControlPlaneInitialization `json:"initialization,omitempty,omitzero"`

	// BootstrappedMachine is the control-plane Machine that Bootstrap was called on, recorded
	// before the call so that a retry after a crash does not call Bootstrap on a second Machine.
	// +optional
	BootstrappedMachine string `json:"bootstrappedMachine,omitempty"`

	// KubeconfigExpiresAt is when the kubeconfig Secret's client certificate expires.
	// +optional
	KubeconfigExpiresAt *metav1.Time `json:"kubeconfigExpiresAt,omitempty"`

	// LastRevokedPruneTime is when the controller last asked a control plane to drop the revoked
	// nodes nothing lets back in, whether or not that succeeded. It asks after each revocation
	// of a control plane's node and otherwise once a day, which also prunes the workers'
	// revocations.
	// +optional
	LastRevokedPruneTime *metav1.Time `json:"lastRevokedPruneTime,omitempty"`

	// ObservedFiles are the cluster-wide documents the control plane's Machines were made from,
	// as a control-plane Machine installs them: spec.files with the fields the controllers fill
	// from the Cluster, the Cluster document's endpoint, ipFamilies, podCIDRs and serviceCIDRs,
	// but without a Machine's own Node document and without Secret documents, and with the
	// image's defaults written out, so that an image defaulting a field every node holds alike
	// differently shows as a change. They are one YAML stream of JSON documents separated by ---,
	// recorded once a Machine exists and none is out of date or being deleted. While the
	// documents a new Machine would get differ from them, the controllers compare the two for
	// changes to fields every node of the cluster holds alike, which hold every new Machine.
	// +optional
	ObservedFiles string `json:"observedFiles,omitempty"`

	// Conditions are the ChalkosControlPlane's current observations. Known condition types are
	// Available, Initialized, MachinesReady, ScalingUp, ScalingDown, RollingOut, Deleting and
	// Paused.
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

// ChalkosControlPlaneInitialization reports the control plane's initialization, as the Cluster
// API contract reads it.
// +kubebuilder:validation:MinProperties=1
type ChalkosControlPlaneInitialization struct {
	// ControlPlaneInitialized is true once the first control plane's Machine has applied the
	// cluster's add-ons and the control plane can accept requests.
	// +optional
	ControlPlaneInitialized *bool `json:"controlPlaneInitialized,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:path=chalkoscontrolplanes,scope=Namespaced,categories=cluster-api
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Cluster",type="string",JSONPath=".metadata.labels['cluster\\.x-k8s\\.io/cluster-name']",description="Cluster"
// +kubebuilder:printcolumn:name="Available",type="string",JSONPath=`.status.conditions[?(@.type=="Available")].status`,description="The control plane passes all availability checks"
// +kubebuilder:printcolumn:name="Desired",type="integer",JSONPath=".spec.replicas",description="The desired number of control planes"
// +kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".status.replicas",description="The number of control planes"
// +kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas",description="The number of control planes with the Ready condition true"
// +kubebuilder:printcolumn:name="Initialized",type="boolean",JSONPath=".status.initialization.controlPlaneInitialized",description="The control plane can accept requests"
// +kubebuilder:printcolumn:name="Paused",type="string",JSONPath=`.status.conditions[?(@.type=="Paused")].status`,description="Reconciliation paused",priority=10
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp",description="Time since creation of ChalkosControlPlane"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version",description="The Kubernetes version of the control plane"

// ChalkosControlPlane is the control-plane provider's contribution to a Cluster: the chalkos
// image and documents its control-plane Machines install, and how many of them there are.
type ChalkosControlPlane struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Spec is the desired state of ChalkosControlPlane.
	// +required
	Spec ChalkosControlPlaneSpec `json:"spec,omitempty,omitzero"`

	// Status is the observed state of ChalkosControlPlane.
	// +optional
	Status ChalkosControlPlaneStatus `json:"status,omitempty,omitzero"`
}

// GetConditions returns the ChalkosControlPlane's conditions.
func (c *ChalkosControlPlane) GetConditions() []metav1.Condition {
	return c.Status.Conditions
}

// SetConditions sets the ChalkosControlPlane's conditions.
func (c *ChalkosControlPlane) SetConditions(conditions []metav1.Condition) {
	c.Status.Conditions = conditions
}

// +kubebuilder:object:root=true

// ChalkosControlPlaneList contains a list of ChalkosControlPlane.
type ChalkosControlPlaneList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []ChalkosControlPlane `json:"items"`
}

func init() {
	objectTypes = append(objectTypes, &ChalkosControlPlane{}, &ChalkosControlPlaneList{})
}
