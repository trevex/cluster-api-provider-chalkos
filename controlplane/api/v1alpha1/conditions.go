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

import clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

// ChalkosControlPlane's condition types.
const (
	// AvailableCondition reports whether the control plane passes all availability checks: at
	// least one control-plane Machine installed and reachable as its node, with a healthy etcd
	// member where chalkos manages etcd.
	AvailableCondition = "Available"

	// InitializedCondition reports whether the control plane is functional enough to accept
	// requests, as the Cluster API contract reads it to start provisioning the rest of the
	// cluster.
	InitializedCondition = "Initialized"

	// MachinesReadyCondition surfaces detail of issues on the control planes' Machines, if any.
	MachinesReadyCondition = "MachinesReady"

	// ScalingUpCondition reports whether the control plane has fewer Machines than Spec.Replicas
	// asks for.
	ScalingUpCondition = "ScalingUp"

	// DeletingCondition reports whether the ChalkosControlPlane is being deleted, and how far its
	// deletion got.
	DeletingCondition = clusterv1.DeletingCondition

	// PausedCondition reports whether reconciliation of the ChalkosControlPlane is paused.
	PausedCondition = "Paused"
)

// Reasons for the Available condition.
const (
	// AvailableReason documents a ChalkosControlPlane that is available.
	AvailableReason = "Available"

	// NotAvailableReason documents a ChalkosControlPlane that is not available.
	NotAvailableReason = "NotAvailable"
)

// Reasons for the Initialized condition.
const (
	// InitializedReason documents a ChalkosControlPlane that has been initialized.
	InitializedReason = "Initialized"

	// NotInitializedReason documents a ChalkosControlPlane that has not been initialized yet.
	NotInitializedReason = "NotInitialized"
)

// Reasons for the ScalingUp condition.
const (
	// ScalingUpReason documents a ChalkosControlPlane with fewer Machines than Spec.Replicas
	// asks for.
	ScalingUpReason = "ScalingUp"

	// NotScalingUpReason documents a ChalkosControlPlane with at least as many Machines as
	// Spec.Replicas asks for.
	NotScalingUpReason = "NotScalingUp"
)

// Reasons documenting why a ChalkosControlPlane's Machine cannot be installed, used across its
// conditions.
const (
	// ConfigRefusedReason documents a ChalkosControlPlane whose Files the controller refused,
	// because they set a field the controllers own. It is the same reason name the bootstrap
	// provider uses for the same situation.
	ConfigRefusedReason = "ConfigRefused"

	// ImageRefusedReason documents a ChalkosControlPlane whose Image the machine's OS check
	// authority refused.
	ImageRefusedReason = "ImageRefused"
)

// Reasons for the MachinesReady condition.
const (
	// MachinesReadyReason documents a ChalkosControlPlane whose Machines are all ready.
	MachinesReadyReason = "Ready"

	// MachinesNotReadyReason documents a ChalkosControlPlane with Machines that are not ready.
	MachinesNotReadyReason = "NotReady"

	// NoReplicasReason documents a ChalkosControlPlane without Machines.
	NoReplicasReason = "NoReplicas"
)

// More reasons for the Initialized condition.
const (
	// BootstrappedMachineGoneReason documents a ChalkosControlPlane whose bootstrapped Machine is
	// gone before the control plane was initialized; the controller bootstraps a new first Machine.
	BootstrappedMachineGoneReason = "BootstrappedMachineGone"

	// BootstrapRefusedReason documents a ChalkosControlPlane whose first Machine refused the
	// bootstrap, as one whose Kubernetes files are not prepared yet. The controller tries again.
	BootstrapRefusedReason = "BootstrapRefused"
)

// Reasons for the Deleting condition.
const (
	// NotDeletingReason documents a ChalkosControlPlane that is not being deleted.
	NotDeletingReason = clusterv1.NotDeletingReason

	// DeletingWaitingForWorkersDeletionReason documents a ChalkosControlPlane whose deletion waits for
	// the cluster's worker Machines and MachinePools to be deleted, before it deletes its own Machines.
	DeletingWaitingForWorkersDeletionReason = clusterv1.ClusterDeletingWaitingForWorkersDeletionReason

	// DeletingWaitingForMachineDeletionReason documents a ChalkosControlPlane whose deletion waits
	// for its Machines to be deleted.
	DeletingWaitingForMachineDeletionReason = "WaitingForMachineDeletion"

	// DeletionCompletedReason documents a ChalkosControlPlane whose Machines are deleted, so that
	// its finalizer is removed.
	DeletionCompletedReason = clusterv1.DeletionCompletedReason
)

// ScalingDownCondition reports whether the control plane removes a Machine: it has more Machines
// than Spec.Replicas asks for, or a Machine is being deleted and its pre-terminate hook runs.
const ScalingDownCondition = "ScalingDown"

// Reasons for the ScalingDown condition.
const (
	// ScalingDownReason documents a ChalkosControlPlane that removes a Machine; the message names
	// the Machine and the step it waits on.
	ScalingDownReason = "ScalingDown"

	// ScalingDownWaitingReason documents a ChalkosControlPlane with more Machines than
	// Spec.Replicas asks for that may not start a step yet, as while a Machine has not joined or
	// etcd is not healthy; the message says why.
	ScalingDownWaitingReason = "WaitingToScaleDown"

	// NotScalingDownReason documents a ChalkosControlPlane that removes no Machine.
	NotScalingDownReason = "NotScalingDown"
)
