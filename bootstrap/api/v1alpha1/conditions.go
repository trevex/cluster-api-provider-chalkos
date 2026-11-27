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

// ChalkosConfig's Ready condition: true once the machine's install has finished and the data
// Secret marker exists.
const (
	// ReadyCondition reports whether the ChalkosConfig is ready.
	ReadyCondition = "Ready"

	// InstalledCondition reports whether the machine has answered as its node in normal mode.
	InstalledCondition = "Installed"

	// PausedCondition reports whether reconciliation of the ChalkosConfig is paused.
	PausedCondition = "Paused"
)

// Reasons for the Ready and Installed conditions.
const (
	// WaitingForInfrastructureReason documents a ChalkosConfig waiting for the Machine's
	// infrastructure to be ready before it can install.
	WaitingForInfrastructureReason = "WaitingForInfrastructure"

	// MachineDeletingReason documents a ChalkosConfig whose Machine is being deleted before its
	// machine was installed: the controller does not install it.
	MachineDeletingReason = "MachineDeleting"

	// WaitingForControlPlaneReason documents a worker's ChalkosConfig waiting for the cluster's
	// control plane to be initialized.
	WaitingForControlPlaneReason = "WaitingForControlPlane"

	// WaitingForClusterSecretsReason documents a ChalkosConfig waiting for the cluster's secrets
	// to exist; the control-plane provider creates them.
	WaitingForClusterSecretsReason = "WaitingForClusterSecrets"

	// InstallingReason documents a ChalkosConfig whose machine is installing.
	InstallingReason = "Installing"

	// ConfigRefusedReason documents a ChalkosConfig whose documents were refused: by the
	// controller, because they set a field the controllers own or the Cluster has no
	// ChalkosControlPlane, or by the machine's image. The controller tries again once the inputs
	// change.
	ConfigRefusedReason = "ConfigRefused"

	// ImageRefusedReason documents a ChalkosConfig whose Image was refused: it trusts another OS
	// CA than the one the cluster's secrets issue client certificates with, or its kubelet is not
	// of the Machine's version. A machine whose booted image trusts another OS CA refuses the
	// controller altogether, which no change of the ChalkosConfig repairs.
	ImageRefusedReason = "ImageRefused"

	// NodeUnreachableReason documents a ChalkosConfig whose machine could not be reached.
	NodeUnreachableReason = "NodeUnreachable"

	// InstalledReason documents a ChalkosConfig whose machine has answered as its node.
	InstalledReason = "Installed"

	// FellBackToMaintenanceReason documents a ChalkosConfig whose machine, once installed,
	// answered in maintenance mode again. The controller does not install it again: a
	// MachineHealthCheck replaces it, given an unhealthyMachineConditions entry for the Machine's
	// BootstrapConfigReady condition, which mirrors the ChalkosConfig's Ready, with status False.
	FellBackToMaintenanceReason = "FellBackToMaintenance"

	// ImageFellBackReason documents a ChalkosConfig whose installed machine runs another image
	// than the one it installed, such as the previous one after a first boot that was not found
	// healthy. The controller does not install it again; a MachineHealthCheck replaces it as it
	// replaces a machine that fell back to maintenance mode.
	ImageFellBackReason = "ImageFellBack"
)
