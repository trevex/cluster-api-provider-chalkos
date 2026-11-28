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

package controllers

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
)

// maxWorkerSkew is how many minors older than its API server Kubernetes supports a kubelet.
const maxWorkerSkew = 3

// workerSkew compares a worker's Kubernetes version with the control plane's oldest: newer tells
// whether the worker is of a newer minor, which no API server of the control plane serves, and
// older how many minors older it is otherwise. Versions of different majors, or that are none,
// are an error.
func workerSkew(worker, oldest string) (newer bool, older uint, err error) {
	w, err := utilversion.ParseSemantic(worker)
	if err != nil {
		return false, 0, err
	}
	o, err := utilversion.ParseSemantic(oldest)
	if err != nil {
		return false, 0, err
	}
	if w.Major() != o.Major() {
		return false, 0, fmt.Errorf("the worker's version %s and the control plane's %s are of different majors", worker, oldest)
	}
	if w.Minor() > o.Minor() {
		return true, 0, nil
	}
	return false, o.Minor() - w.Minor(), nil
}

// holdForControlPlane records in s why the Machine may not install yet because of the
// ChalkosControlPlane cp, and tells whether the install stops and when to look again. A worker
// waits while it is of a newer minor than the oldest of status.versions, which counts the control
// plane's Machines being deleted too. Any Machine is refused, for the inputs in, while cp's
// spec.files, with the fields filled from the Cluster, make no cluster-wide documents, for which
// the control plane holds InvalidFiles. It waits while those documents change, against
// status.observedFiles, a field every node of the cluster holds alike, as the contract of cp's
// image marks it, for which the control plane holds its own new Machines too. Without observed
// files there is nothing to compare. The ChalkosControlPlane's watch reports a change of its
// files, versions or record, and the Cluster's a change of its network.
func (r *ChalkosConfigReconciler) holdForControlPlane(ctx context.Context, s *scope, cp *controlplanev1.ChalkosControlPlane, in string) (ctrl.Result, bool) {
	if !util.IsControlPlaneMachine(s.machine) && len(cp.Status.Versions) > 0 {
		if res, wait := checkWorkerSkew(s, cp.Status.Versions[0].Version); wait {
			return res, true
		}
	}
	docs, err := documents.ClusterWide(s.cluster, cp.Spec.Version, cp.Spec.Files)
	if err != nil {
		s.refuse(bootstrapv1.ConfigRefusedReason, fmt.Sprintf("The cluster-wide documents cannot be made: %v", err), in)
		return ctrl.Result{}, true
	}
	observed := cp.Status.ObservedFiles
	if observed == "" {
		return ctrl.Result{}, false
	}
	c, err := r.artifacts.Contract(ctx, r.APIReader, r.Node, string(cp.Spec.Image), cp.Namespace, cp.Spec.ImagePullSecretRef)
	if err != nil {
		ctrl.LoggerFrom(ctx).V(1).Info("Not checking the cluster-wide documents yet: reading the control plane's image's contract", "reason", err.Error())
		// The message stays alike at every attempt; the error itself is logged.
		s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason,
			fmt.Sprintf("The ChalkosControlPlane's image %s cannot be read, so the cluster-wide documents are not checked against its status.observedFiles yet", cp.Spec.Image))
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, true
	}
	changes, err := documents.FixedChanges(c, observed, docs)
	switch {
	case err != nil:
		s.set(metav1.ConditionFalse, bootstrapv1.ObservedFilesInvalidReason,
			fmt.Sprintf("The ChalkosControlPlane's status.observedFiles cannot be compared with the cluster-wide documents: %v; its RollingOut or ScalingUp condition tells how to clear the record", err))
		return ctrl.Result{}, true
	case len(changes) > 0:
		s.set(metav1.ConditionFalse, bootstrapv1.FixedFieldChangedReason, documents.DescribeFixed(changes)+"; the machine installs once the change is reverted")
		return ctrl.Result{}, true
	}
	return ctrl.Result{}, false
}

// checkWorkerSkew holds a worker of a newer minor than the control plane's oldest version,
// oldest, until the control plane is upgraded, and records in the VersionSkew condition whether it
// is more than maxWorkerSkew minors older. It tells whether the install waits, and when to look
// again: the ChalkosControlPlane's watch reports its upgrade, the requeue covers a missed event.
func checkWorkerSkew(s *scope, oldest string) (ctrl.Result, bool) {
	version := s.machine.Spec.Version
	newer, older, err := workerSkew(version, oldest)
	switch {
	case err != nil:
		// A version that is none is the image check's to refuse.
		return ctrl.Result{}, false
	case newer:
		s.set(metav1.ConditionFalse, bootstrapv1.WaitingForControlPlaneUpgradeReason,
			fmt.Sprintf("The worker is of version %s, of a newer minor than the control plane's oldest, %s; it installs once the control plane is upgraded", version, oldest))
		return ctrl.Result{RequeueAfter: controlPlaneWait}, true
	case older > maxWorkerSkew:
		conditions.Set(s.cfg, metav1.Condition{
			Type: bootstrapv1.VersionSkewCondition, Status: metav1.ConditionTrue, Reason: bootstrapv1.VersionSkewReason,
			Message: fmt.Sprintf("The worker is of version %s, %d minors older than the control plane's oldest, %s; Kubernetes supports kubelets up to %d minors older than the API server", version, older, oldest, maxWorkerSkew),
		})
	default:
		conditions.Set(s.cfg, metav1.Condition{Type: bootstrapv1.VersionSkewCondition, Status: metav1.ConditionFalse, Reason: bootstrapv1.WithinVersionSkewReason})
	}
	return ctrl.Result{}, false
}
