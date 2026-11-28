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
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
)

// setStatus sets the ChalkosControlPlane's replica counts, versions and conditions from its
// Machines, as the Cluster API contract reads them. Before its Machines were listed, it sets only
// the conditions that do not depend on them.
func setStatus(s *scope) {
	st := &s.cp.Status
	initialized := s.initialized()
	if initialized {
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.InitializedCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.InitializedReason})
	} else if !conditions.Has(s.cp, controlplanev1.InitializedCondition) {
		s.notInitialized(controlplanev1.NotInitializedReason, "The control plane is not initialized yet")
	}
	setDeleting(s)
	if !s.listed {
		return
	}

	n := int32(len(s.machines))
	var ready, available, current int32
	var notReady []string
	counts := map[string]int32{}
	for i := range s.machines {
		m := &s.machines[i]
		if conditions.IsTrue(m, clusterv1.MachineReadyCondition) {
			ready++
		} else {
			notReady = append(notReady, m.Name)
		}
		if conditions.IsTrue(m, clusterv1.MachineAvailableCondition) {
			available++
		}
		if s.outOfDate(m) == "" {
			current++
		}
		counts[m.Spec.Version]++
	}
	st.Selector = labels.SelectorFromSet(selectorLabels(s.cp, s.cluster.Name)).String()
	// As with Cluster API's own control plane, the Machines being deleted count as replicas.
	st.Replicas = ptr.To(n + int32(len(s.deleting)))
	st.ReadyReplicas = ptr.To(ready)
	st.AvailableReplicas = ptr.To(available)
	st.UpToDateReplicas = ptr.To(current)
	st.Versions = nil
	for _, v := range slices.SortedFunc(maps.Keys(counts), compareVersions) {
		st.Versions = append(st.Versions, clusterv1.StatusVersion{Version: v, Replicas: counts[v]})
	}
	// The Cluster API contract still reads the deprecated status.version, the lowest version of
	// the Machines, beside status.versions.
	st.Version = "" //nolint:staticcheck // See above.
	if len(st.Versions) > 0 {
		st.Version = st.Versions[0].Version //nolint:staticcheck // See above.
	}

	switch {
	case !s.cp.DeletionTimestamp.IsZero():
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.AvailableCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotAvailableReason,
			Message: "The control plane is being deleted",
		})
	case initialized && ready > 0:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.AvailableCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.AvailableReason})
	case initialized:
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.AvailableCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotAvailableReason,
			Message: "The control plane has no ready Machine",
		})
	default:
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.AvailableCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotAvailableReason,
			Message: "The control plane is not initialized yet",
		})
	}

	// While a Machine is not up to date, RollingOut reports what the guard found; otherwise
	// ScalingUp does, also when no Machine is to be created.
	outOfDate := current < n
	switch want := ptr.Deref(s.cp.Spec.Replicas, 1); {
	case !s.cp.DeletionTimestamp.IsZero():
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.ScalingUpCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotScalingUpReason})
	case s.heldReason != "" && !outOfDate:
		status := metav1.ConditionFalse
		if n < want {
			status = metav1.ConditionTrue
		}
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingUpCondition, Status: status, Reason: s.heldReason,
			Message: machineutil.Truncate(s.held),
		})
	case n < want:
		msg := fmt.Sprintf("Scaling up from %d to %d replicas", n, want)
		for _, w := range []string{s.scaleUpWaiting, s.imageWaiting} {
			if w != "" {
				msg += "; " + w
			}
		}
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingUpCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.ScalingUpReason,
			Message: machineutil.Truncate(msg),
		})
	default:
		msg := s.imageWaiting
		if !outOfDate && s.held != "" {
			msg = s.held
		}
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingUpCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotScalingUpReason,
			Message: machineutil.Truncate(msg),
		})
	}

	switch want := ptr.Deref(s.cp.Spec.Replicas, 1); {
	case !s.cp.DeletionTimestamp.IsZero():
		// The Deleting condition reports the hooks of a deletion.
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.ScalingDownCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotScalingDownReason})
	case s.removing != "":
		reason := s.removingReason
		if reason == "" {
			reason = controlplanev1.ScalingDownReason
		}
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingDownCondition, Status: metav1.ConditionTrue, Reason: reason,
			Message: machineutil.Truncate("Removing a control plane: " + s.removing),
		})
	case n > want && s.scaleDownWaiting != "":
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingDownCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.ScalingDownWaitingReason,
			Message: machineutil.Truncate(fmt.Sprintf("Not scaling down from %d to %d replicas yet: %s", n, want, s.scaleDownWaiting)),
		})
	case n > want:
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingDownCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.ScalingDownReason,
			Message: fmt.Sprintf("Scaling down from %d to %d replicas", n, want),
		})
	default:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.ScalingDownCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotScalingDownReason})
	}

	switch {
	case !s.cp.DeletionTimestamp.IsZero() || !outOfDate:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.RollingOutCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotRollingOutReason})
	case s.heldReason != "":
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.RollingOutCondition, Status: metav1.ConditionTrue, Reason: s.heldReason,
			Message: machineutil.Truncate(s.held),
		})
	default:
		reason, msg := controlplanev1.RollingOutReason, fmt.Sprintf("%d of %d Machines not up to date", n-current, n)
		// A check the guard could not make holds the roll as much as the roll's own wait.
		if w := cmp.Or(s.held, s.rollWaiting); w != "" {
			reason, msg = controlplanev1.RollingOutWaitingReason, msg+"; "+w
		}
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.RollingOutCondition, Status: metav1.ConditionTrue, Reason: reason,
			Message: machineutil.Truncate(msg),
		})
	}

	switch {
	case n == 0:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.MachinesReadyCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NoReplicasReason})
	case len(notReady) > 0:
		// By name, so that the message does not change once a Machine created in this reconcile,
		// listed last, is listed in order at the next.
		slices.Sort(notReady)
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.MachinesReadyCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.MachinesNotReadyReason,
			Message: "Machines not ready: " + strings.Join(notReady, ", "),
		})
	default:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.MachinesReadyCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.MachinesReadyReason})
	}
}

// setDeleting sets the Deleting condition: false while the ChalkosControlPlane is not being
// deleted, and otherwise true with the reason and message its deletion recorded. A deletion that
// recorded none, as one whose Machines could not be listed, keeps an earlier Deleting condition.
func setDeleting(s *scope) {
	if s.cp.DeletionTimestamp.IsZero() {
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.DeletingCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotDeletingReason})
		return
	}
	reason, message := s.deletingReason, s.deletingMessage
	if reason == "" {
		if conditions.IsTrue(s.cp, controlplanev1.DeletingCondition) {
			return
		}
		reason = clusterv1.DeletingReason
	}
	conditions.Set(s.cp, metav1.Condition{
		Type: controlplanev1.DeletingCondition, Status: metav1.ConditionTrue, Reason: reason, Message: message,
	})
}

// compareVersions orders Kubernetes versions by semantic version, and those that are none after
// them by name.
func compareVersions(a, b string) int {
	va, errA := utilversion.ParseSemantic(a)
	_, errB := utilversion.ParseSemantic(b)
	switch {
	case errA == nil && errB == nil:
		if c, err := va.Compare(b); err == nil {
			return c
		}
	case errA == nil:
		return -1
	case errB == nil:
		return 1
	}
	return strings.Compare(a, b)
}
