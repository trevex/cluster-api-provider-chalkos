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
		if upToDate(s.cp, m) {
			current++
		}
		counts[m.Spec.Version]++
	}
	st.Selector = labels.SelectorFromSet(selectorLabels(s.cp, s.cluster.Name)).String()
	st.Replicas = ptr.To(n)
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

	if initialized && ready > 0 {
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.AvailableCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.AvailableReason})
	} else {
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.AvailableCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotAvailableReason,
			Message: "The control plane is not initialized or has no ready Machine",
		})
	}

	switch want := ptr.Deref(s.cp.Spec.Replicas, 1); {
	case n < want && s.cp.DeletionTimestamp.IsZero():
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.ScalingUpCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.ScalingUpReason,
			Message: fmt.Sprintf("Scaling up from %d to %d replicas", n, want),
		})
	default:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.ScalingUpCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NotScalingUpReason})
	}

	switch {
	case n == 0:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.MachinesReadyCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.NoReplicasReason})
	case len(notReady) > 0:
		conditions.Set(s.cp, metav1.Condition{
			Type: controlplanev1.MachinesReadyCondition, Status: metav1.ConditionFalse, Reason: controlplanev1.MachinesNotReadyReason,
			Message: "Machines not ready: " + strings.Join(notReady, ", "),
		})
	default:
		conditions.Set(s.cp, metav1.Condition{Type: controlplanev1.MachinesReadyCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.MachinesReadyReason})
	}
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
