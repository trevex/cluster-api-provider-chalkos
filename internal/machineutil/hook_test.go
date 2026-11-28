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

package machineutil

import (
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
)

// testScheme knows Cluster API's kinds.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clusterv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

// AddHook sets the owner's value on a Machine without the hook and leaves one that has it, or
// that is being deleted, alone; RemoveHook removes it from the stored Machine.
func TestAddAndRemoveHook(t *testing.T) {
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m1"}}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(m).Build()
	ctx := t.Context()
	if added, err := AddHook(ctx, c, m, BootstrapHookOwner); err != nil || !added || m.Annotations[HookAnnotation] != BootstrapHookOwner {
		t.Fatalf("AddHook = %v, %v; annotations %v", added, err, m.Annotations)
	}
	m.Annotations[HookAnnotation] = "someone-else"
	if added, err := AddHook(ctx, c, m, BootstrapHookOwner); err != nil || added || m.Annotations[HookAnnotation] != "someone-else" {
		t.Errorf("a Machine with the hook is changed: %v, %v", added, err)
	}
	if err := RemoveHook(ctx, c, m); err != nil || HasHook(m) {
		t.Fatalf("RemoveHook = %v; annotations %v", err, m.Annotations)
	}
	var got clusterv1.Machine
	if err := c.Get(ctx, client.ObjectKeyFromObject(m), &got); err != nil || HasHook(&got) {
		t.Errorf("the stored Machine's annotations %v, %v", got.Annotations, err)
	}
	if err := RemoveHook(ctx, c, m); err != nil {
		t.Errorf("RemoveHook of a Machine without the hook = %v", err)
	}
	deleting := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "m2", DeletionTimestamp: ptr.To(metav1.Now())}}
	if added, err := AddHook(ctx, c, deleting, BootstrapHookOwner); err != nil || added || HasHook(deleting) {
		t.Errorf("a Machine being deleted got the hook: %v, %v", added, err)
	}
}

// RemoveHook of a Machine that is gone succeeds.
func TestRemoveHookOfAGoneMachine(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).Build()
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "gone", Annotations: map[string]string{HookAnnotation: BootstrapHookOwner}}}
	if err := RemoveHook(t.Context(), c, m); err != nil {
		t.Errorf("RemoveHook = %v", err)
	}
}

// Only a Machine being deleted whose Deleting condition has the reason WaitingForPreTerminateHook
// waits for its hook.
func TestWaitingForHook(t *testing.T) {
	m := &clusterv1.Machine{}
	if DeletingReason(m) != "" {
		t.Error("a Machine without the Deleting condition has a reason")
	}
	set := func(reason string) {
		conditions.Set(m, metav1.Condition{Type: clusterv1.MachineDeletingCondition, Status: metav1.ConditionTrue, Reason: reason})
	}
	set(clusterv1.MachineDeletingWaitingForPreTerminateHookReason)
	if WaitingForHook(m) {
		t.Error("a Machine that is not being deleted waits for its hook")
	}
	m.DeletionTimestamp = ptr.To(metav1.Now())
	if !WaitingForHook(m) {
		t.Error("the reason WaitingForPreTerminateHook is not waiting")
	}
	set(clusterv1.MachineDeletingDrainingNodeReason)
	if WaitingForHook(m) || DeletingReason(m) != clusterv1.MachineDeletingDrainingNodeReason {
		t.Error("a draining Machine waits for its hook")
	}
}

// ControlPlaneMachines lists the cluster's control-plane Machines only, those not being deleted
// first, each group oldest first.
func TestControlPlaneMachines(t *testing.T) {
	now := time.Now()
	machine := func(name, cluster string, controlPlane bool, age time.Duration, deleting bool) *clusterv1.Machine {
		m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: name, CreationTimestamp: metav1.NewTime(now.Add(-age)),
			Labels: map[string]string{clusterv1.ClusterNameLabel: cluster},
		}}
		if controlPlane {
			m.Labels[clusterv1.MachineControlPlaneLabel] = ""
		}
		if deleting {
			m.DeletionTimestamp = ptr.To(metav1.NewTime(now))
			m.Finalizers = []string{"test.chalkos.dev/hold"}
		}
		return m
	}
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(
		machine("cp-young", "c1", true, time.Minute, false),
		machine("cp-old", "c1", true, time.Hour, false),
		machine("cp-deleting", "c1", true, 2*time.Hour, true),
		machine("worker", "c1", false, 3*time.Hour, false),
		machine("cp-other", "c2", true, 3*time.Hour, false),
	).Build()
	ms, err := ControlPlaneMachines(t.Context(), c, &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1"}})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range ms {
		names = append(names, m.Name)
	}
	if want := []string{"cp-old", "cp-young", "cp-deleting"}; !slices.Equal(names, want) {
		t.Errorf("ControlPlaneMachines = %v, want %v", names, want)
	}
}

// ClusterDeletionStarted passes only the update that starts a Cluster's deletion.
func TestClusterDeletionStarted(t *testing.T) {
	alive := &clusterv1.Cluster{}
	deleting := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: ptr.To(metav1.Now())}}
	p := ClusterDeletionStarted()
	if !p.Update(event.UpdateEvent{ObjectOld: alive, ObjectNew: deleting}) {
		t.Error("the update that starts the deletion is dropped")
	}
	if p.Update(event.UpdateEvent{ObjectOld: deleting, ObjectNew: deleting.DeepCopy()}) || p.Update(event.UpdateEvent{ObjectOld: alive, ObjectNew: alive.DeepCopy()}) {
		t.Error("another update passes")
	}
	if p.Create(event.CreateEvent{Object: deleting}) || p.Delete(event.DeleteEvent{Object: deleting}) || p.Generic(event.GenericEvent{Object: deleting}) {
		t.Error("a create, delete or generic event passes")
	}
}

// Event records with the recorder, and a nil recorder records nothing.
func TestEvent(t *testing.T) {
	rec := events.NewFakeRecorder(1)
	m := &clusterv1.Machine{}
	Event(rec, m, nil, corev1.EventTypeNormal, "Revoked", "Revoke", "Revoked the node %s", "m1")
	if got := <-rec.Events; !strings.Contains(got, "Revoked the node m1") || !strings.HasPrefix(got, corev1.EventTypeNormal) {
		t.Errorf("the event %q", got)
	}
	Event(nil, m, nil, corev1.EventTypeNormal, "Revoked", "Revoke", "Revoked the node")
}
