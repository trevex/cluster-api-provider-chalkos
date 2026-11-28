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
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
)

// setMaxSurge sets the ChalkosControlPlane's spec.rollout.strategy.rollingUpdate.maxSurge.
func (e *env) setMaxSurge(n int32) {
	e.t.Helper()
	cp := e.cp()
	cp.Spec.Rollout.Strategy.RollingUpdate.MaxSurge = ptr.To(n)
	if err := testClient.Update(e.t.Context(), cp); err != nil {
		e.t.Fatal(err)
	}
}

// newcomer is the Machine among all that addrs has no address for: the one the controller
// created last.
func (e *env) newcomer(all []clusterv1.Machine, addrs map[string]string) clusterv1.Machine {
	e.t.Helper()
	for _, m := range all {
		if _, ok := addrs[m.Name]; !ok {
			return m
		}
	}
	e.t.Fatal("no new Machine")
	return clusterv1.Machine{}
}

// finishRemoval lets the removal of the Machine gone, held by hold, finish: Cluster API waits for
// its hook, which takes it out of etcd and revokes it, it goes, and etcd lists left.
func (e *env) finishRemoval(gone string, left []clusterv1.Machine, addrs map[string]string) {
	e.t.Helper()
	e.markWaiting(gone)
	e.mustReconcile()
	if e.hooked(gone) {
		e.t.Fatalf("the Machine %s keeps its hook", gone)
	}
	e.release(gone)
	e.setMembers(left, addrs)
}

// rollingControlPlane brings the control plane to n joined Machines of the served image, whose
// files a node accepts, each held by a finalizer once deleted, and returns them oldest first with
// their addresses.
func (e *env) rollingControlPlane(n int) ([]clusterv1.Machine, map[string]string) {
	e.t.Helper()
	e.serveImage()
	e.setFiles(clusterFile("lab.local"), systemDiskFile)
	ms, addrs := e.controlPlanes(n)
	for _, m := range ms {
		e.hold(m.Name)
	}
	return ms, addrs
}

// With maxSurge 1, a patch version rolls the three Machines oldest first: each time an up-to-date
// fourth Machine joins before the oldest that is not up to date is deleted.
func TestRollWithSurge(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.rollingControlPlane(3)
	e.setVersion("v1.37.2")
	left := ms
	for step := range 3 {
		e.mustReconcile()
		repl := e.newcomer(e.wantMachines(4), addrs)
		wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.RollingOutReason, fmt.Sprintf("%d of 4 Machines", 3-step))
		if repl.Spec.Version != "v1.37.2" {
			t.Fatalf("step %d: the replacement is of %s", step, repl.Spec.Version)
		}
		e.mustReconcile()
		if got := e.deleted(); len(got) != 0 {
			t.Fatalf("step %d: deleted %v before the replacement joined", step, got)
		}
		addr := fmt.Sprintf("10.0.1.%d", step+1)
		e.provision(repl, addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
		addrs[repl.Name] = addr
		current := append(slices.Clone(left), repl)
		e.setMembers(current, addrs)
		e.mustReconcile()
		if got := e.deleted(); !slices.Equal(got, []string{left[0].Name}) {
			t.Fatalf("step %d: deleted %v, want the oldest %s", step, got, left[0].Name)
		}
		e.finishRemoval(left[0].Name, current[1:], addrs)
		left = current[1:]
	}
	e.mustReconcile()
	e.wantMachines(3)
	cp := e.cp()
	wantConditionReason(t, cp, controlplanev1.RollingOutCondition, metav1.ConditionFalse, controlplanev1.NotRollingOutReason, "")
	if got := ptr.Deref(cp.Status.UpToDateReplicas, -1); got != 3 {
		t.Errorf("upToDateReplicas %d, want 3", got)
	}
}

// With maxSurge 0, the oldest Machine that is not up to date goes first and its replacement is
// created once it is gone: never more than three Machines.
func TestRollWithoutSurge(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.rollingControlPlane(3)
	e.setMaxSurge(0)
	e.setVersion("v1.37.2")
	left := ms
	for step := range 3 {
		e.mustReconcile()
		if got := e.deleted(); !slices.Equal(got, []string{left[0].Name}) {
			t.Fatalf("step %d: deleted %v, want the oldest %s", step, got, left[0].Name)
		}
		e.mustReconcile()
		e.wantMachines(3)
		wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "being deleted")
		e.finishRemoval(left[0].Name, left[1:], addrs)
		e.mustReconcile()
		repl := e.newcomer(e.wantMachines(3), addrs)
		addr := fmt.Sprintf("10.0.1.%d", step+1)
		e.provision(repl, addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
		addrs[repl.Name] = addr
		left = append(slices.Clone(left[1:]), repl)
		e.setMembers(left, addrs)
	}
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionFalse, controlplanev1.NotRollingOutReason, "")
}

// unknownFile is a document of a kind the image does not know, which a node refuses although the
// controllers make documents of it.
var unknownFile = runtime.RawExtension{Raw: []byte(`{"apiVersion": "example.chalkos.dev/v1alpha1", "kind": "Unknown", "metadata": {"name": "x"}, "spec": {}}`)}

// With maxSurge 0, no Machine is deleted first while the image refuses the documents its
// replacement would install with: RollingOut says so, and later passes write nothing.
func TestRollWithoutSurgeChecksTheReplacement(t *testing.T) {
	e := newEnv(t, 3, true)
	e.rollingControlPlane(3)
	e.setMaxSurge(0)
	e.setFiles(clusterFile("lab.local"), systemDiskFile, unknownFile)
	e.mustReconcile()
	if got := e.deleted(); len(got) != 0 {
		t.Fatalf("deleted %v although the replacement cannot install", got)
	}
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.InvalidFilesReason, "Unknown")
	e.wantNoWrite()
	e.wantMachines(3)
}

// With maxSurge 1, no Machine is created while the image refuses the documents it would install
// with.
func TestRollWithSurgeChecksTheReplacement(t *testing.T) {
	e := newEnv(t, 3, true)
	e.rollingControlPlane(3)
	e.setFiles(clusterFile("lab.local"), systemDiskFile, unknownFile)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.InvalidFilesReason, "Unknown")
}

// A one-replica control plane surges: the replacement joins, then the old Machine goes, its hook
// no longer waiting as etcd's last voter.
func TestOneReplicaRollSurges(t *testing.T) {
	e := newEnv(t, 1, true)
	ms, addrs := e.rollingControlPlane(1)
	old := ms[0].Name
	e.setVersion("v1.37.2")
	e.mustReconcile()
	repl := e.newcomer(e.wantMachines(2), addrs)
	e.provision(repl, "10.0.0.2", kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
	addrs[repl.Name] = "10.0.0.2"
	e.setMembers([]clusterv1.Machine{ms[0], repl}, addrs)
	e.mustReconcile()
	if got := e.deleted(); !slices.Equal(got, []string{old}) {
		t.Fatalf("deleted %v, want %s", got, old)
	}
	e.markWaiting(old)
	e.mustReconcile()
	if e.hooked(old) {
		t.Error("the old Machine keeps its hook although its replacement joined")
	}
}

// A change of files that marks no fixed field rolls.
func TestOtherFileChangesRoll(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	e.controlPlanes(3)
	e.setFiles(clusterFile("lab.local"), systemDiskFile)
	e.mustReconcile()
	e.wantMachines(4)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.RollingOutReason, "3 of 4 Machines")
}

// Scaling down deletes a Machine that is not up to date before an older one that is.
func TestScaleDownTakesOutOfDateMachinesFirst(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, _ := e.controlPlanes(3)
	for _, m := range ms {
		e.hold(m.Name)
	}
	e.stamp(ms[1].Name, "stale")
	e.setReplicas(1)
	e.mustReconcile()
	if got := e.deleted(); !slices.Equal(got, []string{ms[1].Name}) {
		t.Fatalf("deleted %v, want %s, which is not up to date", got, ms[1].Name)
	}
}

// While the roll waits for a Machine to join, a pass writes neither the ChalkosControlPlane nor a
// Machine.
func TestWaitingRollWritesNothing(t *testing.T) {
	e := newEnv(t, 3, true)
	e.rollingControlPlane(3)
	e.setVersion("v1.37.2")
	e.mustReconcile()
	e.wantMachines(4)
	e.mustReconcile()
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.RollingOutWaitingReason, "to join")
	e.wantNoWrite()
}
