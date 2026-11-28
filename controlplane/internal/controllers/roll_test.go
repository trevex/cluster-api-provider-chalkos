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
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
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

// With maxSurge 1, a replacement that never joins holds the roll until the change is reverted;
// then it is the only Machine not up to date and goes without its join, and the roll ends.
func TestStuckSurgeMachineGoesOnRevert(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.rollingControlPlane(3)
	e.setVersion("v1.37.2")
	e.mustReconcile()
	stuck := e.newcomer(e.wantMachines(4), addrs)
	e.hold(stuck.Name)
	e.mustReconcile()
	if got := e.deleted(); len(got) != 0 {
		t.Fatalf("deleted %v before the replacement joined", got)
	}
	e.setVersion(version)
	e.mustReconcile()
	if got := e.deleted(); !slices.Equal(got, []string{stuck.Name}) {
		t.Fatalf("deleted %v, want the replacement %s that never joined", got, stuck.Name)
	}
	e.finishRemoval(stuck.Name, ms, addrs)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionFalse, controlplanev1.NotRollingOutReason, "")
}

// With maxSurge 0, a replacement that never joins goes without its join once the change is
// reverted, and a Machine of the reverted spec replaces it.
func TestStuckReplacementGoesOnRevertWithoutSurge(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.rollingControlPlane(3)
	e.setMaxSurge(0)
	e.setVersion("v1.37.2")
	e.mustReconcile()
	e.finishRemoval(ms[0].Name, ms[1:], addrs)
	e.mustReconcile()
	stuck := e.newcomer(e.wantMachines(3), addrs)
	e.hold(stuck.Name)
	e.setVersion(version)
	e.mustReconcile()
	if got := e.deleted(); !slices.Equal(got, []string{stuck.Name}) {
		t.Fatalf("deleted %v, want the replacement %s that never joined", got, stuck.Name)
	}
	e.finishRemoval(stuck.Name, ms[1:], addrs)
	e.mustReconcile()
	repl := e.newcomer(e.wantMachines(3), addrs)
	if repl.Name == stuck.Name || repl.Spec.Version != version {
		t.Errorf("the new Machine %s is of %s, want a new one of %s", repl.Name, repl.Spec.Version, version)
	}
}

// With maxSurge 0, no Machine goes while etcd has an unhealthy member or a learner.
func TestRollWithoutSurgeWaitsForHealthyEtcd(t *testing.T) {
	for name, tc := range map[string]struct {
		member func(*kubernetesv1.EtcdMember)
		want   string
	}{
		"unhealthy": {func(m *kubernetesv1.EtcdMember) { m.Unhealthy = "context deadline exceeded" }, "unhealthy"},
		"a learner": {func(m *kubernetesv1.EtcdMember) { m.Learner = true }, "learner"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 3, true)
			ms, addrs := e.rollingControlPlane(3)
			var members []*kubernetesv1.EtcdMember
			for i, m := range ms {
				members = append(members, &kubernetesv1.EtcdMember{Id: uint64(i + 1), Name: m.Name, PeerUrls: []string{"https://" + addrs[m.Name] + ":2380"}})
			}
			tc.member(members[1])
			for _, m := range ms {
				e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members = members })
			}
			e.setMaxSurge(0)
			e.setVersion("v1.37.2")
			e.mustReconcile()
			if got := e.deleted(); len(got) != 0 {
				t.Fatalf("deleted %v while etcd is not healthy", got)
			}
			wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.RollingOutWaitingReason, tc.want)
		})
	}
}

// A change of the spec while the surge Machine joins leaves the oldest Machine to go first, once
// the surge Machine joined.
func TestSpecChangeMidSurgeDeletesTheOldestFirst(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.rollingControlPlane(3)
	e.setVersion("v1.37.2")
	e.mustReconcile()
	repl := e.newcomer(e.wantMachines(4), addrs)
	e.hold(repl.Name)
	e.setVersion("v1.37.3")
	e.mustReconcile()
	if got := e.deleted(); len(got) != 0 {
		t.Fatalf("deleted %v before the surge Machine joined", got)
	}
	e.provision(repl, "10.0.1.1", kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
	addrs[repl.Name] = "10.0.1.1"
	e.setMembers(append(slices.Clone(ms), repl), addrs)
	e.mustReconcile()
	// Every Machine joined, so the oldest is the first by creation time, then by name: the
	// surge Machine too, when it was created in the same second as the others.
	want := sorted(e.machines())[0].Name
	if got := e.deleted(); !slices.Equal(got, []string{want}) {
		t.Fatalf("deleted %v, want the oldest %s", got, want)
	}
}

// withoutLearnerOf drops the Machine's learner, by name or by peer URL, and nothing else.
func TestWithoutLearnerOf(t *testing.T) {
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "d"}, Status: clusterv1.MachineStatus{Addresses: clusterv1.MachineAddresses{
		{Type: clusterv1.MachineInternalIP, Address: "10.0.0.4"},
	}}}
	members := []*kubernetesv1.EtcdMember{
		{Id: 1, Name: "a"},
		{Id: 2, Name: "d"},
		{Id: 3, Name: "b", Learner: true},
		{Id: 4, Learner: true, PeerUrls: []string{"https://10.0.0.5:2380"}},
	}
	named := []*kubernetesv1.EtcdMember{{Id: 5, Name: "d", Learner: true}}
	nameless := []*kubernetesv1.EtcdMember{{Id: 6, Learner: true, PeerUrls: []string{"https://10.0.0.4:2380"}}}
	ids := func(ms []*kubernetesv1.EtcdMember) []uint64 {
		var out []uint64
		for _, mem := range ms {
			out = append(out, mem.GetId())
		}
		return out
	}
	if got := ids(withoutLearnerOf(members, m)); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
		t.Errorf("without the learner of d: %v, want every member", got)
	}
	if got := ids(withoutLearnerOf(append(slices.Clone(members), named...), m)); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
		t.Errorf("without the named learner of d: %v", got)
	}
	if got := ids(withoutLearnerOf(append(slices.Clone(members), nameless...), m)); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
		t.Errorf("without the nameless learner of d: %v", got)
	}
}

// Among Machines created in the same second, the first that joined is the oldest; a Machine alone
// in its second is the oldest without asking whether it joined.
func TestOldestOf(t *testing.T) {
	at := func(name string, sec int64) *clusterv1.Machine {
		return &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.Unix(sec, 0)}}
	}
	joinedOf := func(names ...string) func(*clusterv1.Machine) (bool, error) {
		return func(m *clusterv1.Machine) (bool, error) { return slices.Contains(names, m.Name), nil }
	}
	asked := func(*clusterv1.Machine) (bool, error) {
		t.Error("asked whether a Machine joined without a tie")
		return false, nil
	}
	for name, tc := range map[string]struct {
		ms     []*clusterv1.Machine
		joined func(*clusterv1.Machine) (bool, error)
		want   string
	}{
		"none":                          {nil, asked, ""},
		"no tie":                        {[]*clusterv1.Machine{at("b", 1), at("a", 2)}, asked, "b"},
		"a tie, the first joined":       {[]*clusterv1.Machine{at("a", 1), at("b", 1)}, joinedOf("a", "b"), "a"},
		"a tie, only the second joined": {[]*clusterv1.Machine{at("a", 1), at("b", 1), at("c", 2)}, joinedOf("b", "c"), "b"},
		"a tie, a later one joined":     {[]*clusterv1.Machine{at("a", 1), at("b", 1), at("c", 2)}, joinedOf("c"), "a"},
	} {
		got, err := oldestOf(tc.ms, tc.joined)
		if err != nil {
			t.Fatal(err)
		}
		if got := ptr.Deref(got, clusterv1.Machine{}).Name; got != tc.want {
			t.Errorf("%s: oldestOf = %q, want %q", name, got, tc.want)
		}
	}
}
