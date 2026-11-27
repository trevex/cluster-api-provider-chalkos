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
	"slices"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
)

// setReplicas sets the ChalkosControlPlane's spec.replicas.
func (e *env) setReplicas(n int32) {
	e.t.Helper()
	cp := e.cp()
	cp.Spec.Replicas = ptr.To(n)
	if err := testClient.Update(e.t.Context(), cp); err != nil {
		e.t.Fatal(err)
	}
}

// deleted are the names of the control plane's Machines being deleted.
func (e *env) deleted() []string {
	e.t.Helper()
	var names []string
	for _, m := range e.machines() {
		if !m.DeletionTimestamp.IsZero() {
			names = append(names, m.Name)
		}
	}
	return names
}

// 3 → 1 deletes the oldest Machine, waits until it is gone, then deletes the next oldest, and
// never goes below the replicas.
func TestScaleDownOldestFirstOneAtATime(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	for _, m := range ms {
		e.hold(m.Name)
	}
	e.setReplicas(1)
	e.mustReconcile()
	if got := e.deleted(); !slices.Equal(got, []string{ms[0].Name}) {
		t.Fatalf("deleted %v, want the oldest %s", got, ms[0].Name)
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, ms[0].Name)
	e.mustReconcile()
	if got := e.deleted(); len(got) != 1 {
		t.Fatalf("deleted %v while the first is still being deleted", got)
	}
	e.markWaiting(ms[0].Name)
	e.mustReconcile() // the hook runs and is removed
	if e.hooked(ms[0].Name) {
		t.Fatal("the first Machine keeps its hook")
	}
	if got := e.deleted(); len(got) != 1 {
		t.Fatalf("deleted %v before the first is gone", got)
	}
	e.release(ms[0].Name)
	e.setMembers(ms[1:], addrs)
	e.mustReconcile()
	if got := e.deleted(); !slices.Equal(got, []string{ms[1].Name}) {
		t.Fatalf("deleted %v, want %s next", got, ms[1].Name)
	}
	e.markWaiting(ms[1].Name)
	e.mustReconcile()
	e.release(ms[1].Name)
	e.setMembers(ms[2:], addrs)
	e.mustReconcile()
	e.mustReconcile()
	if got := e.deleted(); len(got) != 0 || len(e.machines()) != 1 {
		t.Errorf("deleted %v of %d Machines at one replica", got, len(e.machines()))
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionFalse, controlplanev1.NotScalingDownReason, "")
}

// No step starts while a Machine has not joined, or etcd's members are not one healthy voter per
// Machine; ScalingDown says why, and a waiting pass writes nothing.
func TestScaleDownWaitsForHealth(t *testing.T) {
	for name, tc := range map[string]struct {
		degrade func(e *env, ms []clusterv1.Machine, addrs map[string]string)
		want    string
	}{
		"a Machine joining": {func(e *env, ms []clusterv1.Machine, addrs map[string]string) {
			e.setStatus(addrs[ms[2].Name], kubernetesv1.Phase_PHASE_JOINING, false)
		}, "to join"},
		"an unhealthy member": {func(e *env, ms []clusterv1.Machine, addrs map[string]string) {
			for _, m := range ms {
				e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members[1].Unhealthy = "context deadline exceeded" })
			}
		}, "unhealthy"},
		"a learner": {func(e *env, ms []clusterv1.Machine, addrs map[string]string) {
			for _, m := range ms {
				e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members[2].Learner = true })
			}
		}, "learner"},
		"a missing member": {func(e *env, ms []clusterv1.Machine, addrs map[string]string) {
			e.setMembers(ms[:2], addrs)
			e.fake.Update(addrs[ms[2].Name], func(n *nodetest.Node) { n.Members = n.Members[:2] })
		}, "2 members for 3 Machines"},
		"no control plane answers": {func(e *env, ms []clusterv1.Machine, addrs map[string]string) {
			for _, m := range ms {
				e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members = nil })
			}
		}, "no control plane lists"},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 3, true)
			ms, addrs := e.controlPlanes(3)
			tc.degrade(e, ms, addrs)
			e.setReplicas(1)
			if res := e.mustReconcile(); res.RequeueAfter != machineutil.RetryAfter {
				t.Errorf("reconcile = %+v", res)
			}
			if got := e.deleted(); len(got) != 0 {
				t.Fatalf("deleted %v", got)
			}
			wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionFalse, controlplanev1.ScalingDownWaitingReason, tc.want)
			version := e.cp().ResourceVersion
			e.clock.Step(machineutil.RetryAfter)
			e.mustReconcile()
			if e.cp().ResourceVersion != version {
				t.Error("a waiting pass wrote the ChalkosControlPlane")
			}
		})
	}
}

// A 3-replica control plane whose Machine being deleted is down and still an etcd member does not
// grow to a fourth member; once the member is gone, it creates the replacement.
func TestScaleUpWaitsForEtcdWhileAMachineIsDeleted(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	for _, m := range ms[:2] {
		e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members[2].Unhealthy = "dial tcp: connection refused" })
	}
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.Err = nodetest.Unreachable(addrs[gone]) })
	e.deleteHeld(gone)
	if res := e.mustReconcile(); res.RequeueAfter != machineutil.RetryAfter {
		t.Errorf("reconcile = %+v", res)
	}
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "unhealthy")
	version := e.cp().ResourceVersion
	e.clock.Step(machineutil.RetryAfter)
	e.mustReconcile()
	if e.cp().ResourceVersion != version {
		t.Error("a waiting pass wrote the ChalkosControlPlane")
	}

	e.setMembers(ms[:2], addrs)
	e.mustReconcile()
	e.wantMachines(4)
}

// A single control plane being deleted gets its replacement, which joins its etcd, and only then
// leaves etcd and is revoked through it, also when Cluster API waits for the hook at once.
func TestSingleControlPlaneIsReplaced(t *testing.T) {
	e := newEnv(t, 1, true)
	ms, addrs := e.controlPlanes(1)
	old := ms[0].Name
	e.atHook(old)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[old]); n.Leaves != 0 || !e.hooked(old) {
		t.Fatalf("the only control plane: %d leaves, hook %v, before its replacement exists", n.Leaves, e.hooked(old))
	}
	ms2 := e.wantMachines(2)
	repl := other(ms2, old)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, "replacement")

	e.provision(repl, "10.0.0.2", kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
	addrs[repl.Name] = "10.0.0.2"
	e.setMembers([]clusterv1.Machine{ms[0], repl}, addrs)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[old]); n.Leaves != 1 {
		t.Errorf("%d leaves, want 1", n.Leaves)
	}
	if n, _ := e.fake.Node("10.0.0.2"); !slices.Equal(n.Revoked, []string{old}) {
		t.Errorf("revoked %v through the replacement", n.Revoked)
	}
	if e.hooked(old) {
		t.Error("the hook is kept")
	}
}

func TestMembersProblem(t *testing.T) {
	machines := func(names ...string) []clusterv1.Machine {
		var ms []clusterv1.Machine
		for _, n := range names {
			ms = append(ms, clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: n}})
		}
		return ms
	}
	members := func(names ...string) []*kubernetesv1.EtcdMember {
		var ms []*kubernetesv1.EtcdMember
		for i, n := range names {
			ms = append(ms, &kubernetesv1.EtcdMember{Id: uint64(i + 1), Name: n})
		}
		return ms
	}
	for name, tc := range map[string]struct {
		members            []*kubernetesv1.EtcdMember
		machines, deleting []clusterv1.Machine
		want               string
	}{
		"healthy":                     {members("a", "b"), machines("a", "b"), nil, ""},
		"a member of no Machine":      {members("a", "c"), machines("a", "b"), nil, `"c"`},
		"too few":                     {members("a"), machines("a", "b"), nil, "1 member for 2 Machines"},
		"too many":                    {members("a", "b", "c"), machines("a", "b"), nil, "3 members for 2 Machines"},
		"a deleted one still in etcd": {members("a", "b", "d"), machines("a", "b"), machines("d"), ""},
		"a deleted one out of etcd":   {members("a", "b"), machines("a", "b"), machines("d"), ""},
		"a Machine without a member":  {members("a", "d"), machines("a", "b"), machines("d"), "the Machine b"},
		"a nameless learner":          {[]*kubernetesv1.EtcdMember{{Name: "a"}, {Id: 0xbeef, Learner: true}}, machines("a", "b"), nil, "beef is a learner"},
	} {
		got := membersProblem(tc.members, tc.machines, tc.deleting)
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("%s: membersProblem = %q, want %q", name, got, tc.want)
		}
	}
}
