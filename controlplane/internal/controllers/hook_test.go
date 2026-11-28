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
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
)

// deleteClusterHeld deletes the Cluster, which a finalizer keeps until the test ends.
func (e *env) deleteClusterHeld() {
	e.t.Helper()
	key := client.ObjectKey{Namespace: e.ns, Name: clusterName}
	var cluster clusterv1.Cluster
	if err := testClient.Get(e.t.Context(), key, &cluster); err != nil {
		e.t.Fatal(err)
	}
	controllerutil.AddFinalizer(&cluster, holdFinalizer)
	if err := testClient.Update(e.t.Context(), &cluster); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		var cluster clusterv1.Cluster
		if err := testClient.Get(context.Background(), key, &cluster); err == nil {
			controllerutil.RemoveFinalizer(&cluster, holdFinalizer)
			_ = testClient.Update(context.Background(), &cluster)
		}
	})
	if err := testClient.Delete(e.t.Context(), &cluster); err != nil {
		e.t.Fatal(err)
	}
}

// A control-plane Machine gets the hook when it is created; its ChalkosConfig does not. One that
// lost it gets it back.
func TestHookOnControlPlaneMachines(t *testing.T) {
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	if m.Annotations[machineutil.HookAnnotation] != machineutil.ControlPlaneHookOwner {
		t.Fatalf("the new Machine's annotations %v", m.Annotations)
	}
	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: m.Name}, &cfg); err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Annotations[machineutil.HookAnnotation]; ok {
		t.Error("the ChalkosConfig has the hook")
	}
	original := m.DeepCopy()
	delete(m.Annotations, machineutil.HookAnnotation)
	if err := testClient.Patch(t.Context(), &m, client.MergeFrom(original)); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	if got := e.wantMachines(1)[0]; got.Annotations[machineutil.HookAnnotation] != machineutil.ControlPlaneHookOwner {
		t.Errorf("an existing Machine did not get the hook back: %v", got.Annotations)
	}
}

// A control-plane Machine being deleted does not get the hook back.
func TestNoHookOnADeletingControlPlaneMachine(t *testing.T) {
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.hold(e.wantMachines(1)[0].Name)
	original := m.DeepCopy()
	delete(m.Annotations, machineutil.HookAnnotation)
	if err := testClient.Patch(t.Context(), m, client.MergeFrom(original)); err != nil {
		t.Fatal(err)
	}
	if err := testClient.Delete(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	if err := testClient.Get(t.Context(), client.ObjectKeyFromObject(m), m); err != nil {
		t.Fatal(err)
	}
	if machineutil.HasHook(m) {
		t.Error("a Machine being deleted got the hook")
	}
}

// Deleting the Cluster removes every hook at once, also of the Machines being deleted, and no
// Machine is created while it is being deleted.
func TestClusterDeletionRemovesEveryHook(t *testing.T) {
	e := newEnv(t, 3, true)
	first := e.initialize("10.0.0.1")
	second := other(e.wantMachines(2), first.Name)
	e.deleteHeld(second.Name)
	e.deleteClusterHeld()
	e.mustReconcile()
	for _, m := range e.wantMachines(2) {
		if machineutil.HasHook(&m) {
			t.Errorf("the Machine %s keeps its hook", m.Name)
		}
	}
}

// A ChalkosControlPlane deleted with its Cluster removes the hooks of its Machines, which it
// deletes, and asks no node to leave etcd.
func TestDeletionWithTheClusterRemovesEveryHook(t *testing.T) {
	e := newEnv(t, 3, true)
	first := e.initialize("10.0.0.1")
	e.hold(first.Name)
	e.deleteClusterHeld()
	if err := testClient.Delete(t.Context(), e.cp()); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	e.mustReconcile()
	for _, m := range e.machines() {
		if machineutil.HasHook(&m) || m.DeletionTimestamp.IsZero() {
			t.Errorf("the Machine %s: hook %v, deleted %v", m.Name, machineutil.HasHook(&m), !m.DeletionTimestamp.IsZero())
		}
	}
	if n, _ := e.fake.Node("10.0.0.1"); n.Leaves != 0 {
		t.Errorf("%d leaves", n.Leaves)
	}
}

// sorted is ms oldest first, as the controller orders them.
func sorted(ms []clusterv1.Machine) []clusterv1.Machine {
	slices.SortFunc(ms, func(a, b clusterv1.Machine) int {
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return ms
}

// controlPlanes brings the control plane to n joined Machines at 10.0.0.1, 10.0.0.2, …, whose
// etcd lists one healthy member each, and returns them oldest first with their addresses.
func (e *env) controlPlanes(n int) ([]clusterv1.Machine, map[string]string) {
	e.t.Helper()
	addrs := map[string]string{}
	first := e.initialize("10.0.0.1")
	addrs[first.Name] = "10.0.0.1"
	for i := 0; len(addrs) < n; i++ {
		if i > 3*n {
			e.t.Fatalf("%d of %d control planes", len(addrs), n)
		}
		var next *clusterv1.Machine
		for _, m := range e.machines() {
			if _, ok := addrs[m.Name]; !ok {
				next = &m
				break
			}
		}
		if next == nil {
			e.mustReconcile()
			continue
		}
		addr := fmt.Sprintf("10.0.0.%d", len(addrs)+1)
		e.provision(*next, addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
		addrs[next.Name] = addr
	}
	e.mustReconcile()
	ms := sorted(e.wantMachines(n))
	e.setMembers(ms, addrs)
	return ms, addrs
}

// setMembers makes each machine of ms list one healthy voting member per Machine.
func (e *env) setMembers(ms []clusterv1.Machine, addrs map[string]string) {
	var members []*kubernetesv1.EtcdMember
	for i, m := range ms {
		members = append(members, &kubernetesv1.EtcdMember{Id: uint64(i + 1), Name: m.Name, PeerUrls: []string{"https://" + addrs[m.Name] + ":2380"}})
	}
	for _, m := range ms {
		e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members = members })
	}
}

// atHook deletes the Machine name, held by a finalizer, and marks it as waiting for its
// pre-terminate hooks.
func (e *env) atHook(name string) {
	e.t.Helper()
	e.deleteHeld(name)
	e.markWaiting(name)
}

// markWaiting marks the Machine name, being deleted, as waiting for its pre-terminate hooks, as
// Cluster API's Machine controller does once the drain finished.
func (e *env) markWaiting(name string) {
	e.t.Helper()
	var m clusterv1.Machine
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, &m); err != nil {
		e.t.Fatal(err)
	}
	conditions.Set(&m, metav1.Condition{Type: clusterv1.MachineDeletingCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineDeletingWaitingForPreTerminateHookReason})
	if err := testClient.Status().Update(e.t.Context(), &m); err != nil {
		e.t.Fatal(err)
	}
}

// machine returns the Machine name as it is now.
func (e *env) machine(name string) *clusterv1.Machine {
	e.t.Helper()
	var m clusterv1.Machine
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, &m); err != nil {
		e.t.Fatal(err)
	}
	return &m
}

// hooked tells whether the Machine name still carries the hook.
func (e *env) hooked(name string) bool {
	e.t.Helper()
	return machineutil.HasHook(e.machine(name))
}

// prunes counts the PruneRevoked calls the machine at addr answered.
func (e *env) prunes(addr string) int {
	n, _ := e.fake.Node(addr)
	return n.Prunes
}

// nextEvent is the next event rec recorded, empty when there is none: the reconciler records
// events before it returns.
func nextEvent(rec *events.FakeRecorder) string {
	select {
	case ev := <-rec.Events:
		return ev
	default:
		return ""
	}
}

// A deleted control plane waits for the drain; then it leaves etcd, is revoked through a peer as
// a control plane, has the peer prune the revoked list, and loses its hook.
func TestControlPlaneLeavesAndIsRevoked(t *testing.T) {
	e := newEnv(t, 3, true)
	rec := events.NewFakeRecorder(20)
	e.r.Recorder = rec
	ms, addrs := e.controlPlanes(3)
	gone := ms[1].Name
	e.deleteHeld(gone)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[gone]); n.Leaves != 0 || !e.hooked(gone) {
		t.Fatal("acted before Cluster API waits for the hook")
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, "drained")
	e.markWaiting(gone)
	var allow []bool
	e.fake.Update(addrs[ms[0].Name], func(n *nodetest.Node) {
		n.RevokeNode = func(_ string, a bool) (*kubernetesv1.RevokeNodeResponse, error) {
			allow = append(allow, a)
			return &kubernetesv1.RevokeNodeResponse{}, nil
		}
	})
	prunes := e.prunes(addrs[ms[0].Name])
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[gone]); n.Leaves != 1 {
		t.Errorf("%d leaves, want 1", n.Leaves)
	}
	if n, _ := e.fake.Node(addrs[ms[0].Name]); !slices.Equal(n.Revoked, []string{gone}) || !slices.Equal(allow, []bool{true}) {
		t.Errorf("revoked %v with allowControlPlane %v through the oldest peer", n.Revoked, allow)
	}
	if got := e.prunes(addrs[ms[0].Name]); got != prunes+1 {
		t.Errorf("%d prunes after the revocation, want %d", got, prunes+1)
	}
	if e.hooked(gone) {
		t.Error("the hook is kept")
	}
	if ev := nextEvent(rec); !strings.Contains(ev, "ControlPlaneRemoved") || !strings.Contains(ev, gone) {
		t.Errorf("event %q", ev)
	}
	if n, _ := e.fake.Node(addrs[ms[2].Name]); slices.ContainsFunc(n.Members, func(m *kubernetesv1.EtcdMember) bool { return m.GetName() == gone }) {
		t.Errorf("etcd still lists %s: %v", gone, n.Members)
	}
}

// Etcd's quorum refusal keeps the hook and is tried again, without status writes that would
// reconcile again at once; a restarted reconciler repeats the leave, which a node out of etcd
// answers with NotEtcdMember, and finishes.
func TestControlPlaneLeaveRefusedForQuorum(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.EtcdLeave = func() error { return nodetest.QuorumRefused(gone) } })
	e.atHook(gone)
	if res := e.mustReconcile(); res.RequeueAfter > machineutil.HookRetry || res.RequeueAfter == 0 {
		t.Errorf("reconcile = %+v", res)
	}
	if !e.hooked(gone) {
		t.Fatal("a refused leave removed the hook")
	}
	c := conditions.Get(e.cp(), controlplanev1.ScalingDownCondition)
	if c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, gone) || !strings.Contains(c.Message, "quorum") {
		t.Errorf("ScalingDown = %+v", c)
	}
	for _, m := range ms[:2] {
		if n, _ := e.fake.Node(addrs[m.Name]); len(n.Removed) != 0 {
			t.Errorf("removed %v through %s after a refused leave", n.Removed, m.Name)
		}
	}
	cpVersion, mVersion := e.cp().ResourceVersion, e.machine(gone).ResourceVersion
	e.mustReconcile()
	if e.cp().ResourceVersion != cpVersion || e.machine(gone).ResourceVersion != mVersion {
		t.Error("a waiting hook wrote the ChalkosControlPlane or the Machine again")
	}

	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.EtcdLeave = func() error { return nodetest.NotEtcdMember() } })
	e.r = e.reconciler()
	e.mustReconcile()
	if e.hooked(gone) {
		t.Error("a node out of etcd keeps the hook")
	}
}

// A node that answers Unavailable itself, as when its etcd does not answer, was reached: its
// member is not removed through a peer, and the leave is tried again.
func TestControlPlaneLeaveUnavailableIsTriedAgain(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.EtcdLeave = func() error { return nodetest.EtcdUnavailable() } })
	e.atHook(gone)
	if res := e.mustReconcile(); res.RequeueAfter != machineutil.HookRetry {
		t.Errorf("reconcile = %+v", res)
	}
	if n, _ := e.fake.Node(addrs[ms[0].Name]); len(n.Removed) != 0 || len(n.Revoked) != 0 {
		t.Errorf("through %s: removed %v, revoked %v", ms[0].Name, n.Removed, n.Revoked)
	}
	if !e.hooked(gone) {
		t.Error("the hook is gone")
	}
}

// An unreachable node's member is removed through a peer, the next one when one is no etcd
// member, and the node is revoked through the next one when one fails.
func TestUnreachableControlPlaneIsRemovedThroughAPeer(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[0].Name
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.Err = nodetest.Unreachable(addrs[gone]) })
	e.fake.Update(addrs[ms[1].Name], func(n *nodetest.Node) {
		n.EtcdRemoveMember = func(string) error { return nodetest.NotEtcdMember() }
		n.RevokeNode = func(string, bool) (*kubernetesv1.RevokeNodeResponse, error) {
			return nil, connect.NewError(connect.CodeInternal, "the API server refused the policy").WithRemote()
		}
	})
	e.atHook(gone)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[ms[2].Name]); !slices.Equal(n.Removed, []string{gone}) || !slices.Equal(n.Revoked, []string{gone}) {
		t.Errorf("through %s: removed %v, revoked %v", ms[2].Name, n.Removed, n.Revoked)
	}
	if e.hooked(gone) {
		t.Error("the hook is kept")
	}
}

// A peer's refusal to remove an unreachable node's member, as for etcd's quorum, is the cluster's:
// no other peer is asked, and the hook waits.
func TestRemovalRefusedByAPeerAsksNoOther(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[0].Name
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.Err = nodetest.Unreachable(addrs[gone]) })
	e.fake.Update(addrs[ms[1].Name], func(n *nodetest.Node) {
		n.EtcdRemoveMember = func(member string) error { return nodetest.QuorumRefused(member) }
	})
	e.atHook(gone)
	if res := e.mustReconcile(); res.RequeueAfter != machineutil.HookRetry {
		t.Errorf("reconcile = %+v", res)
	}
	if n, _ := e.fake.Node(addrs[ms[2].Name]); len(n.Removed) != 0 || len(n.Revoked) != 0 {
		t.Errorf("through %s: removed %v, revoked %v", ms[2].Name, n.Removed, n.Revoked)
	}
	if !e.hooked(gone) {
		t.Error("the hook is gone")
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, "quorum")
}

// A member etcd does not list counts as removed, unless etcd lists a learner without a name at
// the node's peer URL: one that never started, which is removed by its ID. The peer URL may be at
// any of the Machine's InternalIPs.
func TestUnstartedLearnerIsRemovedByID(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	members := []*kubernetesv1.EtcdMember{
		{Id: 1, Name: ms[0].Name, PeerUrls: []string{"https://" + addrs[ms[0].Name] + ":2380"}},
		{Id: 2, Name: ms[1].Name, PeerUrls: []string{"https://" + addrs[ms[1].Name] + ":2380"}},
		{Id: 0xbeef, Learner: true, PeerUrls: []string{"https://" + addrs[gone] + ":2380"}},
	}
	for _, m := range ms[:2] {
		e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.Members = members })
	}
	// The Machine's first InternalIP reaches nothing; etcd pinned the node's second.
	m := e.machine(gone)
	m.Status.Addresses = clusterv1.MachineAddresses{
		{Type: clusterv1.MachineInternalIP, Address: "10.0.1.99"},
		{Type: clusterv1.MachineInternalIP, Address: addrs[gone]},
	}
	if err := testClient.Status().Update(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	e.atHook(gone)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[ms[0].Name]); !slices.Equal(n.Removed, []string{"beef"}) || len(n.Members) != 2 {
		t.Errorf("removed %v, members %v, want the learner removed by its ID", n.Removed, n.Members)
	}
	if e.hooked(gone) {
		t.Error("the hook is kept")
	}
}

// A full list is pruned once and the revocation tried again; when it stays full, the hook stays
// and the condition names the CA rotations.
func TestControlPlaneRevocationOfAFullList(t *testing.T) {
	e := newEnv(t, 3, true)
	rec := events.NewFakeRecorder(20)
	e.r.Recorder = rec
	ms, addrs := e.controlPlanes(3)
	for _, m := range ms {
		e.fake.Update(addrs[m.Name], func(n *nodetest.Node) {
			n.RevokeNode = func(node string, _ bool) (*kubernetesv1.RevokeNodeResponse, error) { return nil, nodetest.RevokedListFull(node) }
		})
	}
	gone := ms[2].Name
	prunes := e.prunes(addrs[ms[0].Name])
	e.clock.Step(machineutil.FullListPruneInterval)
	e.atHook(gone)
	e.mustReconcile()
	if !e.hooked(gone) {
		t.Fatal("a full list removed the hook")
	}
	if got := e.prunes(addrs[ms[0].Name]); got != prunes+1 {
		t.Errorf("%d prunes, want %d", got, prunes+1)
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.RevokedListFullReason, "chalkctl rotate os-ca")
	if ev := nextEvent(rec); !strings.Contains(ev, "Warning") {
		t.Errorf("event %q, want a warning", ev)
	}

	// The passes that follow soon after prune no more and write nothing, so the status write of
	// the first does not reconcile again and again.
	cpVersion, mVersion := e.cp().ResourceVersion, e.machine(gone).ResourceVersion
	for range 2 {
		e.clock.Step(machineutil.HookRetry)
		if res := e.mustReconcile(); res.RequeueAfter != machineutil.HookRetry {
			t.Errorf("reconcile = %+v", res)
		}
	}
	if got := e.prunes(addrs[ms[0].Name]); got != prunes+1 {
		t.Errorf("%d prunes within %s, want %d", got, machineutil.FullListPruneInterval, prunes+1)
	}
	if e.cp().ResourceVersion != cpVersion || e.machine(gone).ResourceVersion != mVersion {
		t.Error("a waiting pass wrote the ChalkosControlPlane or the Machine")
	}
	e.clock.Step(machineutil.FullListPruneInterval)
	e.mustReconcile()
	if got := e.prunes(addrs[ms[0].Name]); got != prunes+2 {
		t.Errorf("%d prunes after %s, want %d", got, machineutil.FullListPruneInterval, prunes+2)
	}
}

// A full list that a prune makes room in takes the revocation.
func TestControlPlaneRevokedAfterAPrune(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	full := true
	e.fake.Update(addrs[ms[0].Name], func(n *nodetest.Node) {
		n.RevokeNode = func(node string, _ bool) (*kubernetesv1.RevokeNodeResponse, error) {
			if full {
				return nil, nodetest.RevokedListFull(node)
			}
			return &kubernetesv1.RevokeNodeResponse{}, nil
		}
		n.PruneRevoked = func() (*kubernetesv1.PruneRevokedResponse, error) {
			full = false
			return &kubernetesv1.PruneRevokedResponse{Pruned: []string{"w0"}}, nil
		}
	})
	gone := ms[2].Name
	e.clock.Step(machineutil.FullListPruneInterval)
	e.atHook(gone)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[ms[0].Name]); !slices.Equal(n.Revoked, []string{gone}) {
		t.Errorf("revoked %v", n.Revoked)
	}
	if e.hooked(gone) {
		t.Error("the hook is kept")
	}
}

// Hooks run one Machine at a time: while one waits, the other is not asked to leave.
func TestControlPlaneHooksOneAtATime(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	for _, m := range ms[1:] {
		e.fake.Update(addrs[m.Name], func(n *nodetest.Node) { n.EtcdLeave = func() error { return nodetest.QuorumRefused(m.Name) } })
		e.atHook(m.Name)
	}
	e.mustReconcile()
	var left []string
	for _, m := range ms[1:] {
		if n, _ := e.fake.Node(addrs[m.Name]); n.Leaves > 0 {
			left = append(left, m.Name)
		}
	}
	if len(left) != 1 {
		t.Fatalf("asked %v to leave, want one", left)
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, left[0])
}

// The last control plane of a deleted ChalkosControlPlane leaves no etcd and is not revoked.
func TestLastControlPlaneSkipsEtcd(t *testing.T) {
	e := newEnv(t, 1, true)
	ms, addrs := e.controlPlanes(1)
	e.hold(ms[0].Name)
	if err := testClient.Delete(t.Context(), e.cp()); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	e.markWaiting(ms[0].Name)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[ms[0].Name]); n.Leaves != 0 || len(n.Revoked) != 0 {
		t.Errorf("the last control plane: %d leaves, revoked %v", n.Leaves, n.Revoked)
	}
	if e.hooked(ms[0].Name) {
		t.Error("the hook is kept")
	}
	if c := conditions.Get(e.cp(), controlplanev1.DeletingCondition); c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("Deleting = %+v", c)
	}
}

// A Machine whose machine the bootstrap provider never tried to install loses its hook at once,
// without a call to a node.
func TestNeverInstalledControlPlaneLosesItsHook(t *testing.T) {
	e := newEnv(t, 1, true)
	e.mustReconcile()
	name := e.wantMachines(1)[0].Name
	e.deleteHeld(name)
	e.mustReconcile()
	if e.hooked(name) {
		t.Error("the hook is kept")
	}
}

// A Machine the bootstrap provider tried to install may hold certificates: it is revoked, also
// when it never answered as its node.
func TestControlPlaneOnceReachedIsRevoked(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: gone}, &cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Status.Installed = nil
	cfg.Status.MaintenanceFingerprints = []bootstrapv1.Fingerprint{{SHA256: strings.Repeat("ab", 32), SeenAt: metav1.Now()}}
	if err := testClient.Status().Update(t.Context(), &cfg); err != nil {
		t.Fatal(err)
	}
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.Err = nodetest.Unreachable(addrs[gone]) })
	e.atHook(gone)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[ms[0].Name]); !slices.Equal(n.Revoked, []string{gone}) {
		t.Errorf("revoked %v", n.Revoked)
	}
}

// The revoked list is pruned once the control plane is initialized and once a day after; a
// failed prune is reported and does not hold the reconcile.
func TestRevokedPrunedDaily(t *testing.T) {
	e := newEnv(t, 1, true)
	rec := events.NewFakeRecorder(20)
	e.r.Recorder = rec
	ms, addrs := e.controlPlanes(1)
	addr := addrs[ms[0].Name]
	if got := e.prunes(addr); got != 1 {
		t.Fatalf("%d prunes after the initialization, want 1", got)
	}
	if e.cp().Status.LastRevokedPruneTime == nil {
		t.Error("no LastRevokedPruneTime")
	}
	e.mustReconcile()
	if got := e.prunes(addr); got != 1 {
		t.Errorf("%d prunes within a day, want 1", got)
	}
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.PruneRevoked = func() (*kubernetesv1.PruneRevokedResponse, error) {
			return nil, connect.NewError(connect.CodeInternal, "the API server does not answer").WithRemote()
		}
	})
	e.clock.Step(25 * time.Hour)
	if res := e.mustReconcile(); res.RequeueAfter == 0 || res.RequeueAfter > 24*time.Hour {
		t.Errorf("reconcile = %+v", res)
	}
	if got := e.prunes(addr); got != 2 {
		t.Errorf("%d prunes a day later, want 2", got)
	}
	var warned bool
	for ev := nextEvent(rec); ev != ""; ev = nextEvent(rec) {
		warned = warned || strings.Contains(ev, "PruneRevokedFailed")
	}
	if !warned {
		t.Error("no event of the failed prune")
	}
	e.mustReconcile()
	if got := e.prunes(addr); got != 2 {
		t.Errorf("%d prunes right after a failed one, want 2", got)
	}
}

// deadMemberRefusal is chalkd's refusal of a leave of the node name, whose own etcd member does not
// answer.
func deadMemberRefusal(name string) error {
	return connect.NewError(connect.CodeFailedPrecondition, "this node's etcd member "+name+" does not answer at https://127.0.0.1:2379; pass --through-others to remove it through the other members").WithRemote()
}

// A node whose own etcd member does not answer, while the other control planes list it as
// healthy, refuses to leave; the hook waits and says how to remove the member through another
// control plane, after which the leave is done.
func TestControlPlaneWithADeadEtcdMemberWaits(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.EtcdLeave = func() error { return deadMemberRefusal(gone) } })
	e.atHook(gone)
	e.mustReconcile()
	if !e.hooked(gone) {
		t.Fatal("a refused leave removed the hook")
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, "chalkctl etcd remove-member "+gone)

	// The operator removed the member through another control plane.
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.EtcdLeave = func() error { return nodetest.NotEtcdMember() } })
	e.clock.Step(machineutil.HookRetry)
	e.mustReconcile()
	if e.hooked(gone) {
		t.Error("the hook is kept once the member is gone")
	}
}

// A node that refuses to leave while another control plane lists its member as unhealthy has its
// member removed through that control plane, and is revoked.
func TestUnhealthyEtcdMemberIsRemovedThroughAPeer(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[2].Name
	e.setUnhealthy(ms, addrs, gone)
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.EtcdLeave = func() error { return deadMemberRefusal(gone) } })
	e.atHook(gone)
	e.mustReconcile()
	if n, _ := e.fake.Node(addrs[ms[0].Name]); !slices.Equal(n.Removed, []string{gone}) || !slices.Equal(n.Revoked, []string{gone}) {
		t.Errorf("through %s: removed %v, revoked %v", ms[0].Name, n.Removed, n.Revoked)
	}
	if e.hooked(gone) {
		t.Error("the hook is kept")
	}
}

// The text of the client's own errors for an unreachable node varies with how the dial failed;
// the condition does not, so a waiting pass writes nothing.
func TestWaitingHookIgnoresVaryingDialErrors(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, addrs := e.controlPlanes(3)
	gone := ms[0].Name
	dial := func(why string) error { return connect.NewError(connect.CodeUnavailable, "dial tcp "+addrs[gone]+":50000: "+why) }
	e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.Err = dial("connect: connection refused") })
	for _, m := range ms[1:] {
		e.fake.Update(addrs[m.Name], func(n *nodetest.Node) {
			n.EtcdRemoveMember = func(member string) error { return nodetest.QuorumRefused(member) }
		})
	}
	e.atHook(gone)
	e.mustReconcile()
	wantConditionReason(t, e.cp(), controlplanev1.ScalingDownCondition, metav1.ConditionTrue, controlplanev1.ScalingDownReason, "cannot be reached (unreachable)")
	cpVersion, mVersion := e.cp().ResourceVersion, e.machine(gone).ResourceVersion
	for _, why := range []string{"i/o timeout", "connect: no route to host"} {
		e.fake.Update(addrs[gone], func(n *nodetest.Node) { n.Err = dial(why) })
		e.clock.Step(machineutil.HookRetry)
		e.mustReconcile()
	}
	if e.cp().ResourceVersion != cpVersion || e.machine(gone).ResourceVersion != mVersion {
		t.Error("a waiting pass wrote the ChalkosControlPlane or the Machine")
	}
}
