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
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	"github.com/trevex/chalkos/pkg/chalkd/layers/kubernetes/kubernetestest"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
)

// holdFinalizer keeps a deleted object until the test releases it.
const holdFinalizer = "test.chalkos.dev/hold"

// machineObj returns the Machine name.
func (e *env) machineObj(name string) *clusterv1.Machine {
	e.t.Helper()
	var m clusterv1.Machine
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: name}, &m); err != nil {
		e.t.Fatal(err)
	}
	return &m
}

// deleteHeld deletes obj, which a finalizer keeps until the test ends.
func (e *env) deleteHeld(obj client.Object) {
	e.t.Helper()
	key := client.ObjectKeyFromObject(obj)
	if err := testClient.Get(e.ctx, key, obj); err != nil {
		e.t.Fatal(err)
	}
	controllerutil.AddFinalizer(obj, holdFinalizer)
	if err := testClient.Update(e.ctx, obj); err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() {
		held := obj.DeepCopyObject().(client.Object)
		if err := testClient.Get(context.Background(), key, held); err == nil {
			controllerutil.RemoveFinalizer(held, holdFinalizer)
			_ = testClient.Update(context.Background(), held)
		}
	})
	if err := testClient.Delete(e.ctx, obj); err != nil {
		e.t.Fatal(err)
	}
}

// deleteMachineHeld deletes the Machine name, which a finalizer keeps until the test ends.
func (e *env) deleteMachineHeld(name string) {
	e.t.Helper()
	e.deleteHeld(&clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: name}})
}

// setDeleting gives the Machine name the Deleting condition with the reason, as Cluster API's
// Machine controller does.
func (e *env) setDeleting(name, reason string) {
	e.t.Helper()
	m := e.machineObj(name)
	conditions.Set(m, metav1.Condition{Type: clusterv1.MachineDeletingCondition, Status: metav1.ConditionTrue, Reason: reason})
	if err := testClient.Status().Update(e.ctx, m); err != nil {
		e.t.Fatal(err)
	}
}

// atHook deletes the Machine name and marks it as waiting for its pre-terminate hooks.
func (e *env) atHook(name string) {
	e.t.Helper()
	e.deleteMachineHeld(name)
	e.setDeleting(name, clusterv1.MachineDeletingWaitingForPreTerminateHookReason)
}

// markInstalled records the machine of the ChalkosConfig name as installed.
func (e *env) markInstalled(name string) {
	e.t.Helper()
	c := e.config(name)
	c.Status.Installed = ptr.To(true)
	if err := testClient.Status().Update(e.ctx, c); err != nil {
		e.t.Fatal(err)
	}
}

// A worker Machine gets the bootstrap provider's hook at its first reconcile; a control-plane
// Machine does not, and a Machine with the hook keeps its value.
func TestHookOnWorkers(t *testing.T) {
	e := newEnv(t, false, kubernetestest.Image)
	e.machine("w-1", false, "")
	e.machine("cp-1", true, "")
	e.mustReconcile("w-1")
	e.mustReconcile("cp-1")
	if got := e.machineObj("w-1").Annotations[machineutil.HookAnnotation]; got != machineutil.BootstrapHookOwner {
		t.Errorf("the worker's hook %q", got)
	}
	if machineutil.HasHook(e.machineObj("cp-1")) {
		t.Error("the bootstrap provider set a control plane's hook")
	}
	m := e.machineObj("w-1")
	original := m.DeepCopy()
	m.Annotations[machineutil.HookAnnotation] = "mine"
	if err := testClient.Patch(e.ctx, m, client.MergeFrom(original)); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile("w-1")
	if got := e.machineObj("w-1").Annotations[machineutil.HookAnnotation]; got != "mine" {
		t.Errorf("the hook's value became %q", got)
	}
}

// Deleting the Cluster removes the hook at once: an installed worker being deleted then is not
// revoked.
func TestClusterDeletionRemovesTheHook(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, "")
	e.mustReconcile("w-1")
	e.markInstalled("w-1")
	if !machineutil.HasHook(e.machineObj("w-1")) {
		t.Fatal("the worker has no hook")
	}
	e.atHook("w-1")
	e.deleteHeld(&clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: clusterName}})
	e.mustReconcile("w-1")
	if machineutil.HasHook(e.machineObj("w-1")) {
		t.Error("the worker keeps its hook while the Cluster is being deleted")
	}
	c := conditions.Get(e.config("w-1"), bootstrapv1.NodeRemovedCondition)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != bootstrapv1.DoneReason {
		t.Errorf("NodeRemoved = %+v, want true with the reason %s", c, bootstrapv1.DoneReason)
	}
	if n, _ := e.fake.Node(addr); len(n.Revoked) != 0 {
		t.Errorf("revoked %v", n.Revoked)
	}
}

// Deleting the Cluster removes the hook of a worker that is not being deleted, which gets no
// NodeRemoved condition.
func TestClusterDeletionRemovesTheHookOfAWorkerNotDeleted(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, "")
	e.mustReconcile("w-1")
	if !machineutil.HasHook(e.machineObj("w-1")) {
		t.Fatal("the worker has no hook")
	}
	e.deleteHeld(&clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: clusterName}})
	e.mustReconcile("w-1")
	if machineutil.HasHook(e.machineObj("w-1")) {
		t.Error("the worker keeps its hook while the Cluster is being deleted")
	}
	if c := conditions.Get(e.config("w-1"), bootstrapv1.NodeRemovedCondition); c != nil {
		t.Errorf("NodeRemoved = %+v on a worker not being deleted", c)
	}
}

// cpAddr is the address of the control plane cp-1 in revokableWorker.
const cpAddr = "10.0.0.10"

// revokableWorker serves the installed worker w-1 at addr, which carries the hook, and the
// control plane cp-1 at cpAddr, installed and answering, and returns the env with a recorder.
func revokableWorker(t *testing.T) (*env, *events.FakeRecorder) {
	t.Helper()
	e := newEnv(t, true, kubernetestest.Image)
	rec := events.NewFakeRecorder(20)
	e.r.Recorder = rec
	e.machine("cp-1", true, cpAddr)
	e.fake.Set(cpAddr, nodetest.Node{Name: "cp-1"})
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Name: "w-1"})
	e.mustReconcile("w-1")
	e.markInstalled("w-1")
	if !machineutil.HasHook(e.machineObj("w-1")) {
		t.Fatal("the worker has no hook")
	}
	return e, rec
}

// Nothing is revoked while the Machine drains; once Cluster API waits for the hook, the worker is
// revoked through a control plane, as no etcd member, and the hook goes.
func TestWorkerRevokedAtThePreTerminateHook(t *testing.T) {
	e, rec := revokableWorker(t)
	e.deleteMachineHeld("w-1")
	e.setDeleting("w-1", clusterv1.MachineDeletingDrainingNodeReason)
	e.mustReconcile("w-1")
	if n, _ := e.fake.Node(cpAddr); len(n.Revoked) != 0 || !machineutil.HasHook(e.machineObj("w-1")) {
		t.Fatalf("revoked %v while draining", n.Revoked)
	}
	e.setDeleting("w-1", clusterv1.MachineDeletingWaitingForPreTerminateHookReason)
	var allow []bool
	e.fake.Update(cpAddr, func(n *nodetest.Node) {
		n.RevokeNode = func(_ string, allowControlPlane bool) (*kubernetesv1.RevokeNodeResponse, error) {
			allow = append(allow, allowControlPlane)
			return &kubernetesv1.RevokeNodeResponse{RevokedAt: timestamppb.Now(), ListLength: 120, ListCapacity: 90_000}, nil
		}
	})
	e.mustReconcile("w-1")
	if n, _ := e.fake.Node(cpAddr); !slices.Equal(n.Revoked, []string{"w-1"}) || !slices.Equal(allow, []bool{false}) {
		t.Errorf("revoked %v with allowControlPlane %v", n.Revoked, allow)
	}
	if machineutil.HasHook(e.machineObj("w-1")) {
		t.Error("the hook is kept")
	}
	c := conditions.Get(e.config("w-1"), bootstrapv1.NodeRemovedCondition)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != bootstrapv1.DoneReason || !strings.Contains(c.Message, "120 of 90000") {
		t.Errorf("NodeRemoved = %+v", c)
	}
	if ev := nextEvent(rec); !strings.Contains(ev, "NodeRevoked") {
		t.Errorf("event %q", ev)
	}
}

// A control plane that does not answer is passed over; with none left the hook stays, the step
// is tried again after HookRetry and a reconciler started anew revokes the node.
func TestWorkerRevocationWaitsForAControlPlane(t *testing.T) {
	e, rec := revokableWorker(t)
	e.fake.Update(cpAddr, func(n *nodetest.Node) { n.Err = nodetest.Unreachable(cpAddr) })
	e.atHook("w-1")
	res := e.mustReconcile("w-1")
	if res.RequeueAfter != machineutil.HookRetry || !machineutil.HasHook(e.machineObj("w-1")) {
		t.Fatalf("reconcile = %+v; hook kept %v", res, machineutil.HasHook(e.machineObj("w-1")))
	}
	if c := conditions.Get(e.config("w-1"), bootstrapv1.NodeRemovedCondition); c == nil || c.Reason != bootstrapv1.WaitingForControlPlaneReason || !strings.Contains(c.Message, "cp-1") {
		t.Errorf("NodeRemoved = %+v", c)
	}
	if ev := nextEvent(rec); !strings.HasPrefix(ev, "Warning") {
		t.Errorf("event %q", ev)
	}
	e.machine("cp-2", true, "10.0.0.11")
	e.fake.Set("10.0.0.11", nodetest.Node{Name: "cp-2"})
	e.r = &ChalkosConfigReconciler{Client: testClient, APIReader: testClient, Node: e.fake}
	e.mustReconcile("w-1")
	if n, _ := e.fake.Node("10.0.0.11"); !slices.Equal(n.Revoked, []string{"w-1"}) || machineutil.HasHook(e.machineObj("w-1")) {
		t.Errorf("revoked through cp-2: %v", n.Revoked)
	}
}

// A full list is pruned once through the control plane that refused it and the revocation tried
// again; still full, the hook stays with RevokedListFull, naming the CA rotations. A node revoked
// already counts as revoked.
func TestWorkerRevocationOfAFullList(t *testing.T) {
	e, _ := revokableWorker(t)
	e.fake.Update(cpAddr, func(n *nodetest.Node) {
		n.RevokeNode = func(node string, _ bool) (*kubernetesv1.RevokeNodeResponse, error) { return nil, nodetest.RevokedListFull(node) }
	})
	e.atHook("w-1")
	if res := e.mustReconcile("w-1"); res.RequeueAfter != machineutil.HookRetry {
		t.Errorf("reconcile = %+v", res)
	}
	c := conditions.Get(e.config("w-1"), bootstrapv1.NodeRemovedCondition)
	if c == nil || c.Reason != bootstrapv1.RevokedListFullReason || !strings.Contains(c.Message, "chalkctl rotate os-ca") || !machineutil.HasHook(e.machineObj("w-1")) {
		t.Fatalf("NodeRemoved = %+v", c)
	}
	if n, _ := e.fake.Node(cpAddr); n.Prunes != 1 {
		t.Errorf("pruned %d times, want once", n.Prunes)
	}
	// chalkd answers a node revoked already with its first revocation time: success.
	e.fake.Update(cpAddr, func(n *nodetest.Node) { n.RevokeNode = nil })
	e.mustReconcile("w-1")
	if machineutil.HasHook(e.machineObj("w-1")) {
		t.Error("the hook is kept after the revocation")
	}
}

// A full list that a prune makes room in revokes the node at the second try.
func TestWorkerRevocationAfterAPrune(t *testing.T) {
	e, _ := revokableWorker(t)
	full := true
	e.fake.Update(cpAddr, func(n *nodetest.Node) {
		n.RevokeNode = func(node string, _ bool) (*kubernetesv1.RevokeNodeResponse, error) {
			if full {
				return nil, nodetest.RevokedListFull(node)
			}
			return &kubernetesv1.RevokeNodeResponse{RevokedAt: timestamppb.Now()}, nil
		}
		n.PruneRevoked = func() (*kubernetesv1.PruneRevokedResponse, error) {
			full = false
			return &kubernetesv1.PruneRevokedResponse{Pruned: []string{"old"}}, nil
		}
	})
	e.atHook("w-1")
	e.mustReconcile("w-1")
	if n, _ := e.fake.Node(cpAddr); !slices.Equal(n.Revoked, []string{"w-1"}) || n.Prunes != 1 || machineutil.HasHook(e.machineObj("w-1")) {
		t.Errorf("revoked %v after %d prunes", n.Revoked, n.Prunes)
	}
}

// A worker that was never installed loses its hook without a revocation.
func TestNeverInstalledWorkerLosesItsHook(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, "")
	e.mustReconcile("w-1")
	e.deleteMachineHeld("w-1")
	e.mustReconcile("w-1")
	if machineutil.HasHook(e.machineObj("w-1")) {
		t.Error("the hook is kept")
	}
	wantReason(t, e.config("w-1"), metav1.ConditionFalse, bootstrapv1.MachineDeletingReason)
	c := conditions.Get(e.config("w-1"), bootstrapv1.NodeRemovedCondition)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != bootstrapv1.DoneReason {
		t.Errorf("NodeRemoved = %+v", c)
	}
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
