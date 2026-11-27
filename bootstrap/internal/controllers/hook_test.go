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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	"github.com/trevex/chalkos/pkg/chalkd/layers/kubernetes/kubernetestest"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
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
