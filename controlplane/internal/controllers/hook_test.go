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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
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

// Deleting the Cluster removes every hook at once, also of the Machines being deleted, and a
// Machine created while it is being deleted gets none.
func TestClusterDeletionRemovesEveryHook(t *testing.T) {
	e := newEnv(t, 3, true)
	first := e.initialize("10.0.0.1")
	second := other(e.wantMachines(2), first.Name)
	e.deleteHeld(second.Name)
	e.deleteClusterHeld()
	e.mustReconcile()
	for _, m := range e.wantMachines(3) {
		if machineutil.HasHook(&m) {
			t.Errorf("the Machine %s keeps its hook", m.Name)
		}
	}
}
