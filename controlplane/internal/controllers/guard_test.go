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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
)

// wantNoWrite fails the test when a pass after the retry interval writes the ChalkosControlPlane.
func (e *env) wantNoWrite() {
	e.t.Helper()
	version := e.cp().ResourceVersion
	e.clock.Step(machineutil.RetryAfter)
	e.mustReconcile()
	if e.cp().ResourceVersion != version {
		e.t.Error("a waiting pass wrote the ChalkosControlPlane")
	}
}

// The Machines move to a version in one step when it is of their minor or the next; another
// major, an older minor or more than one minor newer is refused, naming the Machine.
func TestVersionSkew(t *testing.T) {
	ms := func(versions ...string) []clusterv1.Machine {
		var out []clusterv1.Machine
		for i, v := range versions {
			out = append(out, clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("m%d", i)}, Spec: clusterv1.MachineSpec{Version: v}})
		}
		return out
	}
	for _, tc := range []struct {
		version  string
		machines []clusterv1.Machine
		want     string
	}{
		{"v1.37.2", ms("v1.37.1"), ""},
		{"v1.37.0", ms("v1.37.1"), ""},
		{"v1.38.0", ms("v1.37.1"), ""},
		{"v1.38.0", ms("v1.38.0", "v1.37.1"), ""},
		{"v1.38.0", nil, ""},
		{"v1.39.0", ms("v1.37.1"), "one minor at a time, to v1.38 first"},
		{"v1.39.0", ms("v1.38.0", "v1.37.1"), "the Machine m1"},
		{"v1.36.9", ms("v1.37.1"), "does not downgrade a minor"},
		{"v2.0.0", ms("v1.37.1"), "another major version"},
		{"latest", ms("v1.37.1"), "no Kubernetes version"},
		{"v1.37.1", ms("bogus"), "which is no Kubernetes version"},
	} {
		got := versionSkew(tc.version, tc.machines)
		if (tc.want == "") != (got == "") || !strings.Contains(got, tc.want) {
			t.Errorf("versionSkew(%s) = %q, want %q", tc.version, got, tc.want)
		}
	}
}

// A version more than one minor ahead creates no Machine: no replacement of a deleted Machine,
// and the waiting pass writes nothing. One minor ahead creates the replacement.
func TestVersionSkewHoldsEveryNewMachine(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, _ := e.controlPlanes(3)
	e.setVersion("v1.39.0")
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.VersionSkewReason, "one minor at a time")
	e.deleteHeld(ms[2].Name)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "VersionSkew")
	e.wantNoWrite()
	e.setVersion("v1.38.0")
	e.mustReconcile()
	e.wantMachines(4)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.RollingOutReason, "2 of 3 Machines not up to date")
}

// An older minor is refused too.
func TestOlderMinorHolds(t *testing.T) {
	e := newEnv(t, 3, true)
	e.controlPlanes(3)
	e.setVersion("v1.36.5")
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.VersionSkewReason, "does not downgrade")
}

// A change of the Cluster's domain in spec.files, a field every node holds alike, creates no
// Machine, scale-up included, and names the document, the field and both values; reverting it
// lifts the hold, and the deleted Machine is replaced.
func TestFixedFieldChangeHoldsEveryNewMachine(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	e.setFiles(clusterFile("lab.local"))
	ms, _ := e.controlPlanes(3)
	if cp := e.cp(); cp.Status.ObservedFiles == "" || cp.Status.ObservedFiles != e.observed(cp) {
		t.Fatalf("observedFiles %q, want the cluster-wide documents", cp.Status.ObservedFiles)
	}
	e.setFiles(clusterFile("example.org"))
	e.mustReconcile()
	e.wantMachines(3)
	cp := e.cp()
	wantConditionReason(t, cp, controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.FixedFieldChangedReason, `Cluster/default spec.domain from "lab.local" to "example.org"`)
	if c := conditions.Get(cp, controlplanev1.RollingOutCondition); !strings.Contains(c.Message, "revert") {
		t.Errorf("RollingOut %q does not say to revert", c.Message)
	}
	e.deleteHeld(ms[2].Name)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "FixedFieldChanged")
	e.wantNoWrite()
	e.setFiles(clusterFile("lab.local"))
	e.mustReconcile()
	e.wantMachines(4)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionFalse, controlplanev1.NotRollingOutReason, "")
}

// A change of a fixed field through the Cluster leaves every Machine up to date: the refusal
// shows on ScalingUp, with or without a Machine to create, and no Machine is created until the
// change is reverted.
func TestClusterFixedFieldChangeShowsOnScalingUp(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	e.setFiles(clusterFile("lab.local"))
	ms, _ := e.controlPlanes(3)
	cluster := e.cluster()
	pods := cluster.Spec.ClusterNetwork.Pods.CIDRBlocks
	cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"10.200.0.0/16"}
	if err := testClient.Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	cp := e.cp()
	wantConditionReason(t, cp, controlplanev1.RollingOutCondition, metav1.ConditionFalse, controlplanev1.NotRollingOutReason, "")
	wantConditionReason(t, cp, controlplanev1.ScalingUpCondition, metav1.ConditionFalse, controlplanev1.FixedFieldChangedReason, "10.200.0.0/16")
	e.deleteHeld(ms[2].Name)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.FixedFieldChangedReason, "revert")
	e.wantNoWrite()
	cluster = e.cluster()
	cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = pods
	if err := testClient.Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	e.wantMachines(4)
}

// When the image the changed documents are checked against cannot be read, no Machine is
// created, RollingOut waits, and the check is tried again without writing anything new.
func TestUnreadableImageHolds(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	e.setFiles(clusterFile("lab.local"))
	ms, _ := e.controlPlanes(3)
	cp := e.cp()
	cp.Spec.Image = "registry.example/chalkos@sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := testClient.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.deleteHeld(ms[2].Name)
	if res := e.mustReconcile(); res.RequeueAfter != machineutil.RetryAfter {
		t.Errorf("reconcile = %+v, want a retry", res)
	}
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.RollingOutWaitingReason, "image")
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "checked against the image")
	e.wantNoWrite()
}
