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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/event"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
)

// wantNoWrite fails the test when a pass after the retry interval writes the ChalkosControlPlane
// or one of its Machines.
func (e *env) wantNoWrite() {
	e.t.Helper()
	cpVersion := e.cp().ResourceVersion
	versions := map[string]string{}
	for _, m := range e.machines() {
		versions[m.Name] = m.ResourceVersion
	}
	e.clock.Step(machineutil.RetryAfter)
	e.mustReconcile()
	if e.cp().ResourceVersion != cpVersion {
		e.t.Error("a waiting pass wrote the ChalkosControlPlane")
	}
	for _, m := range e.machines() {
		if m.ResourceVersion != versions[m.Name] {
			e.t.Errorf("a waiting pass wrote the Machine %s", m.Name)
		}
	}
}

// ownedFile is a Cluster document that sets spec.endpoint, which Cluster API owns.
var ownedFile = runtime.RawExtension{Raw: []byte(`{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": {"endpoint": "https://other:6443"}}`)}

// A change of the Cluster's endpoint, controlPlaneRef or clusterNetwork reconciles the control
// plane; one of its other fields does not.
func TestClusterInputsChanged(t *testing.T) {
	base := &clusterv1.Cluster{Spec: clusterv1.ClusterSpec{
		ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "10.0.0.1", Port: 6443},
		ClusterNetwork:       clusterv1.ClusterNetwork{Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"10.244.0.0/16"}}},
	}}
	p := clusterInputsChanged()
	for name, tc := range map[string]struct {
		edit func(*clusterv1.Cluster)
		want bool
	}{
		"the endpoint":         {func(c *clusterv1.Cluster) { c.Spec.ControlPlaneEndpoint.Port = 443 }, true},
		"the controlPlaneRef":  {func(c *clusterv1.Cluster) { c.Spec.ControlPlaneRef.Name = "other" }, true},
		"the pod ranges":       {func(c *clusterv1.Cluster) { c.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"10.200.0.0/16"} }, true},
		"the service ranges":   {func(c *clusterv1.Cluster) { c.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"10.96.0.0/12"} }, true},
		"the service domain":   {func(c *clusterv1.Cluster) { c.Spec.ClusterNetwork.ServiceDomain = "lab.local" }, true},
		"the labels":           {func(c *clusterv1.Cluster) { c.Labels = map[string]string{"a": "b"} }, false},
		"the availability gate": {func(c *clusterv1.Cluster) { c.Spec.AvailabilityGates = []clusterv1.ClusterAvailabilityGate{{ConditionType: "X"}} }, false},
	} {
		cur := base.DeepCopy()
		tc.edit(cur)
		if got := p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: cur}); got != tc.want {
			t.Errorf("a change of %s passes %v, want %v", name, got, tc.want)
		}
	}
}

// Files no control-plane Machine can be made from, as one setting a field Cluster API owns,
// create no Machine, also the first, before any observed files are recorded.
func TestInvalidFilesHoldTheFirstMachine(t *testing.T) {
	e := newEnv(t, 1, true)
	e.setFiles(ownedFile)
	e.mustReconcile()
	e.wantMachines(0)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.InvalidFilesReason, "spec.endpoint, which Cluster API owns")
	e.wantNoWrite()
	e.setFiles(clusterFile("lab.local"))
	e.mustReconcile()
	e.wantMachines(1)
}

// Invalid files hold the replacement of a deleted Machine once observed files are recorded too,
// and RollingOut reports them while the Machines are of other files.
func TestInvalidFilesHoldEveryNewMachine(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	ms, _ := e.controlPlanes(3)
	if e.cp().Status.ObservedFiles == "" {
		t.Fatal("no observedFiles recorded")
	}
	e.setFiles(ownedFile)
	e.deleteHeld(ms[2].Name)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.InvalidFilesReason, "cannot be made into a control-plane Machine's documents")
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "InvalidFiles")
	e.wantNoWrite()
}

// A Machine being deleted counts in the skew: it runs until it is gone, so a single control
// plane being replaced gets no replacement more than one minor ahead of it.
func TestVersionSkewCountsDeletingMachines(t *testing.T) {
	e := newEnv(t, 1, true)
	ms, _ := e.controlPlanes(1)
	e.setVersion("v1.39.0")
	e.atHook(ms[0].Name)
	e.mustReconcile()
	e.wantMachines(1)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.VersionSkewReason, "to v1.38 first")
	e.wantNoWrite()
}

// status.observedFiles that cannot be read holds every new Machine with a remedy, the record's
// clearing, which lifts the hold.
func TestUnreadableRecordHolds(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	ms, _ := e.controlPlanes(3)
	cp := e.cp()
	cp.Status.ObservedFiles = "{{{"
	if err := testClient.Status().Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.deleteHeld(ms[2].Name)
	e.mustReconcile()
	e.wantMachines(3)
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.InvalidFilesReason, "--subresource=status")
	e.wantNoWrite()
	cp = e.cp()
	cp.Status.ObservedFiles = ""
	if err := testClient.Status().Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	e.wantMachines(4)
}

// A held change of a fixed field keeps neither scale-down nor the pre-terminate hooks from
// working: 3 → 1 deletes the oldest Machine, and its hook runs.
func TestHeldChangeStillScalesDown(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	ms, _ := e.controlPlanes(3)
	for _, m := range ms {
		e.hold(m.Name)
	}
	e.setFiles(clusterFile("example.org"))
	e.setReplicas(1)
	e.mustReconcile()
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionTrue, controlplanev1.FixedFieldChangedReason, "example.org")
	if got := e.deleted(); !slices.Equal(got, []string{ms[0].Name}) {
		t.Fatalf("deleted %v, want the oldest %s", got, ms[0].Name)
	}
	e.markWaiting(ms[0].Name)
	e.mustReconcile()
	if e.hooked(ms[0].Name) {
		t.Error("the deleted Machine keeps its hook")
	}
	e.wantMachines(3)
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
		{"v1.37.1", ms(""), "has no spec.version; set it"},
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
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionTrue, controlplanev1.ScalingUpReason, "not checked against it")
	e.wantNoWrite()
}

// While no Machine is wanted, an image that cannot be read holds nothing: ScalingUp says that the
// observed files wait for it, alike at every pass.
func TestUnreadableImageInSteadyState(t *testing.T) {
	e := newEnv(t, 3, true)
	e.serveImage()
	e.controlPlanes(3)
	e.r = e.reconciler()
	if err := testClient.Delete(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "pull"}}); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionFalse, controlplanev1.NotScalingUpReason, "cannot be read, so status.observedFiles is not recorded yet")
	wantConditionReason(t, e.cp(), controlplanev1.RollingOutCondition, metav1.ConditionFalse, controlplanev1.NotRollingOutReason, "")
	e.wantNoWrite()
}

// status.versions counts the Machines being deleted, as status.replicas does: they run until
// they are gone, so a worker must not be newer than them either.
func TestVersionsCountDeletingMachines(t *testing.T) {
	e := newEnv(t, 3, true)
	ms, _ := e.controlPlanes(3)
	e.deleteHeld(ms[2].Name)
	e.mustReconcile()
	cp := e.cp()
	var sum int32
	for _, v := range cp.Status.Versions {
		sum += v.Replicas
	}
	if want := ptr.Deref(cp.Status.Replicas, 0); sum != want || len(e.deleted()) != 1 {
		t.Errorf("versions %+v count %d Machines, want the %d replicas, the one being deleted included", cp.Status.Versions, sum, want)
	}
}
