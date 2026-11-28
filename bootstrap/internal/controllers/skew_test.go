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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	"github.com/trevex/chalkos/pkg/chalkd/layers/kubernetes/kubernetestest"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/schema/schematest"
	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/chalkostest"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
)

// A worker is newer when of a newer minor than the control plane's oldest; a newer patch is not.
// Versions of different majors or that are none are errors.
func TestWorkerSkew(t *testing.T) {
	for _, tc := range []struct {
		worker, oldest string
		newer          bool
		older          uint
		err            bool
	}{
		{"v1.37.1", "v1.37.1", false, 0, false},
		{"v1.37.2", "v1.37.1", false, 0, false},
		{"v1.38.0", "v1.37.1", true, 0, false},
		{"v1.34.0", "v1.37.1", false, 3, false},
		{"v1.33.9", "v1.37.1", false, 4, false},
		{"v2.0.0", "v1.37.1", false, 0, true},
		{"latest", "v1.37.1", false, 0, true},
	} {
		newer, older, err := workerSkew(tc.worker, tc.oldest)
		if newer != tc.newer || older != tc.older || (err != nil) != tc.err {
			t.Errorf("workerSkew(%s, %s) = %v, %d, %v", tc.worker, tc.oldest, newer, older, err)
		}
	}
}

// setControlPlane sets the ChalkosControlPlane's spec.files and its status's observedFiles and
// versions, oldest first.
func (e *env) setControlPlane(files []runtime.RawExtension, observed string, versions ...string) {
	e.t.Helper()
	cp := e.controlPlane()
	cp.Spec.Files = files
	if err := testClient.Update(e.ctx, cp); err != nil {
		e.t.Fatal(err)
	}
	cp.Status.ObservedFiles = observed
	cp.Status.Versions = nil
	for _, v := range versions {
		cp.Status.Versions = append(cp.Status.Versions, clusterv1.StatusVersion{Version: v, Replicas: 1})
	}
	if err := testClient.Status().Update(e.ctx, cp); err != nil {
		e.t.Fatal(err)
	}
}

// controlPlane returns the env's ChalkosControlPlane.
func (e *env) controlPlane() *controlplanev1.ChalkosControlPlane {
	e.t.Helper()
	var cp controlplanev1.ChalkosControlPlane
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cp); err != nil {
		e.t.Fatal(err)
	}
	return &cp
}

// clusterFiles are the env's cluster files, of the domain.
func clusterFiles(domain string) []runtime.RawExtension {
	return []runtime.RawExtension{{Raw: []byte(strings.Replace(clusterFile, "lab.local", domain, 1))}}
}

// schemaImage is the env's image with chalkos's schemas, whose contract marks the fields every
// node of a cluster holds alike.
func schemaImage(t *testing.T) *ociimage.Artifact {
	t.Helper()
	chalkostest.Chdir(t)
	return &ociimage.Artifact{
		Reference: image,
		Header:    ociimage.Header{RootHash: rootHash},
		Contract: ociimage.Contract{
			Schemas: schematest.Files(t, schematest.Builtin, schematest.Networkd),
			Facts:   map[string][]byte{"kubernetes": []byte(kubernetestest.Image)},
		},
	}
}

// observed is the status.observedFiles the control-plane provider records for the files, with the
// contract of schemaImage.
func (e *env) observed(files []runtime.RawExtension) string {
	e.t.Helper()
	var cluster clusterv1.Cluster
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cluster); err != nil {
		e.t.Fatal(err)
	}
	docs, err := documents.ClusterWide(&cluster, e.controlPlane().Spec.Version, files)
	if err != nil {
		e.t.Fatal(err)
	}
	c, err := sdk.ArtifactContract(layers.All(), schemaImage(e.t))
	if err != nil {
		e.t.Fatal(err)
	}
	text, err := documents.Observed(c, docs)
	if err != nil {
		e.t.Fatal(err)
	}
	return text
}

// wantWaiting fails the test unless the ChalkosConfig name waits with the reason and a message
// that contains part, and another pass writes nothing.
func (e *env) wantWaiting(name, reason, part string) {
	e.t.Helper()
	c := e.config(name)
	wantReason(e.t, c, metav1.ConditionFalse, reason)
	if msg := conditions.Get(c, bootstrapv1.ReadyCondition).Message; !strings.Contains(msg, part) {
		e.t.Errorf("Ready's message %q does not contain %q", msg, part)
	}
	e.mustReconcile(name)
	if e.config(name).ResourceVersion != c.ResourceVersion {
		e.t.Error("a waiting pass wrote the ChalkosConfig")
	}
	e.wantInstalls(addr, 0)
}

// A worker of a newer minor than the control plane's oldest waits for the upgrade and installs
// once the control plane caught up; a newer patch installs at once.
func TestWorkerWaitsForTheControlPlaneUpgrade(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.setControlPlane(clusterFiles("lab.local"), "", "v1.36.4", "v1.37.1")
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	if res := e.mustReconcile("w-1"); res.RequeueAfter != controlPlaneWait {
		t.Errorf("reconcile = %+v, want a requeue after %s", res, controlPlaneWait)
	}
	e.wantWaiting("w-1", bootstrapv1.WaitingForControlPlaneUpgradeReason, "v1.36.4")
	e.setControlPlane(clusterFiles("lab.local"), "", "v1.37.0")
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
	if c := conditions.Get(e.config("w-1"), bootstrapv1.VersionSkewCondition); c == nil || c.Status != metav1.ConditionFalse || c.Reason != bootstrapv1.WithinVersionSkewReason {
		t.Errorf("VersionSkew = %+v", c)
	}
}

// A control-plane Machine is not held by the control plane's versions: it is the upgrade.
func TestControlPlaneMachineIgnoresTheVersions(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.setControlPlane(clusterFiles("lab.local"), "", "v1.36.4")
	e.machine("cp-2", true, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("cp-2")
	e.wantInstalls(addr, 1)
	if conditions.Has(e.config("cp-2"), bootstrapv1.VersionSkewCondition) {
		t.Error("a control-plane Machine has the VersionSkew condition")
	}
}

// A worker more than three minors older than the control plane installs with the warning
// VersionSkew.
func TestOldWorkerInstallsWithAWarning(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.setControlPlane(clusterFiles("lab.local"), "", "v1.41.0")
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
	c := conditions.Get(e.config("w-1"), bootstrapv1.VersionSkewCondition)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != bootstrapv1.VersionSkewReason || !strings.Contains(c.Message, "4 minors older") {
		t.Errorf("VersionSkew = %+v", c)
	}
}

// A Machine that is not installed, a worker or of the control plane, waits while the
// ChalkosControlPlane's cluster-wide documents change a field every node of the cluster holds
// alike against status.observedFiles, and installs once the change is reverted.
func TestMachineWaitsWhileAFixedFieldChanges(t *testing.T) {
	for _, controlPlane := range []bool{false, true} {
		t.Run(map[bool]string{false: "worker", true: "control plane"}[controlPlane], func(t *testing.T) {
			e := newEnv(t, true, kubernetestest.Image)
			e.fake.SetImage(image, schemaImage(t))
			e.setControlPlane(clusterFiles("example.org"), e.observed(clusterFiles("lab.local")))
			e.machine("m-1", controlPlane, addr)
			e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
			e.mustReconcile("m-1")
			e.wantWaiting("m-1", bootstrapv1.FixedFieldChangedReason, `domain from "lab.local" to "example.org"`)
			e.setControlPlane(clusterFiles("lab.local"), e.observed(clusterFiles("lab.local")))
			e.mustReconcile("m-1")
			e.wantInstalls(addr, 1)
		})
	}
}

// A change of a field every node holds alike that is no fixed-field change, such as a new
// document, installs.
func TestMachineInstallsWithoutAFixedFieldChange(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.fake.SetImage(image, schemaImage(t))
	disk := runtime.RawExtension{Raw: []byte(`{"apiVersion": "storage.chalkos.dev/v1alpha1", "kind": "SystemDisk", "metadata": {"name": "system"}, "spec": {"disk": {"path": "/dev/vda"}}}`)}
	e.setControlPlane(append(clusterFiles("lab.local"), disk), e.observed(clusterFiles("lab.local")))
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
}

// A Machine is refused while the ChalkosControlPlane's files make no cluster-wide documents, and
// waits while its status.observedFiles cannot be compared with them, as the control plane holds
// InvalidFiles.
func TestMachineWaitsForInvalidControlPlaneFiles(t *testing.T) {
	owned := []runtime.RawExtension{{Raw: []byte(`{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": {"endpoint": "https://other:6443"}}`)}}
	e := newEnv(t, true, kubernetestest.Image)
	e.fake.SetImage(image, schemaImage(t))
	e.setControlPlane(owned, "")
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	e.wantWaiting("w-1", bootstrapv1.ConfigRefusedReason, "spec.endpoint")

	e.setControlPlane(clusterFiles("lab.local"), "{{{")
	e.mustReconcile("w-1")
	e.wantWaiting("w-1", bootstrapv1.ObservedFilesInvalidReason, "status.observedFiles")
	e.setControlPlane(clusterFiles("lab.local"), "")
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
}

// A Machine waits while the ChalkosControlPlane's image, whose contract marks the fixed fields,
// cannot be read, with a message that stays alike.
func TestMachineWaitsForTheControlPlaneImage(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.fake.SetImage(image, schemaImage(t))
	e.setControlPlane(clusterFiles("lab.local"), e.observed(clusterFiles("lab.local")))
	cp := e.controlPlane()
	cp.Spec.Image = "registry.example/other@sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	if err := testClient.Update(e.ctx, cp); err != nil {
		t.Fatal(err)
	}
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	e.wantWaiting("w-1", bootstrapv1.InstallingReason, "registry.example/other@sha256:")
}

// Status changes of a ChalkosControlPlane's versions and observedFiles reach its Machines'
// ChalkosConfigs; other status changes do not.
func TestControlPlaneStatusChanged(t *testing.T) {
	p := controlPlaneStatusChanged()
	old := &controlplanev1.ChalkosControlPlane{}
	versions := old.DeepCopy()
	versions.Status.Versions = []clusterv1.StatusVersion{{Version: "v1.37.1", Replicas: 1}}
	observed := old.DeepCopy()
	observed.Status.ObservedFiles = "{}"
	other := old.DeepCopy()
	other.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	for name, tc := range map[string]struct {
		cur  *controlplanev1.ChalkosControlPlane
		want bool
	}{
		"versions":      {versions, true},
		"observedFiles": {observed, true},
		"other":         {other, false},
		"none":          {old.DeepCopy(), false},
	} {
		if got := p.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: tc.cur}); got != tc.want {
			t.Errorf("%s: passed %v, want %v", name, got, tc.want)
		}
	}
}

// A status change of the ChalkosControlPlane reaches only the ChalkosConfigs that are not
// installed; a change of its spec reaches every one.
func TestControlPlaneStatusReachesUninstalledConfigs(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, "")
	e.machine("w-2", false, "")
	c := e.config("w-1")
	c.Status.Installed = ptr.To(true)
	if err := testClient.Status().Update(e.ctx, c); err != nil {
		t.Fatal(err)
	}
	cp := e.controlPlane()
	if reqs := e.r.controlPlaneToUninstalled(e.ctx, cp); len(reqs) != 1 || reqs[0].Name != "w-2" {
		t.Errorf("a status change maps to %v, want w-2", reqs)
	}
	if reqs := e.r.controlPlaneToConfigs(e.ctx, cp); len(reqs) != 2 {
		t.Errorf("a spec change maps to %v, want w-1 and w-2", reqs)
	}
}
