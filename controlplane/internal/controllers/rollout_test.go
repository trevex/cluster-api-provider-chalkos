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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	"github.com/trevex/chalkos/pkg/chalkd/layers/kubernetes/kubernetestest"
	"github.com/trevex/chalkos/pkg/config"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/schema/schematest"
	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/chalkostest"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
)

// clusterFile is a Cluster document of the domain.
func clusterFile(domain string) runtime.RawExtension {
	return runtime.RawExtension{Raw: []byte(`{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": {"domain": "` + domain +
		`", "images": {"etcd": "registry.k8s.io/etcd:3.7.0-0", "coredns": "registry.k8s.io/coredns/coredns:v1.14.6", "pause": "registry.k8s.io/pause:3.10.2", "flannel": "ghcr.io/flannel-io/flannel:v0.28.9"}}}`)}
}

// systemDiskFile is a SystemDisk document, which marks no field.
var systemDiskFile = runtime.RawExtension{Raw: []byte(`{"apiVersion": "storage.chalkos.dev/v1alpha1", "kind": "SystemDisk", "metadata": {"name": "system"}, "spec": {"disk": {"path": "/dev/vda"}}}`)}

// setFiles sets the ChalkosControlPlane's spec.files.
func (e *env) setFiles(files ...runtime.RawExtension) {
	e.t.Helper()
	cp := e.cp()
	cp.Spec.Files = files
	if err := testClient.Update(e.t.Context(), cp); err != nil {
		e.t.Fatal(err)
	}
}

// setVersion sets the ChalkosControlPlane's spec.version.
func (e *env) setVersion(v string) {
	e.t.Helper()
	cp := e.cp()
	cp.Spec.Version = v
	if err := testClient.Update(e.t.Context(), cp); err != nil {
		e.t.Fatal(err)
	}
}

// imageArtifact is the control plane's image with chalkos's built-in kinds, whose Cluster marks
// its domain as held alike by every node.
func imageArtifact(t *testing.T) *ociimage.Artifact {
	t.Helper()
	chalkostest.Chdir(t)
	return &ociimage.Artifact{Reference: image, Contract: ociimage.Contract{
		Schemas: schematest.Files(t, schematest.Builtin, schematest.Networkd),
		Facts:   map[string][]byte{"kubernetes": []byte(kubernetestest.Image)},
	}}
}

// serveImage serves the control plane's image, imageArtifact, and creates its pull Secret.
func (e *env) serveImage() {
	e.t.Helper()
	e.fake.SetImage(image, imageArtifact(e.t))
	e.create(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "pull"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths": {}}`)},
	})
}

// contract is the contract of imageArtifact.
func (e *env) contract() sdk.Contract {
	e.t.Helper()
	c, err := sdk.ArtifactContract(layers.All(), imageArtifact(e.t))
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// cluster returns the Cluster API Cluster.
func (e *env) cluster() *clusterv1.Cluster {
	e.t.Helper()
	var cluster clusterv1.Cluster
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cluster); err != nil {
		e.t.Fatal(err)
	}
	return &cluster
}

// clusterWide returns the cluster-wide documents of the Cluster and the ChalkosControlPlane.
func (e *env) clusterWide(cp *controlplanev1.ChalkosControlPlane) []config.Document {
	e.t.Helper()
	docs, err := documents.ClusterWide(e.cluster(), cp.Spec.Version, cp.Spec.Files)
	if err != nil {
		e.t.Fatal(err)
	}
	return docs
}

// observed is the status.observedFiles of the ChalkosControlPlane's Cluster, version and files
// as imageArtifact defaults them.
func (e *env) observed(cp *controlplanev1.ChalkosControlPlane) string {
	e.t.Helper()
	text, err := documents.Observed(e.contract(), e.clusterWide(cp))
	if err != nil {
		e.t.Fatal(err)
	}
	return text
}

// stamp sets the spec hash of the Machine name and of its ChalkosConfig to hash, or removes it
// when hash is empty, as on a Machine made before Machines carried one.
func (e *env) stamp(name, hash string) {
	e.t.Helper()
	for _, obj := range []client.Object{&clusterv1.Machine{}, &bootstrapv1.ChalkosConfig{}} {
		if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, obj); err != nil {
			e.t.Fatal(err)
		}
		a := obj.GetAnnotations()
		if hash == "" {
			delete(a, controlplanev1.SpecHashAnnotation)
		} else {
			a[controlplanev1.SpecHashAnnotation] = hash
		}
		obj.SetAnnotations(a)
		if err := testClient.Update(e.t.Context(), obj); err != nil {
			e.t.Fatal(err)
		}
	}
}

// config returns the ChalkosConfig name.
func (e *env) config(name string) *bootstrapv1.ChalkosConfig {
	e.t.Helper()
	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, &cfg); err != nil {
		e.t.Fatal(err)
	}
	return &cfg
}

// wantUpToDate fails the test unless the Machine name's UpToDate condition has the status and its
// message holds the text.
func (e *env) wantUpToDate(name string, status metav1.ConditionStatus, text string) {
	e.t.Helper()
	c := conditions.Get(e.machine(name), clusterv1.MachineUpToDateCondition)
	if c == nil || c.Status != status || !strings.Contains(c.Message, text) {
		e.t.Fatalf("UpToDate of %s = %+v, want %s with %q", name, c, status, text)
	}
}

// The hash covers the version, the image, the files, exactly and whatever their keys' order, and
// the infrastructure template; neither the replicas, the template's metadata nor rollout.after.
func TestSpecHash(t *testing.T) {
	base := func() *controlplanev1.ChalkosControlPlane {
		return &controlplanev1.ChalkosControlPlane{Spec: controlplanev1.ChalkosControlPlaneSpec{
			Replicas: ptr.To[int32](3), Version: version, Image: image,
			Files: []runtime.RawExtension{{Raw: []byte(`{"kind": "Cluster", "spec": {"domain": "a", "big": 12345678901234567890}}`)}},
			MachineTemplate: controlplanev1.ChalkosControlPlaneMachineTemplate{Spec: controlplanev1.ChalkosControlPlaneMachineTemplateSpec{
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "KubevirtMachineTemplate", Name: "cp"},
			}},
		}}
	}
	hash := func(edit func(*controlplanev1.ChalkosControlPlane)) string {
		cp := base()
		edit(cp)
		h, err := specHash(cp)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	same := hash(func(*controlplanev1.ChalkosControlPlane) {})
	for name, edit := range map[string]func(*controlplanev1.ChalkosControlPlane){
		"the version": func(cp *controlplanev1.ChalkosControlPlane) { cp.Spec.Version = "v1.37.2" },
		"the image": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.Image = "registry.example/chalkos@sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		},
		"a file": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.Files[0].Raw = []byte(`{"kind": "Cluster", "spec": {"domain": "b", "big": 12345678901234567890}}`)
		},
		"a big number": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.Files[0].Raw = []byte(`{"kind": "Cluster", "spec": {"domain": "a", "big": 12345678901234567891}}`)
		},
		"another file": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.Files = append(cp.Spec.Files, runtime.RawExtension{Raw: []byte(`{"kind": "Flannel"}`)})
		},
		"the infrastructure group": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.MachineTemplate.Spec.InfrastructureRef.APIGroup = "infrastructure.example.org"
		},
		"the infrastructure kind": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.MachineTemplate.Spec.InfrastructureRef.Kind = "MetalMachineTemplate"
		},
		"the infrastructure template": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.MachineTemplate.Spec.InfrastructureRef.Name = "cp2"
		},
	} {
		if hash(edit) == same {
			t.Errorf("%s does not change the hash", name)
		}
	}
	for name, edit := range map[string]func(*controlplanev1.ChalkosControlPlane){
		"the keys' order": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.Files[0].Raw = []byte(`{"spec": {"big": 12345678901234567890, "domain": "a"}, "kind": "Cluster"}`)
		},
		"the spacing": func(cp *controlplanev1.ChalkosControlPlane) {
			cp.Spec.Files[0].Raw = []byte(`{"kind":"Cluster","spec":{"domain":"a","big":12345678901234567890}}`)
		},
		"the replicas":            func(cp *controlplanev1.ChalkosControlPlane) { cp.Spec.Replicas = ptr.To[int32](5) },
		"the template's metadata": func(cp *controlplanev1.ChalkosControlPlane) { cp.Spec.MachineTemplate.ObjectMeta.Labels = map[string]string{"a": "b"} },
		"rollout.after":           func(cp *controlplanev1.ChalkosControlPlane) { cp.Spec.Rollout.After = metav1.Now() },
	} {
		if hash(edit) != same {
			t.Errorf("%s changes the hash", name)
		}
	}
}

// A new Machine and its ChalkosConfig carry the spec hash; the Machine is up to date, and the
// cluster-wide documents it was made from become status.observedFiles. A pass that finds nothing
// to change writes neither the ChalkosControlPlane nor the Machine.
func TestNewMachinesCarryTheSpecHash(t *testing.T) {
	e := newEnv(t, 1, true)
	e.serveImage()
	e.setFiles(clusterFile("lab.local"))
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	cp := e.cp()
	if cp.Status.ObservedFiles == "" || cp.Status.ObservedFiles != e.observed(cp) {
		t.Errorf("observedFiles\n%s\nwant\n%s", cp.Status.ObservedFiles, e.observed(cp))
	}
	if !strings.Contains(cp.Status.ObservedFiles, "lab.local") {
		t.Errorf("observedFiles %s lack the domain", cp.Status.ObservedFiles)
	}
	want, err := specHash(cp)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Annotations[controlplanev1.SpecHashAnnotation]; got != want {
		t.Errorf("the Machine's hash %q, want %q", got, want)
	}
	if got := e.config(m.Name).Annotations[controlplanev1.SpecHashAnnotation]; got != want {
		t.Errorf("the ChalkosConfig's hash %q, want %q", got, want)
	}
	e.mustReconcile()
	e.wantUpToDate(m.Name, metav1.ConditionTrue, "")

	cpVersion, mVersion := e.cp().ResourceVersion, e.machine(m.Name).ResourceVersion
	e.clock.Step(machineutil.RetryAfter)
	e.mustReconcile()
	if e.cp().ResourceVersion != cpVersion || e.machine(m.Name).ResourceVersion != mVersion {
		t.Error("a pass with nothing to change wrote the ChalkosControlPlane or the Machine")
	}
}

// status.observedFiles stays while a Machine is out of date, and follows a change of files that
// holds no fixed field once every Machine is made from them, as after a roll.
func TestObservedFilesAfterARoll(t *testing.T) {
	e := newEnv(t, 1, true)
	e.serveImage()
	e.setFiles(clusterFile("lab.local"))
	first := e.initialize("10.0.0.1")
	before := e.cp().Status.ObservedFiles
	e.setFiles(clusterFile("lab.local"), systemDiskFile)
	e.mustReconcile()
	if got := e.cp().Status.ObservedFiles; got != before {
		t.Errorf("observedFiles changed while the Machine is out of date:\n%s", got)
	}
	e.wantUpToDate(first.Name, metav1.ConditionFalse, "files")
	hash, err := specHash(e.cp())
	if err != nil {
		t.Fatal(err)
	}
	e.stamp(first.Name, hash)
	e.mustReconcile()
	cp := e.cp()
	if cp.Status.ObservedFiles != e.observed(cp) || !strings.Contains(cp.Status.ObservedFiles, "SystemDisk") {
		t.Errorf("observedFiles after the roll\n%s\nwant\n%s", cp.Status.ObservedFiles, e.observed(cp))
	}
}

// Without the image's contract, status.observedFiles is not recorded, ScalingUp says why, and the
// controller tries again without writing anything new.
func TestObservedFilesWaitForTheImage(t *testing.T) {
	e := newEnv(t, 1, true)
	e.setFiles(clusterFile("lab.local"))
	if res := e.mustReconcile(); res.RequeueAfter != machineutil.RetryAfter {
		t.Errorf("reconcile = %+v, want a retry", res)
	}
	e.wantMachines(1)
	if got := e.cp().Status.ObservedFiles; got != "" {
		t.Errorf("observedFiles %s recorded without the image", got)
	}
	wantConditionReason(t, e.cp(), controlplanev1.ScalingUpCondition, metav1.ConditionFalse, controlplanev1.NotScalingUpReason, "cannot be read")
	// The pass after the Machine's creation sees it listed in order; the one after writes nothing.
	e.mustReconcile()
	e.wantNoWrite()
	e.serveImage()
	e.mustReconcile()
	if cp := e.cp(); cp.Status.ObservedFiles != e.observed(cp) {
		t.Errorf("observedFiles %s once the image is read, want %s", cp.Status.ObservedFiles, e.observed(cp))
	}
}

// A Machine without the hash, as made by a provider before it, is stamped with it when it is of
// the control plane's version and image, and is up to date; one of another version is not.
func TestMachinesWithoutTheHashAreAdopted(t *testing.T) {
	e := newEnv(t, 1, true)
	first := e.initialize("10.0.0.1")
	e.stamp(first.Name, "")
	e.mustReconcile()
	want, err := specHash(e.cp())
	if err != nil {
		t.Fatal(err)
	}
	if got := e.machine(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; got != want {
		t.Errorf("the adopted Machine's hash %q, want %q", got, want)
	}
	if got := e.config(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; got != want {
		t.Errorf("the adopted ChalkosConfig's hash %q, want %q", got, want)
	}
	e.wantUpToDate(first.Name, metav1.ConditionTrue, "")

	e.stamp(first.Name, "")
	e.setVersion("v1.37.2")
	e.mustReconcile()
	if _, ok := e.machine(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; ok {
		t.Error("a Machine of another version was adopted")
	}
	e.wantUpToDate(first.Name, metav1.ConditionFalse, controlplanev1.SpecHashAnnotation)
}

// A Machine without the hash whose ChalkosConfig is of another image is not adopted.
func TestMachinesOfAnotherImageAreNotAdopted(t *testing.T) {
	e := newEnv(t, 1, true)
	first := e.initialize("10.0.0.1")
	e.stamp(first.Name, "")
	cp := e.cp()
	cp.Spec.Image = "registry.example/chalkos@sha256:1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := testClient.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	if _, ok := e.machine(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; ok {
		t.Error("a Machine of another image was adopted")
	}
	if _, ok := e.config(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; ok {
		t.Error("the ChalkosConfig of another image was stamped")
	}
	e.wantUpToDate(first.Name, metav1.ConditionFalse, "another image")
}

// A Machine created before spec.rollout.after is out of date once that time has passed; until
// then the controller reconciles again when it passes.
func TestRolloutAfter(t *testing.T) {
	e := newEnv(t, 1, true)
	e.serveImage()
	first := e.initialize("10.0.0.1")
	cp := e.cp()
	cp.Spec.Rollout.After = metav1.NewTime(e.clock.Now().Add(time.Hour))
	if err := testClient.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	if res := e.mustReconcile(); res.RequeueAfter <= 0 || res.RequeueAfter > time.Hour {
		t.Errorf("reconcile = %+v, want a requeue when rollout.after passes", res)
	}
	e.wantUpToDate(first.Name, metav1.ConditionTrue, "")
	e.clock.Step(2 * time.Hour)
	e.mustReconcile()
	e.wantUpToDate(first.Name, metav1.ConditionFalse, "spec.rollout.after")
}

// The machine template's labels and annotations reach the existing Machines in place: no Machine
// is replaced.
func TestTemplateMetadataInPlace(t *testing.T) {
	e := newEnv(t, 1, true)
	first := e.initialize("10.0.0.1")
	hash := e.machine(first.Name).Annotations[controlplanev1.SpecHashAnnotation]
	cp := e.cp()
	cp.Spec.MachineTemplate.ObjectMeta.Labels["tier"] = "gold"
	cp.Spec.MachineTemplate.ObjectMeta.Annotations = map[string]string{"example.org/note": "kept"}
	if err := testClient.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	m := e.machine(first.Name)
	if m.Labels["tier"] != "gold" || m.Annotations["example.org/note"] != "kept" || m.Labels["pool"] != "cp" {
		t.Errorf("labels %v and annotations %v, want the template's", m.Labels, m.Annotations)
	}
	if m.Annotations[controlplanev1.SpecHashAnnotation] != hash {
		t.Error("the template's metadata changed the spec hash")
	}
	e.wantUpToDate(first.Name, metav1.ConditionTrue, "")
	e.wantMachines(1)
}

// A change of a fixed field through the Cluster, which the spec hash does not cover, keeps
// status.observedFiles, so that the change is still found against it.
func TestClusterChangeKeepsTheObservedFiles(t *testing.T) {
	e := newEnv(t, 1, true)
	e.serveImage()
	e.setFiles(clusterFile("lab.local"))
	first := e.initialize("10.0.0.1")
	before := e.cp().Status.ObservedFiles
	if before == "" {
		t.Fatal("no observedFiles recorded")
	}
	cluster := e.cluster()
	cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"10.200.0.0/16"}
	if err := testClient.Update(t.Context(), cluster); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	cp := e.cp()
	if cp.Status.ObservedFiles != before {
		t.Errorf("observedFiles changed with the Cluster's pod CIDRs:\n%s", cp.Status.ObservedFiles)
	}
	e.wantUpToDate(first.Name, metav1.ConditionTrue, "")
	changes, err := documents.FixedChanges(e.contract(), cp.Status.ObservedFiles, e.clusterWide(cp))
	if err != nil || len(changes) == 0 {
		t.Errorf("FixedChanges = %v, %v; want the pod CIDRs' change", changes, err)
	}
}

// The machine template cannot set the spec hash or the pre-terminate hook: neither on a new
// Machine nor on an existing one, which stays up to date.
func TestTemplateCannotSetReservedAnnotations(t *testing.T) {
	e := newEnv(t, 1, true)
	cp := e.cp()
	cp.Spec.MachineTemplate.ObjectMeta.Annotations = map[string]string{
		controlplanev1.SpecHashAnnotation: "bogus", machineutil.HookAnnotation: "bogus", "example.org/note": "kept",
	}
	if err := testClient.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	first := e.initialize("10.0.0.1")
	e.mustReconcile()
	want, err := specHash(e.cp())
	if err != nil {
		t.Fatal(err)
	}
	m := e.machine(first.Name)
	if m.Annotations[controlplanev1.SpecHashAnnotation] != want || m.Annotations[machineutil.HookAnnotation] != machineutil.ControlPlaneHookOwner ||
		m.Annotations["example.org/note"] != "kept" {
		t.Errorf("the Machine's annotations %v, want the controller's hash and hook", m.Annotations)
	}
	if got := e.config(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; got != want {
		t.Errorf("the ChalkosConfig's hash %q, want %q", got, want)
	}
	e.wantUpToDate(first.Name, metav1.ConditionTrue, "")
}

// A Machine without the hash whose ChalkosConfig is gone is not adopted, and is out of date.
func TestMachinesWithoutTheirConfigAreNotAdopted(t *testing.T) {
	e := newEnv(t, 1, true)
	first := e.initialize("10.0.0.1")
	e.stamp(first.Name, "")
	if err := testClient.Delete(t.Context(), e.config(first.Name)); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	if _, ok := e.machine(first.Name).Annotations[controlplanev1.SpecHashAnnotation]; ok {
		t.Error("a Machine without its ChalkosConfig was adopted")
	}
	e.wantUpToDate(first.Name, metav1.ConditionFalse, "missing or of another image")
}
