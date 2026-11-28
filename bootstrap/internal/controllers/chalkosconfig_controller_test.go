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
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
	"github.com/trevex/chalkos/pkg/chalkd/layers/kubernetes/kubernetestest"
	"github.com/trevex/chalkos/pkg/check"
	"github.com/trevex/chalkos/pkg/config"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/sdk"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

const (
	clusterName   = "c1"
	image         = "registry.example/chalkos@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	addr          = "10.0.0.1"
	fingerprint   = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	rootHash      = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	clusterImages = `{"etcd": "registry.k8s.io/etcd:3.7.0-0", "coredns": "registry.k8s.io/coredns/coredns:v1.14.6", "pause": "registry.k8s.io/pause:3.10.2", "flannel": "ghcr.io/flannel-io/flannel:v0.28.9"}`
	clusterFile   = `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": {"domain": "lab.local", "images": ` + clusterImages + `}}`
	kubeAPI       = "kubernetes.chalkos.dev/v1alpha1"
)

// env is one test's namespace with a Cluster, the fake machines and images, and the reconciler.
type env struct {
	t      *testing.T
	ctx    context.Context
	ns     string
	labels map[string]string
	fake   *nodetest.Fake
	r      *ChalkosConfigReconciler
}

// newEnv returns an env in a new namespace whose Cluster's control plane is initialized or not,
// with the cluster's secrets and the image at image, of the kubelet facts.
func newEnv(t *testing.T, initialized bool, kubelet string) *env {
	return newLabelledEnv(t, initialized, kubelet, nil)
}

// newLabelledEnv is newEnv with the labels on every object the env creates.
func newLabelledEnv(t *testing.T, initialized bool, kubelet string, labels map[string]string) *env {
	t.Helper()
	ctx := t.Context()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "bootstrap-"}}
	if err := testClient.Create(ctx, ns); err != nil {
		t.Fatal(err)
	}
	fake := nodetest.New()
	fake.SetImage(image, &ociimage.Artifact{
		Reference: image,
		Header:    ociimage.Header{RootHash: rootHash},
		Contract:  ociimage.Contract{Facts: map[string][]byte{"kubernetes": []byte(kubelet)}},
	})
	e := &env{t: t, ctx: ctx, ns: ns.Name, labels: labels, fake: fake, r: &ChalkosConfigReconciler{
		Client:    testClient,
		APIReader: testClient,
		Node:      fake,
	}}

	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: clusterName},
		Spec: clusterv1.ClusterSpec{
			ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "10.96.100.1", Port: 6443},
			ClusterNetwork: clusterv1.ClusterNetwork{
				Pods:     clusterv1.NetworkRanges{CIDRBlocks: []string{"10.244.0.0/16"}},
				Services: clusterv1.NetworkRanges{CIDRBlocks: []string{"10.96.0.0/12"}},
			},
			ControlPlaneRef: clusterv1.ContractVersionedObjectReference{APIGroup: controlplanev1.GroupVersion.Group, Kind: "ChalkosControlPlane", Name: clusterName},
		},
	}
	e.create(cluster)
	if initialized {
		cluster.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
		if err := testClient.Status().Update(ctx, cluster); err != nil {
			t.Fatal(err)
		}
	}
	cp := &controlplanev1.ChalkosControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: e.ns, Name: clusterName,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: cluster.Name, UID: cluster.UID}},
		},
		Spec: controlplanev1.ChalkosControlPlaneSpec{
			Version: "v1.37.1",
			Image:   image,
			MachineTemplate: controlplanev1.ChalkosControlPlaneMachineTemplate{
				Spec: controlplanev1.ChalkosControlPlaneMachineTemplateSpec{
					InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "KubevirtMachineTemplate", Name: "cp"},
				},
			},
			Files: []runtime.RawExtension{{Raw: []byte(clusterFile)}},
		},
	}
	e.create(cp)
	owner := metav1.OwnerReference{APIVersion: controlplanev1.GroupVersion.String(), Kind: "ChalkosControlPlane", Name: cp.Name, UID: cp.UID}
	if _, err := secrets.Ensure(ctx, testClient, client.ObjectKeyFromObject(cluster), owner, time.Now()); err != nil {
		t.Fatal(err)
	}
	return e
}

// create creates obj with the env's labels.
func (e *env) create(obj client.Object) {
	e.t.Helper()
	if len(e.labels) > 0 {
		labels := maps.Clone(obj.GetLabels())
		if labels == nil {
			labels = map[string]string{}
		}
		maps.Copy(labels, e.labels)
		obj.SetLabels(labels)
	}
	if err := testClient.Create(e.ctx, obj); err != nil {
		e.t.Fatal(err)
	}
}

// machine creates the Machine name, of the control plane or a worker, and its ChalkosConfig of
// the same name, which edit changes first. A non-empty address gives the Machine its providerID
// and that InternalIP.
func (e *env) machine(name string, controlPlane bool, address string, edit ...func(*bootstrapv1.ChalkosConfig)) {
	e.t.Helper()
	labels := map[string]string{clusterv1.ClusterNameLabel: clusterName}
	if controlPlane {
		labels[clusterv1.MachineControlPlaneLabel] = ""
	}
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: name, Labels: labels},
		Spec: clusterv1.MachineSpec{
			ClusterName: clusterName,
			Version:     "v1.37.1",
			Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: bootstrapv1.GroupVersion.Group, Kind: "ChalkosConfig", Name: name,
			}},
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "KubevirtMachine", Name: name},
		},
	}
	if address != "" {
		m.Spec.ProviderID = "kubevirt://" + name
	}
	e.create(m)
	if address != "" {
		m.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineInternalIP, Address: address}}
		if err := testClient.Status().Update(e.ctx, m); err != nil {
			e.t.Fatal(err)
		}
	}
	c := &bootstrapv1.ChalkosConfig{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: e.ns, Name: name, Labels: map[string]string{clusterv1.ClusterNameLabel: clusterName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: clusterv1.GroupVersion.String(), Kind: "Machine", Name: name, UID: m.UID, Controller: ptr.To(true),
			}},
		},
		Spec: bootstrapv1.ChalkosConfigSpec{Image: image},
	}
	for _, f := range edit {
		f(c)
	}
	e.create(c)
}

// reconcile reconciles the ChalkosConfig name once. A ChalkosConfig without the Paused condition
// is reconciled once more first: that reconcile sets the condition alone, and the update it
// writes triggers the next.
func (e *env) reconcile(name string) (ctrl.Result, error) {
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: e.ns, Name: name}}
	if !conditions.Has(e.config(name), bootstrapv1.PausedCondition) {
		if _, err := e.r.Reconcile(e.ctx, req); err != nil {
			return ctrl.Result{}, err
		}
	}
	return e.r.Reconcile(e.ctx, req)
}

// mustReconcile reconciles the ChalkosConfig name once, failing the test on an error.
func (e *env) mustReconcile(name string) ctrl.Result {
	e.t.Helper()
	res, err := e.reconcile(name)
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

// config returns the ChalkosConfig name.
func (e *env) config(name string) *bootstrapv1.ChalkosConfig {
	e.t.Helper()
	var c bootstrapv1.ChalkosConfig
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: name}, &c); err != nil {
		e.t.Fatal(err)
	}
	return &c
}

// installs are the install requests the machine at address received.
func (e *env) installs(address string) []sdk.InstallRequest {
	n, _ := e.fake.Node(address)
	return n.Installs
}

// wantInstalls fails the test unless the machine at address received n installs.
func (e *env) wantInstalls(address string, n int) {
	e.t.Helper()
	if got := len(e.installs(address)); got != n {
		e.t.Fatalf("%d installs, want %d", got, n)
	}
}

// wantReason fails the test unless the ChalkosConfig's Ready condition has the status and reason.
func wantReason(t *testing.T, c *bootstrapv1.ChalkosConfig, status metav1.ConditionStatus, reason string) {
	t.Helper()
	got := conditions.Get(c, bootstrapv1.ReadyCondition)
	if got == nil || got.Status != status || got.Reason != reason {
		t.Fatalf("Ready = %+v, want %s with reason %s", got, status, reason)
	}
}

// installAs makes an install of the machine at address install it as name, running the image of
// the root hash.
func (e *env) installAs(address, name, hash string) {
	e.fake.Update(address, func(n *nodetest.Node) {
		n.Install = func(sdk.InstallRequest) error {
			e.fake.Update(address, func(n *nodetest.Node) { n.Name, n.RootHash = name, hash })
			return nil
		}
	})
}

// A Machine without infrastructure gets its marker data Secret, which unblocks the infrastructure
// provider, also again once it was deleted, and waits.
func TestMarkerSecretFirst(t *testing.T) {
	e := newEnv(t, false, kubernetestest.Image)
	e.machine("cp-1", true, "")
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("cp-1")
	c := e.config("cp-1")
	if c.Status.DataSecretName != "cp-1" || !ptr.Deref(c.Status.Initialization.DataSecretCreated, false) {
		t.Errorf("status %+v, want the data Secret cp-1 created", c.Status)
	}
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.WaitingForInfrastructureReason)
	key := client.ObjectKey{Namespace: e.ns, Name: "cp-1"}
	var s corev1.Secret
	if err := testClient.Get(e.ctx, key, &s); err != nil {
		t.Fatal(err)
	}
	if s.Type != clusterv1.ClusterSecretType || s.Labels[clusterv1.ClusterNameLabel] != clusterName || string(s.Data["value"]) != dataSecretValue {
		t.Errorf("data Secret %+v", s)
	}
	if ref := metav1.GetControllerOf(&s); ref == nil || ref.Kind != "ChalkosConfig" || ref.Name != "cp-1" {
		t.Errorf("data Secret's controller %+v, want the ChalkosConfig", ref)
	}
	e.wantInstalls(addr, 0)

	if err := testClient.Delete(e.ctx, &s); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile("cp-1")
	if err := testClient.Get(e.ctx, key, &corev1.Secret{}); err != nil {
		t.Errorf("the deleted data Secret is not created again: %v", err)
	}
}

// A worker waits for the control plane to be initialized, and installs once it is.
func TestWorkerWaitsForControlPlane(t *testing.T) {
	e := newEnv(t, false, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	wantReason(t, e.config("w-1"), metav1.ConditionFalse, bootstrapv1.WaitingForControlPlaneReason)
	e.wantInstalls(addr, 0)

	var cluster clusterv1.Cluster
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	if err := testClient.Status().Update(e.ctx, &cluster); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
}

// A control plane installs before the control plane is initialized, with the documents of the
// cluster, the Cluster and the Machine, and its maintenance fingerprint and the image's root hash
// are recorded. While it reboots into the install, it is not installed again.
func TestInstallsAControlPlane(t *testing.T) {
	e := newEnv(t, false, kubernetestest.Image)
	e.machine("cp-1", true, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("cp-1")
	e.wantInstalls(addr, 1)
	r := e.installs(addr)[0]
	if r.Node != "cp-1" || r.Image != image {
		t.Errorf("install of %s with %s, want cp-1 with %s", r.Node, r.Image, image)
	}
	spec := func(kind string) map[string]any {
		d, ok := config.Documents(r.Documents).Single(kubeAPI, kind)
		if !ok {
			t.Fatalf("no %s among %v", kind, r.Documents)
		}
		spec, _ := d.Object["spec"].(map[string]any)
		return spec
	}
	if n := spec("Node"); n["type"] != "controlplane" || n["providerID"] != "kubevirt://cp-1" {
		t.Errorf("Node spec %v, want a control plane of kubevirt://cp-1", n)
	}
	c := spec("Cluster")
	pods, _ := c["podCIDRs"].(map[string]any)
	services, _ := c["serviceCIDRs"].(map[string]any)
	if c["domain"] != "lab.local" || c["endpoint"] != "https://10.96.100.1:6443" || pods["ipv4"] != "10.244.0.0/16" || services["ipv4"] != "10.96.0.0/12" {
		t.Errorf("Cluster spec %v, want the ChalkosControlPlane's domain with the Cluster's endpoint and ranges", c)
	}
	cfg := e.config("cp-1")
	if fps := cfg.Status.MaintenanceFingerprints; len(fps) != 1 || fps[0].SHA256 != fingerprint {
		t.Errorf("fingerprints %+v, want %s", fps, fingerprint)
	}
	if cfg.Status.ImageRootHash != rootHash || cfg.Status.LastInstallTime == nil {
		t.Errorf("status %+v, want the image's root hash and the install's time", cfg.Status)
	}

	if res := e.mustReconcile("cp-1"); res.RequeueAfter == 0 {
		t.Error("a machine rebooting into its install is not reconciled again")
	}
	e.wantInstalls(addr, 1)
	wantReason(t, e.config("cp-1"), metav1.ConditionFalse, bootstrapv1.InstallingReason)
}

// staleClient returns the ChalkosConfig as it was before, as a cache that has not seen the last
// reconcile's writes does.
type staleClient struct {
	client.Client
	cfg *bootstrapv1.ChalkosConfig
}

func (c staleClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if cfg, ok := obj.(*bootstrapv1.ChalkosConfig); ok {
		c.cfg.DeepCopyInto(cfg)
		return nil
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// A machine rebooting into its install is not installed again by a reconcile whose cache still
// holds the ChalkosConfig from before the install.
func TestStaleCacheDoesNotInstallAgain(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
	stale := e.config("w-1")
	stale.Status.LastInstallTime = nil
	e.r.Client = staleClient{Client: testClient, cfg: stale}
	_, err := e.reconcile("w-1")
	e.wantInstalls(addr, 1)
	if err != nil {
		t.Fatal(err)
	}
}

// A machine that presents another certificate in maintenance mode is pinned to it again, and the
// new fingerprint is recorded besides the old one.
func TestRepinsAndRecordsNewFingerprint(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	other := strings.Repeat("b", 64)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint, Install: func(sdk.InstallRequest) error {
		return errors.New("connection reset")
	}})
	for i := range 3 {
		if i == 1 {
			e.fake.Update(addr, func(n *nodetest.Node) { n.Fingerprint = other })
		}
		if _, err := e.reconcile("w-1"); err == nil {
			t.Error("a failed install reconciled without an error")
		}
	}
	fps := e.config("w-1").Status.MaintenanceFingerprints
	if len(fps) != 2 || fps[0].SHA256 != fingerprint || fps[1].SHA256 != other {
		t.Errorf("fingerprints %+v, want %s then %s", fps, fingerprint, other)
	}
	e.wantInstalls(addr, 3)
}

// A config the machine refuses is not installed again until the ChalkosConfig changes.
func TestRefusedConfigStops(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint, Install: func(sdk.InstallRequest) error {
		return &check.Refusal{Message: "the Node's labels are refused", Reason: check.RefusedReason}
	}})
	e.mustReconcile("w-1")
	c := e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.ConfigRefusedReason)
	if c.Status.RefusedInputs == "" {
		t.Error("the refusal records no inputs")
	}
	if res := e.mustReconcile("w-1"); !res.IsZero() {
		t.Errorf("reconcile of a refused config = %+v, want done", res)
	}
	e.wantInstalls(addr, 1)
	c.Spec.Files = []runtime.RawExtension{{Raw: []byte(`{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Node", "metadata": {"name": "default"}, "spec": {"labels": {"pool": "a"}}}`)}}
	if err := testClient.Update(e.ctx, c); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 2)
}

// A worker refused for the cluster's files is installed once the ChalkosControlPlane's files are
// fixed, without a change of its ChalkosConfig.
func TestRefusedClusterFilesRetryOnFix(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	var cp controlplanev1.ChalkosControlPlane
	key := client.ObjectKey{Namespace: e.ns, Name: clusterName}
	if err := testClient.Get(e.ctx, key, &cp); err != nil {
		t.Fatal(err)
	}
	cp.Spec.Files = []runtime.RawExtension{{Raw: []byte(strings.Replace(clusterFile, `"domain"`, `"endpoint": "https://elsewhere:6443", "domain"`, 1))}}
	if err := testClient.Update(e.ctx, &cp); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile("w-1")
	wantReason(t, e.config("w-1"), metav1.ConditionFalse, bootstrapv1.ConfigRefusedReason)
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 0)

	if err := testClient.Get(e.ctx, key, &cp); err != nil {
		t.Fatal(err)
	}
	cp.Spec.Files = []runtime.RawExtension{{Raw: []byte(clusterFile)}}
	if err := testClient.Update(e.ctx, &cp); err != nil {
		t.Fatal(err)
	}
	if reqs := e.r.controlPlaneToConfigs(e.ctx, &cp); len(reqs) != 1 || reqs[0].Name != "w-1" {
		t.Errorf("the ChalkosControlPlane maps to %v, want w-1", reqs)
	}
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
}

// A Cluster whose control plane is not a ChalkosControlPlane gives no cluster files, which is
// refused.
func TestNeedsAChalkosControlPlane(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	var cluster clusterv1.Cluster
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cluster); err != nil {
		t.Fatal(err)
	}
	cluster.Spec.ControlPlaneRef = clusterv1.ContractVersionedObjectReference{APIGroup: controlplanev1.GroupVersion.Group, Kind: "KubeadmControlPlane", Name: clusterName}
	if err := testClient.Update(e.ctx, &cluster); err != nil {
		t.Fatal(err)
	}
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	c := e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.ConfigRefusedReason)
	if msg := conditions.GetMessage(c, bootstrapv1.ReadyCondition); !strings.Contains(msg, "needs a ChalkosControlPlane") {
		t.Errorf("message %q", msg)
	}
	e.wantInstalls(addr, 0)
}

// An image whose kubelet is of another release than the Machine's version is refused without an
// install.
func TestImageOfAnotherVersion(t *testing.T) {
	e := newEnv(t, true, `{"version": "1.36.4", "cni": "flannel"}`)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	wantReason(t, e.config("w-1"), metav1.ConditionFalse, bootstrapv1.ImageRefusedReason)
	e.wantInstalls(addr, 0)
}

// A machine that answers as its node with its image after the install is installed and Ready, and
// is asked again later; while it is unreachable, it stays installed but not Ready.
func TestVerifiesInstalled(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.installAs(addr, "w-1", rootHash)
	if res := e.mustReconcile("w-1"); res.RequeueAfter < verifyEvery {
		t.Errorf("an installed machine is reconciled again after %v, want about %v", res.RequeueAfter, verifyEvery)
	}
	c := e.config("w-1")
	if !ptr.Deref(c.Status.Installed, false) {
		t.Error("not installed")
	}
	wantReason(t, c, metav1.ConditionTrue, bootstrapv1.InstalledReason)
	if !conditions.IsTrue(c, bootstrapv1.InstalledCondition) {
		t.Error("the Installed condition is not true")
	}
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)

	e.fake.Update(addr, func(n *nodetest.Node) { n.Err = errors.New("connection refused") })
	e.mustReconcile("w-1")
	c = e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.NodeUnreachableReason)
	if !conditions.IsTrue(c, bootstrapv1.InstalledCondition) {
		t.Error("an unreachable installed machine is not installed anymore")
	}
}

// An installed machine that answers in maintenance mode again is reported, not installed again.
func TestFallbackToMaintenance(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.installAs(addr, "w-1", rootHash)
	e.mustReconcile("w-1")
	e.fake.Update(addr, func(n *nodetest.Node) { n.Name = "" })
	for range 2 {
		e.mustReconcile("w-1")
	}
	c := e.config("w-1")
	if got := conditions.Get(c, bootstrapv1.InstalledCondition); got == nil || got.Reason != bootstrapv1.FellBackToMaintenanceReason {
		t.Errorf("Installed = %+v, want reason %s", got, bootstrapv1.FellBackToMaintenanceReason)
	}
	e.wantInstalls(addr, 1)
}

// A machine that answers as its node with another image than it installed fell back, and is not
// installed again.
func TestImageFellBack(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.installAs(addr, "w-1", strings.Repeat("d", 64))
	for range 2 {
		if res := e.mustReconcile("w-1"); !res.IsZero() {
			t.Errorf("reconcile of a machine that fell back = %+v, want done", res)
		}
	}
	c := e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.ImageFellBackReason)
	if conditions.GetReason(c, bootstrapv1.InstalledCondition) != bootstrapv1.ImageFellBackReason {
		t.Errorf("Installed = %+v", conditions.Get(c, bootstrapv1.InstalledCondition))
	}
	e.wantInstalls(addr, 1)
}

// A machine that answers as its node already, as after an install whose status was not written,
// is installed without another install.
func TestResumesAFinishedInstall(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Name: "w-1", RootHash: rootHash})
	e.mustReconcile("w-1")
	c := e.config("w-1")
	if !ptr.Deref(c.Status.Installed, false) {
		t.Error("not installed")
	}
	wantReason(t, c, metav1.ConditionTrue, bootstrapv1.InstalledReason)
	e.wantInstalls(addr, 0)
}

// A machine whose booted image refuses the controller's certificate is refused for good.
func TestBootedImageRefusesTheController(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint, Err: fmt.Errorf("%w: the image's OS CA", sdk.ErrOSCARefused)})
	e.mustReconcile("w-1")
	c := e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.ImageRefusedReason)
	if msg := conditions.GetMessage(c, bootstrapv1.ReadyCondition); !strings.Contains(msg, "booted image") {
		t.Errorf("message %q", msg)
	}
	e.fake.Update(addr, func(n *nodetest.Node) { n.Err = nil })
	if res := e.mustReconcile("w-1"); !res.IsZero() {
		t.Errorf("reconcile = %+v, want done", res)
	}
	e.wantInstalls(addr, 0)
}

// A machine that refuses an install because it reboots into one it accepted is waited for.
func TestRebootingMachineIsWaitedFor(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint, Install: func(sdk.InstallRequest) error {
		return fmt.Errorf("install: %w", nodetest.Reasoned(connect.CodeFailedPrecondition, nodev1.ErrorReason_ERROR_REASON_INSTALLED_AND_REBOOTS, "the node is installed and reboots"))
	}})
	if res := e.mustReconcile("w-1"); res.RequeueAfter == 0 {
		t.Error("a rebooting machine is not reconciled again")
	}
	c := e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.InstallingReason)
	if c.Status.LastInstallTime == nil {
		t.Error("no install time")
	}
	e.mustReconcile("w-1")
	e.wantInstalls(addr, 1)
}

// A refusal that says the machine reboots but gives no reason is an install failure: only the
// reason tells.
func TestRebootMessageWithoutReasonIsAFailure(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint, Install: func(sdk.InstallRequest) error {
		return connect.NewError(connect.CodeFailedPrecondition, "the node is installed and reboots")
	}})
	if _, err := e.reconcile("w-1"); err == nil {
		t.Error("a refusal without a reason does not fail the reconcile")
	}
	c := e.config("w-1")
	wantReason(t, c, metav1.ConditionFalse, bootstrapv1.InstallingReason)
	if msg := conditions.GetMessage(c, bootstrapv1.ReadyCondition); !strings.Contains(msg, "The install failed") {
		t.Errorf("message %q, want the install failure", msg)
	}
}

// An install waits for a free install slot.
func TestInstallSlots(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.r.MaxInstalls = 1
	const other = "10.0.0.2"
	e.machine("w-1", false, addr)
	e.machine("w-2", false, other)
	started, release := make(chan struct{}), make(chan struct{})
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint, Install: func(sdk.InstallRequest) error {
		close(started)
		<-release
		return errors.New("connection reset")
	}})
	e.fake.Set(other, nodetest.Node{Fingerprint: fingerprint})
	first := make(chan error, 1)
	go func() {
		_, err := e.reconcile("w-1")
		first <- err
	}()
	<-started
	res := e.mustReconcile("w-2")
	msg := conditions.GetMessage(e.config("w-2"), bootstrapv1.ReadyCondition)
	if res.RequeueAfter != slotWait || !strings.Contains(msg, "install slots") {
		t.Errorf("reconcile with every slot taken = %+v, %q; want a wait for a slot", res, msg)
	}
	e.wantInstalls(other, 0)
	close(release)
	if err := <-first; err == nil {
		t.Error("the failed install reconciled without an error")
	}
	e.mustReconcile("w-2")
	e.wantInstalls(other, 1)
}

// keychains is a Fake that records the keychain an image is read with.
type keychains struct {
	*nodetest.Fake
	mu  sync.Mutex
	got authn.Keychain
}

func (k *keychains) Image(ctx context.Context, ref string, kc authn.Keychain) (*ociimage.Artifact, error) {
	k.mu.Lock()
	k.got = kc
	k.mu.Unlock()
	return k.Fake.Image(ctx, ref, kc)
}

// auth is a docker config's entry for the user, whose password is the same.
func auth(user string) string {
	return `{"auth": "` + base64.StdEncoding.EncodeToString([]byte(user+":"+user)) + `"}`
}

// username is the user the keychain resolves the repository's credentials to; empty for
// anonymous ones.
func username(t *testing.T, k authn.Keychain, repository string) string {
	t.Helper()
	repo, err := name.NewRepository(repository)
	if err != nil {
		t.Fatal(err)
	}
	a, err := k.Resolve(repo)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := authn.Authorization(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Username
}

// The image is read and installed with the credentials of the ChalkosConfig's image pull Secret.
func TestImagePullSecretReachesTheKeychain(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	k := &keychains{Fake: e.fake}
	e.r.Node = k
	e.create(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "pull"},
		Type:       corev1.SecretTypeDockerConfigJson,
		Data:       map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths": {"registry.example": ` + auth("puller") + `}}`)},
	})
	e.machine("w-1", false, addr, func(c *bootstrapv1.ChalkosConfig) {
		c.Spec.ImagePullSecretRef = &corev1.LocalObjectReference{Name: "pull"}
	})
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.mustReconcile("w-1")
	if k.got == nil || username(t, k.got, "registry.example/chalkos") != "puller" {
		t.Error("the image is not read with the pull Secret's credentials")
	}
	if r := e.installs(addr); len(r) != 1 || r[0].Keychain == nil || username(t, r[0].Keychain, "registry.example/chalkos") != "puller" {
		t.Error("the install does not read the image with the pull Secret's credentials")
	}
}

// Machines map to their ChalkosConfigs, and those of another bootstrap provider to none.
func TestMappings(t *testing.T) {
	e := newEnv(t, true, kubernetestest.Image)
	e.machine("w-1", false, addr)
	kubeadm := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "k-1", Labels: map[string]string{clusterv1.ClusterNameLabel: clusterName}},
		Spec: clusterv1.MachineSpec{
			ClusterName: clusterName,
			Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: "bootstrap.cluster.x-k8s.io", Kind: "KubeadmConfig", Name: "k-1",
			}},
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "KubevirtMachine", Name: "k-1"},
		},
	}
	e.create(kubeadm)
	if reqs := machineToConfig(e.ctx, kubeadm); len(reqs) != 0 {
		t.Errorf("a KubeadmConfig's Machine maps to %v", reqs)
	}
	var cluster clusterv1.Cluster
	if err := testClient.Get(e.ctx, client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cluster); err != nil {
		t.Fatal(err)
	}
	if reqs := e.r.clusterToConfigs(e.ctx, &cluster); len(reqs) != 1 || reqs[0].Name != "w-1" {
		t.Errorf("the Cluster maps to %v, want w-1 alone", reqs)
	}
	orphan := &controlplanev1.ChalkosControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: "orphan"}}
	if reqs := e.r.controlPlaneToConfigs(e.ctx, orphan); len(reqs) != 0 {
		t.Errorf("a ChalkosControlPlane without a Cluster maps to %v", reqs)
	}
}

// Through the manager, a paused ChalkosConfig reports the pause and waits, and installs once it
// is resumed; the manager reconciles only objects with its watch filter's label.
func TestPausedThroughTheManager(t *testing.T) {
	const filter = "manager-test"
	e := newLabelledEnv(t, true, kubernetestest.Image, map[string]string{clusterv1.WatchLabel: filter})
	e.machine("w-1", false, addr, func(c *bootstrapv1.ChalkosConfig) {
		c.Annotations = map[string]string{clusterv1.PausedAnnotation: ""}
	})
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.installAs(addr, "w-1", rootHash)

	mgr, err := ctrl.NewManager(testConfig, ctrl.Options{
		Scheme:     testScheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &ChalkosConfigReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Node: e.fake, WatchFilterValue: filter}
	if err := r.SetupWithManager(e.ctx, mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(e.ctx)
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	defer func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	}()

	eventually(t, "the Paused condition", func() bool { return conditions.IsTrue(e.config("w-1"), bootstrapv1.PausedCondition) })
	e.wantInstalls(addr, 0)
	c := e.config("w-1")
	c.Annotations = nil
	if err := testClient.Update(e.ctx, c); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the install", func() bool { return ptr.Deref(e.config("w-1").Status.Installed, false) })
	e.wantInstalls(addr, 1)
}

// eventually polls done until it holds, failing the test after ten seconds.
func eventually(t *testing.T, what string, done func() bool) {
	t.Helper()
	err := wait.PollUntilContextTimeout(t.Context(), 20*time.Millisecond, 10*time.Second, true, func(context.Context) (bool, error) {
		return done(), nil
	})
	if err != nil {
		t.Fatalf("waiting for %s: %v", what, err)
	}
}

// A Machine that is deleted before its machine was installed is not installed.
func TestDeletingMachineNotInstalled(t *testing.T) {
	e := newEnv(t, false, kubernetestest.Image)
	e.machine("cp-1", true, addr)
	e.fake.Set(addr, nodetest.Node{Fingerprint: fingerprint})
	e.deleteMachineHeld("cp-1")
	e.mustReconcile("cp-1")
	e.wantInstalls(addr, 0)
	wantReason(t, e.config("cp-1"), metav1.ConditionFalse, bootstrapv1.MachineDeletingReason)
}

// The Machine watch passes the changes a reconcile reads and drops the others, such as the
// Machine's conditions.
func TestMachineChanged(t *testing.T) {
	base := &clusterv1.Machine{Spec: clusterv1.MachineSpec{Version: "v1.37.1"}}
	now := metav1.Now()
	cases := map[string]struct {
		edit func(*clusterv1.Machine)
		want bool
	}{
		"conditions": {func(m *clusterv1.Machine) {
			m.Status.Conditions = []metav1.Condition{{Type: clusterv1.ReadyCondition, Status: metav1.ConditionTrue}}
		}, false},
		"providerID": {func(m *clusterv1.Machine) { m.Spec.ProviderID = "kubevirt://m" }, true},
		"addresses": {func(m *clusterv1.Machine) {
			m.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineInternalIP, Address: addr}}
		}, true},
		"version":        {func(m *clusterv1.Machine) { m.Spec.Version = "v1.37.2" }, true},
		"labels":         {func(m *clusterv1.Machine) { m.Labels = map[string]string{clusterv1.MachineControlPlaneLabel: ""} }, true},
		"dataSecretName": {func(m *clusterv1.Machine) { m.Spec.Bootstrap.DataSecretName = ptr.To("m") }, true},
		"infrastructure": {func(m *clusterv1.Machine) { m.Status.Initialization.InfrastructureProvisioned = ptr.To(true) }, true},
		"deletion":       {func(m *clusterv1.Machine) { m.DeletionTimestamp = &now }, true},
		"hook": {func(m *clusterv1.Machine) {
			m.Annotations = map[string]string{machineutil.HookAnnotation: machineutil.BootstrapHookOwner}
		}, true},
		"other annotation": {func(m *clusterv1.Machine) { m.Annotations = map[string]string{"example.com/a": ""} }, false},
		"deleting reason": {func(m *clusterv1.Machine) {
			m.Status.Conditions = []metav1.Condition{{Type: clusterv1.MachineDeletingCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineDeletingWaitingForPreTerminateHookReason}}
		}, true},
	}
	for name, c := range cases {
		cur := base.DeepCopy()
		c.edit(cur)
		if got := machineChanged().Update(event.UpdateEvent{ObjectOld: base, ObjectNew: cur}); got != c.want {
			t.Errorf("%s: the update passes %v, want %v", name, got, c.want)
		}
	}
}

// The Cluster watch passes the update that turns status.initialization.controlPlaneInitialized
// true, whatever the Cluster's conditions say.
func TestControlPlaneInitialized(t *testing.T) {
	before := &clusterv1.Cluster{Status: clusterv1.ClusterStatus{Conditions: []metav1.Condition{
		{Type: clusterv1.ClusterControlPlaneInitializedCondition, Status: metav1.ConditionTrue},
	}}}
	after := before.DeepCopy()
	after.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	p := controlPlaneInitialized()
	if !p.Update(event.UpdateEvent{ObjectOld: before, ObjectNew: after}) {
		t.Error("the update that initializes the control plane is dropped")
	}
	if p.Update(event.UpdateEvent{ObjectOld: after, ObjectNew: after.DeepCopy()}) {
		t.Error("an update of an initialized Cluster passes")
	}
}

// The Cluster watch passes the updates that change the Cluster fields a refusal's inputs
// include, and drops the others.
func TestClusterInputsChanged(t *testing.T) {
	base := &clusterv1.Cluster{Spec: clusterv1.ClusterSpec{
		ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "10.0.0.1", Port: 6443},
		ClusterNetwork: clusterv1.ClusterNetwork{
			Pods: clusterv1.NetworkRanges{CIDRBlocks: []string{"10.244.0.0/16"}},
		},
	}}
	cases := map[string]struct {
		edit func(*clusterv1.Cluster)
		want bool
	}{
		"conditions": {func(c *clusterv1.Cluster) {
			c.Status.Conditions = []metav1.Condition{{Type: clusterv1.ReadyCondition, Status: metav1.ConditionTrue}}
		}, false},
		"labels":               {func(c *clusterv1.Cluster) { c.Labels = map[string]string{"a": "b"} }, false},
		"controlPlaneEndpoint": {func(c *clusterv1.Cluster) { c.Spec.ControlPlaneEndpoint.Port = 443 }, true},
		"clusterNetwork": {func(c *clusterv1.Cluster) {
			c.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"10.96.0.0/12"}
		}, true},
		"controlPlaneRef": {func(c *clusterv1.Cluster) {
			c.Spec.ControlPlaneRef = clusterv1.ContractVersionedObjectReference{
				APIGroup: controlplanev1.GroupVersion.Group, Kind: "ChalkosControlPlane", Name: "cp",
			}
		}, true},
	}
	for name, c := range cases {
		cur := base.DeepCopy()
		c.edit(cur)
		if got := clusterInputsChanged().Update(event.UpdateEvent{ObjectOld: base, ObjectNew: cur}); got != c.want {
			t.Errorf("%s: the update passes %v, want %v", name, got, c.want)
		}
	}
}
