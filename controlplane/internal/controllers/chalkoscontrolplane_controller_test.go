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
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/clientcmd"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/certs"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/test/builder"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

const (
	clusterName  = "c1"
	cpName       = "c1-cp"
	templateName = "c1-cp-infra"
	image        = "registry.example/chalkos@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	version      = "v1.37.1"
)

// env is one test's namespace with a Cluster, its ChalkosControlPlane and infrastructure
// template, the fake machines, and the reconciler.
type env struct {
	t     *testing.T
	ns    string
	fake  *nodetest.Fake
	clock *clocktesting.FakeClock
	r     *ChalkosControlPlaneReconciler
}

// newEnv returns an env in a new namespace whose ChalkosControlPlane asks for replicas, and whose
// Cluster has a controlPlaneEndpoint or not.
func newEnv(t *testing.T, replicas int32, endpoint bool) *env {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "controlplane-"}}
	if err := testClient.Create(t.Context(), ns); err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, ns: ns.Name, fake: nodetest.New(), clock: clocktesting.NewFakeClock(time.Now())}
	e.r = e.reconciler()

	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: e.ns, Name: clusterName},
		Spec: clusterv1.ClusterSpec{
			ControlPlaneRef: clusterv1.ContractVersionedObjectReference{APIGroup: controlplanev1.GroupVersion.Group, Kind: "ChalkosControlPlane", Name: cpName},
		},
	}
	if endpoint {
		cluster.Spec.ControlPlaneEndpoint = clusterv1.APIEndpoint{Host: "10.96.100.1", Port: 6443}
	}
	e.create(cluster)
	template := &unstructured.Unstructured{}
	template.SetGroupVersionKind(builder.InfrastructureGroupVersion.WithKind(builder.GenericInfrastructureMachineTemplateKind))
	template.SetNamespace(e.ns)
	template.SetName(templateName)
	if err := unstructured.SetNestedField(template.Object, "large", "spec", "template", "spec", "size"); err != nil {
		t.Fatal(err)
	}
	e.create(template)
	cp := &controlplanev1.ChalkosControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: e.ns, Name: cpName,
			Labels:          map[string]string{clusterv1.ClusterNameLabel: clusterName},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: cluster.Name, UID: cluster.UID}},
		},
		Spec: controlplanev1.ChalkosControlPlaneSpec{
			Replicas:           ptr.To(replicas),
			Version:            version,
			Image:              image,
			ImagePullSecretRef: &corev1.LocalObjectReference{Name: "pull"},
			MachineTemplate: controlplanev1.ChalkosControlPlaneMachineTemplate{
				ObjectMeta: clusterv1.ObjectMeta{Labels: map[string]string{"pool": "cp"}},
				Spec: controlplanev1.ChalkosControlPlaneMachineTemplateSpec{
					InfrastructureRef: clusterv1.ContractVersionedObjectReference{
						APIGroup: builder.InfrastructureGroupVersion.Group, Kind: builder.GenericInfrastructureMachineTemplateKind, Name: templateName,
					},
					Deletion: controlplanev1.ChalkosControlPlaneMachineTemplateDeletionSpec{NodeDrainTimeoutSeconds: ptr.To[int32](60)},
				},
			},
		},
	}
	e.create(cp)
	return e
}

// reconciler returns a new reconciler of the env's fake machines and clock, as after a restart of
// the controller.
func (e *env) reconciler() *ChalkosControlPlaneReconciler {
	return &ChalkosControlPlaneReconciler{Client: testClient, APIReader: testClient, Node: e.fake, Clock: e.clock}
}

// create creates obj.
func (e *env) create(obj client.Object) {
	e.t.Helper()
	if err := testClient.Create(e.t.Context(), obj); err != nil {
		e.t.Fatal(err)
	}
}

// reconcile reconciles the ChalkosControlPlane once. One without the finalizer or the Paused
// condition is reconciled first until it has both: the reconciles that set them do nothing else.
func (e *env) reconcile() (ctrl.Result, error) {
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: e.ns, Name: cpName}}
	for range 2 {
		var cp controlplanev1.ChalkosControlPlane
		if err := testClient.Get(e.t.Context(), req.NamespacedName, &cp); err != nil ||
			(controllerutil.ContainsFinalizer(&cp, controlplanev1.ChalkosControlPlaneFinalizer) && conditions.Has(&cp, controlplanev1.PausedCondition)) {
			break
		}
		if _, err := e.r.Reconcile(e.t.Context(), req); err != nil {
			return ctrl.Result{}, err
		}
	}
	return e.r.Reconcile(e.t.Context(), req)
}

// mustReconcile reconciles the ChalkosControlPlane once, failing the test on an error.
func (e *env) mustReconcile() ctrl.Result {
	e.t.Helper()
	res, err := e.reconcile()
	if err != nil {
		e.t.Fatal(err)
	}
	return res
}

// cp returns the ChalkosControlPlane.
func (e *env) cp() *controlplanev1.ChalkosControlPlane {
	e.t.Helper()
	var cp controlplanev1.ChalkosControlPlane
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: cpName}, &cp); err != nil {
		e.t.Fatal(err)
	}
	return &cp
}

// setEndpoint gives the Cluster its controlPlaneEndpoint.
func (e *env) setEndpoint() {
	e.t.Helper()
	var cluster clusterv1.Cluster
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: clusterName}, &cluster); err != nil {
		e.t.Fatal(err)
	}
	cluster.Spec.ControlPlaneEndpoint = clusterv1.APIEndpoint{Host: "10.96.100.1", Port: 6443}
	if err := testClient.Update(e.t.Context(), &cluster); err != nil {
		e.t.Fatal(err)
	}
}

// machines are the control plane's Machines.
func (e *env) machines() []clusterv1.Machine {
	e.t.Helper()
	var list clusterv1.MachineList
	if err := testClient.List(e.t.Context(), &list, client.InNamespace(e.ns), client.MatchingLabels{clusterv1.MachineControlPlaneNameLabel: cpName}); err != nil {
		e.t.Fatal(err)
	}
	return list.Items
}

// wantMachines fails the test unless the control plane has n Machines, and returns them.
func (e *env) wantMachines(n int) []clusterv1.Machine {
	e.t.Helper()
	ms := e.machines()
	if len(ms) != n {
		e.t.Fatalf("%d Machines, want %d", len(ms), n)
	}
	return ms
}

// provision gives the Machine, as it is now, the InternalIP addr, marks its ChalkosConfig Ready
// and serves the machine at addr installed as its node, in the phase.
func (e *env) provision(m clusterv1.Machine, addr string, phase kubernetesv1.Phase) {
	e.t.Helper()
	if err := testClient.Get(e.t.Context(), client.ObjectKeyFromObject(&m), &m); err != nil {
		e.t.Fatal(err)
	}
	m.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineInternalIP, Address: addr}}
	if err := testClient.Status().Update(e.t.Context(), &m); err != nil {
		e.t.Fatal(err)
	}
	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: m.Spec.Bootstrap.ConfigRef.Name}, &cfg); err != nil {
		e.t.Fatal(err)
	}
	conditions.Set(&cfg, metav1.Condition{Type: bootstrapv1.ReadyCondition, Status: metav1.ConditionTrue, Reason: bootstrapv1.InstalledReason})
	if err := testClient.Status().Update(e.t.Context(), &cfg); err != nil {
		e.t.Fatal(err)
	}
	e.fake.Set(addr, nodetest.Node{Name: m.Name, Status: &kubernetesv1.StatusResponse{Phase: phase}})
}

// setStatus sets the Kubernetes status of the machine at addr.
func (e *env) setStatus(addr string, phase kubernetesv1.Phase, addonsApplied bool) {
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Status = &kubernetesv1.StatusResponse{Phase: phase, AddonsApplied: addonsApplied}
	})
}

// bootstrapsTo makes a bootstrap of the machine at addr bootstrap it and apply the add-ons.
func (e *env) bootstrapsTo(addr string) {
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, true)
			return nil
		}
	})
}

// bootstraps counts the bootstraps of the machine at addr.
func (e *env) bootstraps(addr string) int {
	n, _ := e.fake.Node(addr)
	return n.Bootstraps
}

// initialize creates the first Machine, provisions it at addr and bootstraps it.
func (e *env) initialize(addr string) clusterv1.Machine {
	e.t.Helper()
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	e.provision(m, addr, kubernetesv1.Phase_PHASE_WAITING)
	e.bootstrapsTo(addr)
	e.mustReconcile()
	if !ptr.Deref(e.cp().Status.Initialization.ControlPlaneInitialized, false) {
		e.t.Fatal("the control plane is not initialized")
	}
	return m
}

// wantCondition fails the test unless the ChalkosControlPlane's condition has the status.
func wantCondition(t *testing.T, cp *controlplanev1.ChalkosControlPlane, typ string, status metav1.ConditionStatus) {
	t.Helper()
	if got := conditions.Get(cp, typ); got == nil || got.Status != status {
		t.Fatalf("%s = %+v, want %s", typ, got, status)
	}
}

// A Cluster without a controlPlaneEndpoint gets no secrets and no Machines yet; once it has one,
// it gets both.
func TestWaitsForEndpoint(t *testing.T) {
	e := newEnv(t, 1, false)
	e.mustReconcile()
	e.wantMachines(0)
	if _, ok, err := secrets.Get(t.Context(), testClient, client.ObjectKey{Namespace: e.ns, Name: clusterName}); err != nil || ok {
		t.Fatalf("secrets before the endpoint: ok %v, err %v", ok, err)
	}
	e.setEndpoint()
	e.mustReconcile()
	e.wantMachines(1)
	if _, ok, err := secrets.Get(t.Context(), testClient, client.ObjectKey{Namespace: e.ns, Name: clusterName}); err != nil || !ok {
		t.Fatalf("secrets after the endpoint: ok %v, err %v", ok, err)
	}
}

// The cluster's secrets are generated once, owned by the ChalkosControlPlane and labelled for
// clusterctl move.
func TestGeneratesSecretsOnce(t *testing.T) {
	e := newEnv(t, 1, true)
	e.mustReconcile()
	key := client.ObjectKey{Namespace: e.ns, Name: secrets.Name(clusterName)}
	var first corev1.Secret
	if err := testClient.Get(t.Context(), key, &first); err != nil {
		t.Fatal(err)
	}
	if ref := first.OwnerReferences; len(ref) != 1 || ref[0].Kind != "ChalkosControlPlane" || ref[0].Name != cpName {
		t.Errorf("owners %+v, want the ChalkosControlPlane", ref)
	}
	if _, ok := first.Labels["clusterctl.cluster.x-k8s.io/move"]; !ok || first.Labels[clusterv1.ClusterNameLabel] != clusterName {
		t.Errorf("labels %v, want the cluster's name and clusterctl's move label", first.Labels)
	}
	e.mustReconcile()
	e.r = e.reconciler()
	e.mustReconcile()
	var second corev1.Secret
	if err := testClient.Get(t.Context(), key, &second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Data[secrets.Key], second.Data[secrets.Key]) {
		t.Error("the secrets were generated again")
	}
}

// Before the control plane is initialized, only its first Machine is created, with its
// ChalkosConfig and infrastructure clone.
func TestCreatesFirstMachineOnly(t *testing.T) {
	e := newEnv(t, 3, true)
	e.mustReconcile()
	e.mustReconcile()
	m := e.wantMachines(1)[0]

	if !strings.HasPrefix(m.Name, cpName+"-") {
		t.Errorf("Machine name %s, want the prefix %s-", m.Name, cpName)
	}
	for k, v := range map[string]string{
		clusterv1.ClusterNameLabel:             clusterName,
		clusterv1.MachineControlPlaneLabel:     "",
		clusterv1.MachineControlPlaneNameLabel: cpName,
		"pool":                                 "cp",
	} {
		if got, ok := m.Labels[k]; !ok || got != v {
			t.Errorf("label %s = %q, want %q", k, got, v)
		}
	}
	if ref := metav1.GetControllerOf(&m); ref == nil || ref.Kind != "ChalkosControlPlane" || ref.Name != cpName {
		t.Errorf("controller %+v, want the ChalkosControlPlane", ref)
	}
	if m.Spec.ClusterName != clusterName || m.Spec.Version != version || ptr.Deref(m.Spec.Deletion.NodeDrainTimeoutSeconds, 0) != 60 {
		t.Errorf("spec %+v", m.Spec)
	}
	if ref := m.Spec.Bootstrap.ConfigRef; ref.APIGroup != bootstrapv1.GroupVersion.Group || ref.Kind != "ChalkosConfig" || ref.Name != m.Name {
		t.Errorf("configRef %+v", ref)
	}
	if ref := m.Spec.InfrastructureRef; ref.APIGroup != builder.InfrastructureGroupVersion.Group || ref.Kind != builder.GenericInfrastructureMachineKind || ref.Name != m.Name {
		t.Errorf("infrastructureRef %+v", ref)
	}

	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: m.Name}, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Spec.Image != image || cfg.Spec.ImagePullSecretRef == nil || cfg.Spec.ImagePullSecretRef.Name != "pull" || len(cfg.Spec.Files) != 0 {
		t.Errorf("ChalkosConfig spec %+v", cfg.Spec)
	}
	if !slices.ContainsFunc(cfg.OwnerReferences, func(r metav1.OwnerReference) bool { return r.Kind == "ChalkosControlPlane" && r.Name == cpName }) {
		t.Errorf("ChalkosConfig owners %+v, want the ChalkosControlPlane", cfg.OwnerReferences)
	}

	infra := &unstructured.Unstructured{}
	infra.SetGroupVersionKind(builder.InfrastructureGroupVersion.WithKind(builder.GenericInfrastructureMachineKind))
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: m.Name}, infra); err != nil {
		t.Fatal(err)
	}
	if size, _, _ := unstructured.NestedString(infra.Object, "spec", "size"); size != "large" {
		t.Errorf("infrastructure spec %v, want the template's", infra.Object["spec"])
	}
	if refs := infra.GetOwnerReferences(); len(refs) != 1 || refs[0].Kind != "ChalkosControlPlane" {
		t.Errorf("infrastructure owners %+v, want the ChalkosControlPlane", refs)
	}

	cp := e.cp()
	if ptr.Deref(cp.Status.Replicas, 0) != 1 || cp.Status.Selector == "" || cp.Status.Version != version { //nolint:staticcheck // The contract still reads it.
		t.Errorf("status %+v", cp.Status)
	}
	if v := cp.Status.Versions; len(v) != 1 || v[0].Version != version || v[0].Replicas != 1 {
		t.Errorf("versions %+v", v)
	}
	wantCondition(t, cp, controlplanev1.ScalingUpCondition, metav1.ConditionTrue)
	wantCondition(t, cp, controlplanev1.InitializedCondition, metav1.ConditionFalse)
}

// Bootstrap is called once on the first Machine: the record of the call keeps another reconcile,
// also of a restarted controller, from calling it again.
func TestBootstrapsOnceAcrossRestart(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	e.mustReconcile()
	if e.cp().Status.BootstrappedNode != "" {
		t.Fatal("bootstrapped before the Machine's config is ready")
	}
	e.provision(m, addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
			return nil
		}
	})
	e.mustReconcile()
	if got := e.cp().Status.BootstrappedNode; got != m.Name {
		t.Errorf("bootstrappedNode %q, want %s", got, m.Name)
	}
	e.mustReconcile()
	e.r = e.reconciler()
	e.mustReconcile()
	if n := e.bootstraps(addr); n != 1 {
		t.Errorf("%d bootstraps, want 1", n)
	}
}

// A refusal of Bootstrap because the node is bootstrapped already counts as the bootstrap.
func TestBootstrappedAlreadyIsDone(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	e.provision(e.wantMachines(1)[0], addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
			return connect.NewError(connect.CodeFailedPrecondition, "the node is bootstrapped already")
		}
	})
	if _, err := e.reconcile(); err != nil {
		t.Errorf("a refusal of a bootstrapped node fails the reconcile: %v", err)
	}
	e.mustReconcile()
	if n := e.bootstraps(addr); n != 1 {
		t.Errorf("%d bootstraps, want 1", n)
	}
}

// A Bootstrap that failed before the node started bootstrapping is called again on the same
// node, which still waits for it.
func TestBootstrapRetriedWhileWaiting(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	e.provision(m, addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error { return errors.New("connection reset") }
	})
	if _, err := e.reconcile(); err == nil {
		t.Error("a failed bootstrap reconciled without an error")
	}
	e.bootstrapsTo(addr)
	e.mustReconcile()
	e.mustReconcile()
	if n := e.bootstraps(addr); n != 2 {
		t.Errorf("%d bootstraps, want 2", n)
	}
	cp := e.cp()
	if cp.Status.BootstrappedNode != m.Name || !ptr.Deref(cp.Status.Initialization.ControlPlaneInitialized, false) {
		t.Errorf("status %+v, want %s bootstrapped and initialized", cp.Status, m.Name)
	}
}

// The control plane is initialized once the bootstrapped node applied the add-ons, and stays so
// when the node reports them unapplied again, as after a restart of chalkd.
func TestInitializedOnAddonsApplied(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	e.provision(m, addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
			return nil
		}
	})
	e.mustReconcile()
	cp := e.cp()
	if ptr.Deref(cp.Status.Initialization.ControlPlaneInitialized, false) {
		t.Fatal("initialized before the add-ons were applied")
	}
	wantCondition(t, cp, controlplanev1.InitializedCondition, metav1.ConditionFalse)

	e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, true)
	e.mustReconcile()
	cp = e.cp()
	if !ptr.Deref(cp.Status.Initialization.ControlPlaneInitialized, false) {
		t.Fatal("not initialized once the add-ons were applied")
	}
	wantCondition(t, cp, controlplanev1.InitializedCondition, metav1.ConditionTrue)
	wantCondition(t, cp, controlplanev1.AvailableCondition, metav1.ConditionFalse)

	e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
	m = e.wantMachines(1)[0]
	conditions.Set(&m, metav1.Condition{Type: clusterv1.MachineReadyCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineReadyReason})
	if err := testClient.Status().Update(t.Context(), &m); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	cp = e.cp()
	if !ptr.Deref(cp.Status.Initialization.ControlPlaneInitialized, false) {
		t.Fatal("no longer initialized once the node reports its add-ons unapplied")
	}
	wantCondition(t, cp, controlplanev1.InitializedCondition, metav1.ConditionTrue)
	wantCondition(t, cp, controlplanev1.AvailableCondition, metav1.ConditionTrue)
	if ptr.Deref(cp.Status.ReadyReplicas, 0) != 1 {
		t.Errorf("readyReplicas %v, want 1", cp.Status.ReadyReplicas)
	}
}

// clientCert returns the client certificate of the kubeconfig Secret's kubeconfig.
func clientCert(t *testing.T, s *corev1.Secret) *x509.Certificate {
	t.Helper()
	cfg, err := clientcmd.Load(s.Data["value"])
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.AuthInfos) != 1 {
		t.Fatalf("%d users, want 1", len(cfg.AuthInfos))
	}
	for _, user := range cfg.AuthInfos {
		cert, err := certs.DecodeCertPEM(user.ClientCertificateData)
		if err != nil {
			t.Fatal(err)
		}
		return cert
	}
	return nil
}

// Once initialized, the cluster gets its kubeconfig Secret, whose certificate is issued again
// once two thirds of its TTL passed.
func TestKubeconfigIssuedAndRenewed(t *testing.T) {
	e := newEnv(t, 1, true)
	key := client.ObjectKey{Namespace: e.ns, Name: clusterName + "-kubeconfig"}
	e.mustReconcile()
	if err := testClient.Get(t.Context(), key, &corev1.Secret{}); err == nil {
		t.Fatal("a kubeconfig Secret before the control plane is initialized")
	}
	e.initialize("10.0.0.1")

	var s corev1.Secret
	if err := testClient.Get(t.Context(), key, &s); err != nil {
		t.Fatal(err)
	}
	if s.Type != clusterv1.ClusterSecretType || s.Labels[clusterv1.ClusterNameLabel] != clusterName {
		t.Errorf("Secret type %s, labels %v", s.Type, s.Labels)
	}
	if ref := metav1.GetControllerOf(&s); ref == nil || ref.Kind != "ChalkosControlPlane" || ref.Name != cpName {
		t.Errorf("controller %+v, want the ChalkosControlPlane", ref)
	}
	cfg, err := clientcmd.Load(s.Data["value"])
	if err != nil {
		t.Fatal(err)
	}
	if c := cfg.Clusters[clusterName]; c == nil || c.Server != "https://10.96.100.1:6443" {
		t.Errorf("clusters %v, want %s at the endpoint", cfg.Clusters, clusterName)
	}
	cert := clientCert(t, &s)
	ttl := 24 * time.Hour
	if cert.Subject.CommonName != "cluster-api" || cert.NotAfter.Sub(e.clock.Now()).Round(time.Minute) != ttl {
		t.Errorf("certificate of %s until %s, want cluster-api for %s", cert.Subject.CommonName, cert.NotAfter, ttl)
	}
	cp := e.cp()
	if at := cp.Status.KubeconfigExpiresAt; at == nil || !at.Time.Equal(cert.NotAfter) {
		t.Errorf("kubeconfigExpiresAt %v, want %s", at, cert.NotAfter)
	}

	res := e.mustReconcile()
	if res.RequeueAfter <= 0 || res.RequeueAfter > ttl*2/3 {
		t.Errorf("requeue after %s, want by the renewal at two thirds of %s", res.RequeueAfter, ttl)
	}
	if err := testClient.Get(t.Context(), key, &s); err != nil {
		t.Fatal(err)
	}
	if !clientCert(t, &s).Equal(cert) {
		t.Error("the certificate was issued again before two thirds of its TTL passed")
	}

	e.clock.Step(ttl*2/3 + time.Minute)
	e.mustReconcile()
	if err := testClient.Get(t.Context(), key, &s); err != nil {
		t.Fatal(err)
	}
	renewed := clientCert(t, &s)
	if !renewed.NotAfter.After(cert.NotAfter) {
		t.Errorf("certificate until %s after the renewal, want after %s", renewed.NotAfter, cert.NotAfter)
	}
	if at := e.cp().Status.KubeconfigExpiresAt; at == nil || !at.Time.Equal(renewed.NotAfter) {
		t.Errorf("kubeconfigExpiresAt %v, want %s", at, renewed.NotAfter)
	}
}

// Once initialized, the control plane gets its next Machine only when every Machine it has
// joined the cluster.
func TestJoinsOneAtATime(t *testing.T) {
	e := newEnv(t, 3, true)
	first := e.initialize("10.0.0.1")
	ms := e.wantMachines(2)
	second := ms[0]
	if second.Name == first.Name {
		second = ms[1]
	}
	e.mustReconcile()
	e.wantMachines(2)

	e.provision(second, "10.0.0.2", kubernetesv1.Phase_PHASE_JOINING)
	e.mustReconcile()
	e.wantMachines(2)
	waiting := "Scaling up from 2 to 3 replicas; waiting for the Machine " + second.Name + " to join the cluster"
	if got := conditions.Get(e.cp(), controlplanev1.ScalingUpCondition); got == nil || got.Message != waiting {
		t.Errorf("ScalingUp = %+v, want the message %q", got, waiting)
	}

	e.setStatus("10.0.0.2", kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
	e.mustReconcile()
	e.wantMachines(3)
	e.mustReconcile()
	e.wantMachines(3)
	cp := e.cp()
	wantCondition(t, cp, controlplanev1.ScalingUpCondition, metav1.ConditionFalse)
	if n := e.bootstraps("10.0.0.2"); n != 0 {
		t.Errorf("%d bootstraps of a joining Machine, want 0", n)
	}
}

// ChalkosConfigs map to their ChalkosControlPlane through their owners, first the
// ChalkosControlPlane, then their Machine; Clusters map by their controlPlaneRef, and those of
// another control plane to none.
func TestMappings(t *testing.T) {
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: m.Name}, &cfg); err != nil {
		t.Fatal(err)
	}
	if reqs := e.r.configToControlPlane(t.Context(), &cfg); len(reqs) != 1 || reqs[0].Name != cpName || reqs[0].Namespace != e.ns {
		t.Errorf("a ChalkosConfig the control plane owns maps to %v", reqs)
	}
	cfg.OwnerReferences = []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Machine", Name: m.Name, UID: m.UID, Controller: ptr.To(true)}}
	if reqs := e.r.configToControlPlane(t.Context(), &cfg); len(reqs) != 1 || reqs[0].Name != cpName {
		t.Errorf("a ChalkosConfig its Machine took over maps to %v", reqs)
	}
	if reqs := e.r.configToControlPlane(t.Context(), &bootstrapv1.ChalkosConfig{}); len(reqs) != 0 {
		t.Errorf("a ChalkosConfig without owners maps to %v", reqs)
	}
	cluster := &clusterv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: clusterName},
		Spec: clusterv1.ClusterSpec{ControlPlaneRef: clusterv1.ContractVersionedObjectReference{
			APIGroup: controlplanev1.GroupVersion.Group, Kind: "ChalkosControlPlane", Name: cpName,
		}},
	}
	if reqs := clusterToControlPlane(t.Context(), cluster); len(reqs) != 1 || reqs[0].Name != cpName {
		t.Errorf("the Cluster maps to %v", reqs)
	}
	cluster.Spec.ControlPlaneRef.Kind = "KubeadmControlPlane"
	if reqs := clusterToControlPlane(t.Context(), cluster); len(reqs) != 0 {
		t.Errorf("a Cluster of another control plane maps to %v", reqs)
	}
}

// Through the manager, whose client reads from its cache, a ChalkosControlPlane with the watch
// filter's label gets its first Machine, cloned from the infrastructure template, and the Cluster's
// endpoint, set later, starts it.
func TestThroughTheManager(t *testing.T) {
	const filter = "manager-test"
	e := newEnv(t, 1, false)
	for _, obj := range []client.Object{&clusterv1.Cluster{}, &controlplanev1.ChalkosControlPlane{}} {
		name := clusterName
		if _, ok := obj.(*controlplanev1.ChalkosControlPlane); ok {
			name = cpName
		}
		if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, obj); err != nil {
			t.Fatal(err)
		}
		labels := obj.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[clusterv1.WatchLabel] = filter
		obj.SetLabels(labels)
		if err := testClient.Update(t.Context(), obj); err != nil {
			t.Fatal(err)
		}
	}

	mgr, err := ctrl.NewManager(testConfig, ctrl.Options{
		Scheme:     testScheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := &ChalkosControlPlaneReconciler{Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), Node: e.fake, WatchFilterValue: filter}
	if err := r.SetupWithManager(t.Context(), mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- mgr.Start(ctx) }()
	defer func() {
		cancel()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
	}()

	eventually(t, "the Paused condition", func() bool { return conditions.Has(e.cp(), controlplanev1.PausedCondition) })
	e.wantMachines(0)
	e.setEndpoint()
	eventually(t, "the first Machine", func() bool { return len(e.machines()) == 1 })
	if got := e.machines()[0].Labels[clusterv1.WatchLabel]; got != filter {
		t.Errorf("the Machine's watch filter label is %q, want %q", got, filter)
	}
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

// holdFinalizer is a finalizer the tests give a Machine to keep it while it is being deleted.
const holdFinalizer = "test.chalkos.dev/hold"

// hold gives the Machine name a finalizer that keeps it, once deleted, until release.
func (e *env) hold(name string) *clusterv1.Machine {
	e.t.Helper()
	var m clusterv1.Machine
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, &m); err != nil {
		e.t.Fatal(err)
	}
	controllerutil.AddFinalizer(&m, holdFinalizer)
	if err := testClient.Update(e.t.Context(), &m); err != nil {
		e.t.Fatal(err)
	}
	return &m
}

// deleteHeld deletes the Machine name, which a finalizer keeps until release.
func (e *env) deleteHeld(name string) {
	e.t.Helper()
	m := e.hold(name)
	if err := testClient.Delete(e.t.Context(), m); err != nil {
		e.t.Fatal(err)
	}
}

// release lets the Machine name, deleted by deleteHeld, go.
func (e *env) release(name string) {
	e.t.Helper()
	var m clusterv1.Machine
	if err := testClient.Get(e.t.Context(), client.ObjectKey{Namespace: e.ns, Name: name}, &m); err != nil {
		e.t.Fatal(err)
	}
	controllerutil.RemoveFinalizer(&m, holdFinalizer)
	if err := testClient.Update(e.t.Context(), &m); err != nil {
		e.t.Fatal(err)
	}
}

// other returns the Machine among ms that is not name.
func other(ms []clusterv1.Machine, name string) clusterv1.Machine {
	if ms[0].Name == name {
		return ms[1]
	}
	return ms[0]
}

// wantReason fails the test unless the ChalkosControlPlane's Initialized condition has the reason
// and its message contains the text.
func wantReason(t *testing.T, cp *controlplanev1.ChalkosControlPlane, reason, text string) {
	t.Helper()
	c := conditions.Get(cp, controlplanev1.InitializedCondition)
	if c == nil || c.Reason != reason || !strings.Contains(c.Message, text) {
		t.Fatalf("Initialized = %+v, want the reason %s with %q", c, reason, text)
	}
}

// A deleted ChalkosControlPlane deletes its Machines, creates none, and stays until they are
// gone.
func TestDeletionDeletesMachines(t *testing.T) {
	e := newEnv(t, 3, true)
	e.mustReconcile()
	m := e.hold(e.wantMachines(1)[0].Name)
	cp := e.cp()
	if !controllerutil.ContainsFinalizer(cp, controlplanev1.ChalkosControlPlaneFinalizer) {
		t.Fatal("the ChalkosControlPlane has no finalizer")
	}
	if err := testClient.Delete(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	e.mustReconcile()
	ms := e.wantMachines(1)
	if ms[0].DeletionTimestamp.IsZero() {
		t.Error("the Machine is not deleted")
	}
	if e.cp().DeletionTimestamp.IsZero() {
		t.Fatal("the ChalkosControlPlane is not being deleted")
	}
	e.release(m.Name)
	e.wantMachines(0)
	e.mustReconcile()
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: cpName}, &controlplanev1.ChalkosControlPlane{}); !apierrors.IsNotFound(err) {
		t.Errorf("the ChalkosControlPlane after its Machines are gone: %v, want not found", err)
	}
}

// A bootstrapped Machine that is deleted before the control plane is initialized is waited for,
// then forgotten, and a new first Machine is created.
func TestBootstrappedMachineGone(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	e.provision(m, addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
			return nil
		}
	})
	e.mustReconcile()
	e.deleteHeld(m.Name)
	e.mustReconcile()
	if ms := e.wantMachines(1); ms[0].Name != m.Name {
		t.Fatalf("a new Machine %s while the bootstrapped one is being deleted", ms[0].Name)
	}
	if got := e.cp().Status.BootstrappedNode; got != m.Name {
		t.Fatalf("bootstrappedNode %q while its Machine is being deleted, want %s", got, m.Name)
	}
	e.release(m.Name)
	e.mustReconcile()
	cp := e.cp()
	if cp.Status.BootstrappedNode != "" {
		t.Errorf("bootstrappedNode %q once its Machine is gone, want none", cp.Status.BootstrappedNode)
	}
	wantReason(t, cp, controlplanev1.BootstrappedMachineGoneReason, m.Name)
	if ms := e.wantMachines(1); ms[0].Name == m.Name {
		t.Error("no new first Machine")
	}
}

// The versions and UpToDate counts follow the Machines' versions: a Machine of an older version is
// not up to date, and the deprecated version is the lowest.
func TestVersionsAndUpToDate(t *testing.T) {
	const newVersion = "v1.37.2"
	e := newEnv(t, 3, true)
	first := e.initialize("10.0.0.1")
	second := other(e.wantMachines(2), first.Name)
	cp := e.cp()
	cp.Spec.Version = newVersion
	if err := testClient.Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	e.provision(second, "10.0.0.2", kubernetesv1.Phase_PHASE_BOOTSTRAPPED)
	e.mustReconcile()
	e.mustReconcile()
	ms := e.wantMachines(3)
	cp = e.cp()
	want := []clusterv1.StatusVersion{{Version: version, Replicas: 2}, {Version: newVersion, Replicas: 1}}
	if !slices.Equal(cp.Status.Versions, want) || cp.Status.Version != version { //nolint:staticcheck // The contract still reads it.
		t.Errorf("versions %+v and version %s, want %+v and %s", cp.Status.Versions, cp.Status.Version, want, version) //nolint:staticcheck // As above.
	}
	if got := ptr.Deref(cp.Status.UpToDateReplicas, -1); got != 1 {
		t.Errorf("upToDateReplicas %d, want 1", got)
	}
	for _, m := range ms {
		c := conditions.Get(&m, clusterv1.MachineUpToDateCondition)
		wantTrue := m.Spec.Version == newVersion
		if c == nil || (c.Status == metav1.ConditionTrue) != wantTrue || (!wantTrue && c.Reason != clusterv1.MachineNotUpToDateReason) {
			t.Errorf("Machine %s of %s: UpToDate %+v", m.Name, m.Spec.Version, c)
		}
	}
}

// A crash after the bootstrapped node was recorded and before Bootstrap was called leaves the
// node waiting; the next reconcile calls Bootstrap on it exactly once.
func TestCrashAfterCheckpoint(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	e.provision(m, addr, kubernetesv1.Phase_PHASE_WAITING)
	e.bootstrapsTo(addr)
	cp := e.cp()
	cp.Status.BootstrappedNode = m.Name
	if err := testClient.Status().Update(t.Context(), cp); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		e.mustReconcile()
	}
	if n := e.bootstraps(addr); n != 1 {
		t.Errorf("%d bootstraps, want 1", n)
	}
	if !ptr.Deref(e.cp().Status.Initialization.ControlPlaneInitialized, false) {
		t.Error("not initialized")
	}
}

// A Machine that cannot be created leaves neither its infrastructure nor its ChalkosConfig behind.
func TestFailedCreateCleansUp(t *testing.T) {
	e := newEnv(t, 1, true)
	base, err := client.NewWithWatch(testConfig, client.Options{Scheme: testScheme})
	if err != nil {
		t.Fatal(err)
	}
	e.r.Client = interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*clusterv1.Machine); ok {
				return errors.New("injected")
			}
			return c.Create(ctx, obj, opts...)
		},
	})
	if _, err := e.reconcile(); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("reconcile: %v, want the injected error", err)
	}
	e.wantMachines(0)
	infra := &unstructured.UnstructuredList{}
	infra.SetGroupVersionKind(builder.InfrastructureGroupVersion.WithKind(builder.GenericInfrastructureMachineKind + "List"))
	var cfgs bootstrapv1.ChalkosConfigList
	for _, list := range []client.ObjectList{infra, &cfgs} {
		if err := testClient.List(t.Context(), list, client.InNamespace(e.ns)); err != nil {
			t.Fatal(err)
		}
	}
	if len(infra.Items) != 0 || len(cfgs.Items) != 0 {
		t.Errorf("%d infrastructure machines and %d ChalkosConfigs left, want none", len(infra.Items), len(cfgs.Items))
	}
}

// A Machine being deleted is neither counted nor waited for to join: its replacement is created.
func TestDeletingMachinesNotCounted(t *testing.T) {
	e := newEnv(t, 3, true)
	first := e.initialize("10.0.0.1")
	second := other(e.wantMachines(2), first.Name)
	e.deleteHeld(second.Name)
	e.mustReconcile()
	e.wantMachines(3)
	if got := ptr.Deref(e.cp().Status.Replicas, -1); got != 2 {
		t.Errorf("replicas %d, want 2", got)
	}
}

// A refusal of Bootstrap other than for a bootstrapped node is reported and tried again.
func TestBootstrapRefusedIsReported(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	e.provision(e.wantMachines(1)[0], addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			return connect.NewError(connect.CodeFailedPrecondition, "the node's Kubernetes files are not prepared yet")
		}
	})
	res, err := e.reconcile()
	if err != nil || res.RequeueAfter == 0 {
		t.Errorf("reconcile of a refused bootstrap = %+v, %v; want a requeue", res, err)
	}
	wantReason(t, e.cp(), controlplanev1.BootstrapRefusedReason, "not prepared yet")
}

// A Machine whose config is ready but that has no InternalIP is not bootstrapped.
func TestNoBootstrapWithoutAddress(t *testing.T) {
	e := newEnv(t, 1, true)
	e.mustReconcile()
	m := e.wantMachines(1)[0]
	var cfg bootstrapv1.ChalkosConfig
	if err := testClient.Get(t.Context(), client.ObjectKey{Namespace: e.ns, Name: m.Name}, &cfg); err != nil {
		t.Fatal(err)
	}
	conditions.Set(&cfg, metav1.Condition{Type: bootstrapv1.ReadyCondition, Status: metav1.ConditionTrue, Reason: bootstrapv1.InstalledReason})
	if err := testClient.Status().Update(t.Context(), &cfg); err != nil {
		t.Fatal(err)
	}
	e.mustReconcile()
	if got := e.cp().Status.BootstrappedNode; got != "" {
		t.Errorf("bootstrappedNode %q without an InternalIP", got)
	}
}

// A kubeconfig Secret the ChalkosControlPlane does not control is left as it is.
func TestUnownedKubeconfigLeftAlone(t *testing.T) {
	e := newEnv(t, 1, true)
	key := client.ObjectKey{Namespace: e.ns, Name: clusterName + "-kubeconfig"}
	e.create(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}, Data: map[string][]byte{"value": []byte("mine")}})
	e.initialize("10.0.0.1")
	var s corev1.Secret
	if err := testClient.Get(t.Context(), key, &s); err != nil {
		t.Fatal(err)
	}
	if string(s.Data["value"]) != "mine" || len(s.OwnerReferences) != 0 {
		t.Errorf("the user's kubeconfig Secret was changed: %+v", s)
	}
	if at := e.cp().Status.KubeconfigExpiresAt; at != nil {
		t.Errorf("kubeconfigExpiresAt %v for a kubeconfig the controller did not issue", at)
	}
}

// A kubeconfig whose certificate expired while the controller was down is issued again.
func TestExpiredKubeconfigReissued(t *testing.T) {
	e := newEnv(t, 1, true)
	e.initialize("10.0.0.1")
	key := client.ObjectKey{Namespace: e.ns, Name: clusterName + "-kubeconfig"}
	var s corev1.Secret
	if err := testClient.Get(t.Context(), key, &s); err != nil {
		t.Fatal(err)
	}
	e.clock.Step(72 * time.Hour)
	e.mustReconcile()
	if err := testClient.Get(t.Context(), key, &s); err != nil {
		t.Fatal(err)
	}
	if cert := clientCert(t, &s); !cert.NotAfter.After(e.clock.Now()) {
		t.Errorf("certificate until %s, want after %s", cert.NotAfter, e.clock.Now())
	}
}

// A bootstrapped node whose status cannot be read is asked again later, and the error is reported.
func TestStatusErrorRequeues(t *testing.T) {
	const addr = "10.0.0.1"
	e := newEnv(t, 1, true)
	e.mustReconcile()
	e.provision(e.wantMachines(1)[0], addr, kubernetesv1.Phase_PHASE_WAITING)
	e.fake.Update(addr, func(n *nodetest.Node) {
		n.Bootstrap = func() error {
			e.setStatus(addr, kubernetesv1.Phase_PHASE_BOOTSTRAPPED, false)
			return nil
		}
	})
	e.mustReconcile()
	e.fake.Update(addr, func(n *nodetest.Node) { n.Err = errors.New("connection refused") })
	res, err := e.reconcile()
	if err != nil || res.RequeueAfter == 0 {
		t.Errorf("reconcile of an unreachable node = %+v, %v; want a requeue", res, err)
	}
	wantReason(t, e.cp(), controlplanev1.NotInitializedReason, "connection refused")
}
