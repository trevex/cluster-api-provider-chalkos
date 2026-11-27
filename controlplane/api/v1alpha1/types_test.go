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

package v1alpha1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// testEnv and testClient are shared across this package's tests: envtest starts one apiserver
// with this package's CRDs installed, and every test talks to it through the same client.
var (
	testEnv    *envtest.Environment
	testClient client.Client
)

func TestMain(m *testing.M) {
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting envtest:", err)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, "adding corev1 to scheme:", err)
		os.Exit(1)
	}
	if err := AddToScheme(scheme); err != nil {
		fmt.Fprintln(os.Stderr, "adding controlplane v1alpha1 to scheme:", err)
		os.Exit(1)
	}

	testClient, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintln(os.Stderr, "creating client:", err)
		os.Exit(1)
	}

	// The apiserver bootstraps the "default" namespace itself; ignore it already being there.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	if err := testClient.Create(context.Background(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
		fmt.Fprintln(os.Stderr, "creating default namespace:", err)
		os.Exit(1)
	}

	code := m.Run()

	if err := testEnv.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, "stopping envtest:", err)
	}
	os.Exit(code)
}

// digestImage returns a well-formed repository@sha256:… reference, filled with the given byte
// so different tests don't collide on a shared image.
func digestImage(fill byte) string {
	return fmt.Sprintf("example.com/chalkos@sha256:%s", strings.Repeat(string(fill), 64))
}

// validMachineTemplate returns a ChalkosControlPlaneMachineTemplate referencing a plausible
// infrastructure template, valid under the ContractVersionedObjectReference's own pattern rules.
func validMachineTemplate() ChalkosControlPlaneMachineTemplate {
	return ChalkosControlPlaneMachineTemplate{
		Spec: ChalkosControlPlaneMachineTemplateSpec{
			InfrastructureRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: "infrastructure.cluster.x-k8s.io",
				Kind:     "DockerMachineTemplate",
				Name:     "test-control-plane",
			},
		},
	}
}

func TestChalkosControlPlane_ReplicasOdd(t *testing.T) {
	ctx := t.Context()

	cases := []struct {
		name     string
		replicas *int32
		wantErr  bool
	}{
		{name: "one", replicas: ptr.To(int32(1)), wantErr: false},
		{name: "three", replicas: ptr.To(int32(3)), wantErr: false},
		{name: "unset defaults to one", replicas: nil, wantErr: false},
		{name: "two", replicas: ptr.To(int32(2)), wantErr: true},
		{name: "zero", replicas: ptr.To(int32(0)), wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cp := &ChalkosControlPlane{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "replicas-", Namespace: "default"},
				Spec: ChalkosControlPlaneSpec{
					Replicas:        c.replicas,
					Version:         "v1.33.0",
					Image:           digestImage('a'),
					MachineTemplate: validMachineTemplate(),
				},
			}

			err := testClient.Create(ctx, cp)
			if c.wantErr {
				if err == nil {
					t.Fatalf("Create with replicas %v: want an error, got none", c.replicas)
				}
				if !apierrors.IsInvalid(err) {
					t.Fatalf("Create with replicas %v: got %v, want an Invalid error", c.replicas, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Create with replicas %v: %v", c.replicas, err)
			}
		})
	}
}

func TestChalkosControlPlane_ImagePattern(t *testing.T) {
	ctx := t.Context()

	cases := []struct {
		name    string
		image   string
		wantErr bool
	}{
		{name: "by digest", image: digestImage('b'), wantErr: false},
		{name: "by tag", image: "example.com/chalkos:latest", wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cp := &ChalkosControlPlane{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "image-", Namespace: "default"},
				Spec: ChalkosControlPlaneSpec{
					Version:         "v1.33.0",
					Image:           c.image,
					MachineTemplate: validMachineTemplate(),
				},
			}

			err := testClient.Create(ctx, cp)
			if c.wantErr {
				if err == nil {
					t.Fatalf("Create with image %q: want an error, got none", c.image)
				}
				if !apierrors.IsInvalid(err) {
					t.Fatalf("Create with image %q: got %v, want an Invalid error", c.image, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Create with image %q: %v", c.image, err)
			}
		})
	}
}

// TestChalkosControlPlane_SpecRequired checks that a ChalkosControlPlane with no spec at all is
// refused: an optional spec would let Version's and Image's own validation go unevaluated, since
// a required field inside an absent object is not checked.
func TestChalkosControlPlane_SpecRequired(t *testing.T) {
	ctx := t.Context()

	cp := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"generateName": "nospec-", "namespace": "default"},
	}}
	cp.SetGroupVersionKind(GroupVersion.WithKind("ChalkosControlPlane"))

	err := testClient.Create(ctx, cp)
	if err == nil {
		t.Fatal("Create with no spec: want an error, got none")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Create with no spec: got %v, want an Invalid error", err)
	}
}

// TestChalkosControlPlane_FilesMustBeAList checks that Files, whose schema has to stay
// permissive enough to carry any chalkos document, is still constrained to a list of objects: a
// bare string or object is refused rather than silently accepted.
func TestChalkosControlPlane_FilesMustBeAList(t *testing.T) {
	ctx := t.Context()

	cp := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"generateName": "files-notlist-", "namespace": "default"},
		"spec": map[string]any{
			"version": "v1.33.0",
			"image":   digestImage('e'),
			"machineTemplate": map[string]any{
				"spec": map[string]any{
					"infrastructureRef": map[string]any{
						"apiGroup": "infrastructure.cluster.x-k8s.io",
						"kind":     "DockerMachineTemplate",
						"name":     "test-control-plane",
					},
				},
			},
			"files": "not-a-list",
		},
	}}
	cp.SetGroupVersionKind(GroupVersion.WithKind("ChalkosControlPlane"))

	err := testClient.Create(ctx, cp)
	if err == nil {
		t.Fatal("Create with non-list files: want an error, got none")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Create with non-list files: got %v, want an Invalid error", err)
	}
}

// TestChalkosControlPlane_KubeconfigDefault checks that omitting Kubeconfig entirely still
// defaults TTLSeconds: the parent field needs its own default for the nested one to apply.
func TestChalkosControlPlane_KubeconfigDefault(t *testing.T) {
	ctx := t.Context()

	cp := &ChalkosControlPlane{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "kubeconfig-", Namespace: "default"},
		Spec: ChalkosControlPlaneSpec{
			Version:         "v1.33.0",
			Image:           digestImage('f'),
			MachineTemplate: validMachineTemplate(),
		},
	}
	if err := testClient.Create(ctx, cp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got := &ChalkosControlPlane{}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(cp), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Kubeconfig.TTLSeconds == nil || *got.Spec.Kubeconfig.TTLSeconds != 86400 {
		t.Fatalf("Kubeconfig.TTLSeconds: got %v, want 86400", got.Spec.Kubeconfig.TTLSeconds)
	}
}

// TestChalkosControlPlaneTemplate_ImageOptional checks that the instantiated ChalkosControlPlane's
// Image can be left unset in a ClusterClass's template: its topology may compute it instead.
func TestChalkosControlPlaneTemplate_ImageOptional(t *testing.T) {
	ctx := t.Context()

	tmpl := &ChalkosControlPlaneTemplate{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "cptmpl-", Namespace: "default"},
		Spec: ChalkosControlPlaneTemplateSpec{
			Template: ChalkosControlPlaneTemplateResource{
				Spec: ChalkosControlPlaneTemplateResourceSpec{
					MachineTemplate: ChalkosControlPlaneTemplateMachineTemplate{
						ObjectMeta: clusterv1.ObjectMeta{Labels: map[string]string{"pool": "control-plane"}},
					},
				},
			},
		},
	}

	if err := testClient.Create(ctx, tmpl); err != nil {
		t.Fatalf("Create without image: %v", err)
	}

	got := &ChalkosControlPlaneTemplate{}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(tmpl), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Template.Spec.Image != "" {
		t.Fatalf("Image: got %q, want empty", got.Spec.Template.Spec.Image)
	}
}

// TestChalkosControlPlaneTemplate_NoInfrastructureRef checks that the template accepts a
// MachineTemplate without an infrastructure reference: a ClusterClass's topology computes that
// reference for each Cluster that uses it, so the template's type has no field for it at all.
func TestChalkosControlPlaneTemplate_NoInfrastructureRef(t *testing.T) {
	ctx := t.Context()

	tmpl := &ChalkosControlPlaneTemplate{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "cptmpl-noinfra-", Namespace: "default"},
		Spec: ChalkosControlPlaneTemplateSpec{
			Template: ChalkosControlPlaneTemplateResource{
				Spec: ChalkosControlPlaneTemplateResourceSpec{
					Image: digestImage('d'),
					MachineTemplate: ChalkosControlPlaneTemplateMachineTemplate{
						Spec: ChalkosControlPlaneTemplateMachineTemplateSpec{
							Deletion: ChalkosControlPlaneMachineTemplateDeletionSpec{
								NodeDrainTimeoutSeconds: ptr.To(int32(120)),
							},
						},
					},
				},
			},
		},
	}

	if err := testClient.Create(ctx, tmpl); err != nil {
		t.Fatalf("Create without infrastructureRef: %v", err)
	}
}

func TestChalkosControlPlane_Scale(t *testing.T) {
	ctx := t.Context()

	cp := &ChalkosControlPlane{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "scale-", Namespace: "default"},
		Spec: ChalkosControlPlaneSpec{
			Replicas:        ptr.To(int32(3)),
			Version:         "v1.33.0",
			Image:           digestImage('c'),
			MachineTemplate: validMachineTemplate(),
		},
	}
	if err := testClient.Create(ctx, cp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	scale := &autoscalingv1.Scale{}
	if err := testClient.SubResource("scale").Get(ctx, cp, scale); err != nil {
		t.Fatalf("Get scale: %v", err)
	}
	if scale.Spec.Replicas != 3 {
		t.Fatalf("scale.Spec.Replicas: got %d, want 3", scale.Spec.Replicas)
	}

	scale.Spec.Replicas = 5
	if err := testClient.SubResource("scale").Update(ctx, cp, client.WithSubResourceBody(scale)); err != nil {
		t.Fatalf("Update scale: %v", err)
	}

	got := &ChalkosControlPlane{}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(cp), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 5 {
		t.Fatalf("Spec.Replicas after scale: got %v, want 5", got.Spec.Replicas)
	}
}

// TestChalkosControlPlane_ScaleEvenRefused checks that the odd-replicas rule still applies when
// scaling through the scale subresource, not just on a direct update of spec.replicas.
func TestChalkosControlPlane_ScaleEvenRefused(t *testing.T) {
	ctx := t.Context()

	cp := &ChalkosControlPlane{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "scale-even-", Namespace: "default"},
		Spec: ChalkosControlPlaneSpec{
			Replicas:        ptr.To(int32(3)),
			Version:         "v1.33.0",
			Image:           digestImage('0'),
			MachineTemplate: validMachineTemplate(),
		},
	}
	if err := testClient.Create(ctx, cp); err != nil {
		t.Fatalf("Create: %v", err)
	}

	scale := &autoscalingv1.Scale{}
	if err := testClient.SubResource("scale").Get(ctx, cp, scale); err != nil {
		t.Fatalf("Get scale: %v", err)
	}

	scale.Spec.Replicas = 4
	err := testClient.SubResource("scale").Update(ctx, cp, client.WithSubResourceBody(scale))
	if err == nil {
		t.Fatal("Update scale to 4: want an error, got none")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Update scale to 4: got %v, want an Invalid error", err)
	}

	got := &ChalkosControlPlane{}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(cp), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Replicas == nil || *got.Spec.Replicas != 3 {
		t.Fatalf("Spec.Replicas after refused scale: got %v, want unchanged at 3", got.Spec.Replicas)
	}
}
