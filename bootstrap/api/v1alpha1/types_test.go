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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
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
		fmt.Fprintln(os.Stderr, "adding bootstrap v1alpha1 to scheme:", err)
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

func TestChalkosConfig_ImagePattern(t *testing.T) {
	ctx := t.Context()

	cases := []struct {
		name    string
		image   string
		wantErr bool
	}{
		{name: "by digest", image: digestImage('a'), wantErr: false},
		{name: "by tag", image: "example.com/chalkos:latest", wantErr: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &ChalkosConfig{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "image-", Namespace: "default"},
				Spec:       ChalkosConfigSpec{Image: c.image},
			}

			err := testClient.Create(ctx, cfg)
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

// TestChalkosConfig_SpecRequired checks that a ChalkosConfig with no spec at all is refused: an
// optional spec would let Image's own validation go unevaluated, since a required field inside
// an absent object is not checked.
func TestChalkosConfig_SpecRequired(t *testing.T) {
	ctx := t.Context()

	cfg := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"generateName": "nospec-", "namespace": "default"},
	}}
	cfg.SetGroupVersionKind(GroupVersion.WithKind("ChalkosConfig"))

	err := testClient.Create(ctx, cfg)
	if err == nil {
		t.Fatal("Create with no spec: want an error, got none")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Create with no spec: got %v, want an Invalid error", err)
	}
}

// TestChalkosConfig_FilesMustBeAList checks that Files, whose schema has to stay permissive
// enough to carry any chalkos document, is still constrained to a list of objects: a bare
// string or object is refused rather than silently accepted.
func TestChalkosConfig_FilesMustBeAList(t *testing.T) {
	ctx := t.Context()

	cfg := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"generateName": "files-notlist-", "namespace": "default"},
		"spec": map[string]any{
			"image": digestImage('c'),
			"files": "not-a-list",
		},
	}}
	cfg.SetGroupVersionKind(GroupVersion.WithKind("ChalkosConfig"))

	err := testClient.Create(ctx, cfg)
	if err == nil {
		t.Fatal("Create with non-list files: want an error, got none")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("Create with non-list files: got %v, want an Invalid error", err)
	}
}

// TestChalkosConfigTemplate_ImageOptional checks that a cloned ChalkosConfig's Image can be left
// unset in a template: a MachineDeployment's or MachineSet's rollout may compute it instead.
func TestChalkosConfigTemplate_ImageOptional(t *testing.T) {
	ctx := t.Context()

	tmpl := &ChalkosConfigTemplate{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "tmpl-", Namespace: "default"},
		Spec: ChalkosConfigTemplateSpec{
			Template: ChalkosConfigTemplateResource{
				ObjectMeta: clusterv1.ObjectMeta{Labels: map[string]string{"pool": "workers"}},
			},
		},
	}

	if err := testClient.Create(ctx, tmpl); err != nil {
		t.Fatalf("Create without image: %v", err)
	}

	got := &ChalkosConfigTemplate{}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(tmpl), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Template.Spec.Image != "" {
		t.Fatalf("Image: got %q, want empty", got.Spec.Template.Spec.Image)
	}
}

func TestChalkosConfig_FilesRoundTrip(t *testing.T) {
	ctx := t.Context()

	doc := []byte(`{"kind":"Node","labels":{"role":"worker"}}`)
	cfg := &ChalkosConfig{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "files-", Namespace: "default"},
		Spec: ChalkosConfigSpec{
			Image: digestImage('b'),
			Files: []runtime.RawExtension{{Raw: doc}},
		},
	}

	if err := testClient.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got := &ChalkosConfig{}
	if err := testClient.Get(ctx, client.ObjectKeyFromObject(cfg), got); err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.Spec.Files) != 1 {
		t.Fatalf("Files: got %d entries, want 1", len(got.Spec.Files))
	}

	var want, have map[string]any
	if err := json.Unmarshal(doc, &want); err != nil {
		t.Fatalf("unmarshalling the document sent: %v", err)
	}
	if err := json.Unmarshal(got.Spec.Files[0].Raw, &have); err != nil {
		t.Fatalf("unmarshalling the document read back: %v", err)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatalf("Files[0] round-trip: got %v, want %v", have, want)
	}
}
