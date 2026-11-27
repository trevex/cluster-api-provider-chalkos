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

package secrets_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/trevex/chalkos/pkg/clustersecrets"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

var (
	cluster = client.ObjectKey{Namespace: "lab", Name: "c1"}
	owner   = metav1.OwnerReference{APIVersion: "controlplane.cluster.x-k8s.io/v1alpha1", Kind: "ChalkosControlPlane", Name: "c1", UID: types.UID("uid-1")}
)

func encode(t *testing.T, s clustersecrets.Secrets) []byte {
	t.Helper()
	data, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestEnsureGeneratesOnce(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().Build()
	if _, ok, err := secrets.Get(ctx, c, cluster); err != nil || ok {
		t.Fatalf("Get before Ensure = %v, %v; want none", ok, err)
	}
	first, err := secrets.Ensure(ctx, c, cluster, owner, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	second, err := secrets.Ensure(ctx, c, cluster, owner, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encode(t, first), encode(t, second)) {
		t.Error("the second Ensure generated other secrets")
	}
	got, ok, err := secrets.Get(ctx, c, cluster)
	if err != nil || !ok {
		t.Fatalf("Get = %v, %v", ok, err)
	}
	if !bytes.Equal(encode(t, first), encode(t, got)) {
		t.Error("Get loaded other secrets")
	}

	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "lab", Name: "c1-chalkos-secrets"}, &secret); err != nil {
		t.Fatal(err)
	}
	if secret.Type != corev1.SecretTypeOpaque {
		t.Errorf("type %s, want Opaque", secret.Type)
	}
	if secret.Labels["cluster.x-k8s.io/cluster-name"] != "c1" {
		t.Errorf("labels %v, want the cluster name", secret.Labels)
	}
	if _, ok := secret.Labels["clusterctl.cluster.x-k8s.io/move"]; !ok {
		t.Errorf("labels %v, want the move label", secret.Labels)
	}
	if len(secret.OwnerReferences) != 1 || secret.OwnerReferences[0].UID != owner.UID {
		t.Errorf("owners %v, want the control plane", secret.OwnerReferences)
	}
}

func TestEnsureKeepsTheUsersSecret(t *testing.T) {
	ctx := context.Background()
	mine, err := clustersecrets.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "lab", Name: secrets.Name("c1")},
		Data:       map[string][]byte{secrets.Key: encode(t, mine)},
	}).Build()
	got, err := secrets.Ensure(ctx, c, cluster, owner, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encode(t, mine), encode(t, got)) {
		t.Error("Ensure replaced the user's secrets")
	}
	var secret corev1.Secret
	if err := c.Get(ctx, client.ObjectKey{Namespace: "lab", Name: secrets.Name("c1")}, &secret); err != nil {
		t.Fatal(err)
	}
	if len(secret.OwnerReferences) != 0 || len(secret.Labels) != 0 {
		t.Errorf("Ensure changed the user's Secret: %v, %v", secret.OwnerReferences, secret.Labels)
	}
}

func TestMalformedSecret(t *testing.T) {
	ctx := context.Background()
	c := fake.NewClientBuilder().WithObjects(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "lab", Name: secrets.Name("c1")},
		Data:       map[string][]byte{secrets.Key: []byte(`{"version": 1, "bogus": true}`)},
	}).Build()
	if _, _, err := secrets.Get(ctx, c, cluster); err == nil {
		t.Error("Get loaded a malformed Secret")
	}
	if _, err := secrets.Ensure(ctx, c, cluster, owner, time.Now()); err == nil {
		t.Error("Ensure accepted a malformed Secret")
	}
}

// A Secret another reconcile created, which the client does not read yet, is an error rather than
// empty secrets.
func TestEnsureSecretNotReadableYet(t *testing.T) {
	ctx := context.Background()
	gr := schema.GroupResource{Resource: "secrets"}
	c := fake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return apierrors.NewNotFound(gr, secrets.Name("c1"))
		},
		Create: func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error {
			return apierrors.NewAlreadyExists(gr, secrets.Name("c1"))
		},
	}).Build()
	_, err := secrets.Ensure(ctx, c, cluster, owner, time.Now())
	if err == nil || !strings.Contains(err.Error(), "exists but is not readable yet") {
		t.Errorf("Ensure = %v, want the Secret not readable yet", err)
	}
}
