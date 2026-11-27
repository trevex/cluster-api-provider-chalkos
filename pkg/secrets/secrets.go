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

// Package secrets keeps a cluster's chalkos secrets, the CAs and keys its nodes are installed
// with, in a Secret next to the Cluster.
package secrets

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	clusterctlv1 "sigs.k8s.io/cluster-api/cmd/clusterctl/api/v1alpha3"

	"github.com/trevex/chalkos/pkg/clustersecrets"
)

// Key is the Secret's key that holds the plaintext secrets file.
const Key = "secrets.json"

// Name is the name of the cluster's secrets Secret: <cluster>-chalkos-secrets.
func Name(cluster string) string { return cluster + "-chalkos-secrets" }

// Get loads the secrets of cluster, the Cluster's namespace and name. ok is false when the Secret
// does not exist, or when c is a cache that does not hold it yet.
func Get(ctx context.Context, c client.Reader, cluster client.ObjectKey) (s clustersecrets.Secrets, ok bool, err error) {
	var secret corev1.Secret
	err = c.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: Name(cluster.Name)}, &secret)
	if apierrors.IsNotFound(err) {
		return clustersecrets.Secrets{}, false, nil
	}
	if err != nil {
		return clustersecrets.Secrets{}, false, err
	}
	s, err = decode(&secret)
	return s, err == nil, err
}

// Ensure loads the secrets of cluster or generates them at now, in a Secret owned by owner and
// labelled with the cluster's name and for clusterctl move. A Secret that exists already, such as
// one the user created first, is kept as it is; a Secret the user creates carries the labels
// cluster.x-k8s.io/cluster-name and clusterctl.cluster.x-k8s.io/move itself, since clusterctl move
// finds a Secret without an owner by its labels alone. c reads uncached, as mgr.GetAPIReader()
// does, or from a cache that holds these Secrets: a Secret another reconcile created that c does
// not read yet is an error, not secrets to generate again.
func Ensure(ctx context.Context, c client.Client, cluster client.ObjectKey, owner metav1.OwnerReference, now time.Time) (clustersecrets.Secrets, error) {
	s, ok, err := Get(ctx, c, cluster)
	if err != nil || ok {
		return s, err
	}
	s, err = clustersecrets.Generate(now)
	if err != nil {
		return clustersecrets.Secrets{}, err
	}
	data, err := s.Encode()
	if err != nil {
		return clustersecrets.Secrets{}, err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      Name(cluster.Name),
			Labels: map[string]string{
				clusterv1.ClusterNameLabel:       cluster.Name,
				clusterctlv1.ClusterctlMoveLabel: "",
			},
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{Key: data},
	}
	err = c.Create(ctx, secret)
	if apierrors.IsAlreadyExists(err) {
		// Another reconcile created it first; its secrets are the cluster's.
		s, ok, err = Get(ctx, c, cluster)
		if err == nil && !ok {
			return clustersecrets.Secrets{}, fmt.Errorf("the Secret %s/%s exists but is not readable yet", cluster.Namespace, Name(cluster.Name))
		}
		return s, err
	}
	if err != nil {
		return clustersecrets.Secrets{}, err
	}
	return s, nil
}

// decode reads the secrets file the Secret holds.
func decode(secret *corev1.Secret) (clustersecrets.Secrets, error) {
	data, ok := secret.Data[Key]
	if !ok {
		return clustersecrets.Secrets{}, fmt.Errorf("the Secret %s/%s has no key %s", secret.Namespace, secret.Name, Key)
	}
	s, err := clustersecrets.ReadJSON(data)
	if err != nil {
		return clustersecrets.Secrets{}, fmt.Errorf("the Secret %s/%s: %w", secret.Namespace, secret.Name, err)
	}
	return s, nil
}
