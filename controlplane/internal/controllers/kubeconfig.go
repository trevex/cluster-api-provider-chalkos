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
	"crypto/x509"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/certs"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/secret"

	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/pki"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
)

// kubeconfigUser is the user name of the kubeconfig Secret's client certificate.
const kubeconfigUser = "cluster-api"

// defaultKubeconfigTTL is the validity of the kubeconfig Secret's client certificate when the
// ChalkosControlPlane names none.
const defaultKubeconfigTTL = 24 * time.Hour

// reconcileKubeconfig keeps the cluster's kubeconfig Secret, <cluster>-kubeconfig, whose client
// certificate is issued again once two thirds of its validity passed. It returns how long until
// then. A kubeconfig Secret the ChalkosControlPlane does not control, such as one the user
// created, is left as it is.
func (r *ChalkosControlPlaneReconciler) reconcileKubeconfig(ctx context.Context, s *scope, sec clustersecrets.Secrets) (time.Duration, error) {
	now := r.clock().Now()
	key := client.ObjectKey{Namespace: s.cluster.Namespace, Name: secret.Name(s.cluster.Name, secret.Kubeconfig)}
	existing := &corev1.Secret{}
	err := r.APIReader.Get(ctx, key, existing)
	switch {
	case apierrors.IsNotFound(err):
		existing = nil
	case err != nil:
		return 0, err
	case !util.IsControlledBy(existing, s.cp, controlPlaneKind.GroupKind()):
		s.cp.Status.KubeconfigExpiresAt = nil
		return 0, nil
	default:
		// A certificate that cannot be read is issued again.
		if cert, err := kubeconfigCert(existing.Data[secret.KubeconfigDataName]); err == nil && now.Before(pki.RenewAt(cert)) {
			s.cp.Status.KubeconfigExpiresAt = &metav1.Time{Time: cert.NotAfter}
			return pki.RenewAt(cert).Sub(now), nil
		}
	}

	ttl := defaultKubeconfigTTL
	if secs := s.cp.Spec.Kubeconfig.TTLSeconds; secs != nil {
		ttl = time.Duration(*secs) * time.Second
	}
	endpoint := s.cluster.Spec.ControlPlaneEndpoint
	data, err := sdkkubernetes.Kubeconfig(sec, sdkkubernetes.KubeconfigRequest{
		Context: s.cluster.Name,
		Server:  documents.Endpoint(endpoint.Host, endpoint.Port),
		User:    kubeconfigUser,
		TTL:     ttl,
	}, now)
	if err != nil {
		return 0, fmt.Errorf("issue the kubeconfig: %w", err)
	}
	cert, err := kubeconfigCert(data)
	if err != nil {
		return 0, err
	}
	want := kubeconfig.GenerateSecretWithOwner(util.ObjectKey(s.cluster), data, *metav1.NewControllerRef(s.cp, controlPlaneKind))
	if existing == nil {
		err = r.Client.Create(ctx, want)
	} else {
		existing.Data = want.Data
		err = r.Client.Update(ctx, existing)
	}
	if err != nil {
		return 0, err
	}
	s.cp.Status.KubeconfigExpiresAt = ptr.To(metav1.NewTime(cert.NotAfter))
	return pki.RenewAt(cert).Sub(now), nil
}

// kubeconfigCert is the client certificate of the kubeconfig's single user.
func kubeconfigCert(data []byte) (*x509.Certificate, error) {
	cfg, err := clientcmd.Load(data)
	if err != nil {
		return nil, fmt.Errorf("the kubeconfig: %w", err)
	}
	users := slices.Collect(maps.Values(cfg.AuthInfos))
	if len(users) != 1 {
		return nil, fmt.Errorf("the kubeconfig has %d users, not 1", len(users))
	}
	cert, err := certs.DecodeCertPEM(users[0].ClientCertificateData)
	if err == nil && cert == nil {
		err = errors.New("the kubeconfig's user has no client certificate")
	}
	return cert, err
}
