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

// Package images reads the chalkos images the controllers install from their registries: with
// the credentials of an image pull Secret, keeping the artifacts read before.
package images

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// Timeout bounds the read of an image's header and contract from its registry.
const Timeout = 2 * time.Minute

// maxArtifacts is how many image artifacts, and how many contracts, a Cache holds.
const maxArtifacts = 64

// Keychain returns the credentials of the image pull Secret ref names in the namespace, read with
// r; without a ref, images are read anonymously.
func Keychain(ctx context.Context, r client.Reader, namespace string, ref *corev1.LocalObjectReference) (authn.Keychain, error) {
	if ref == nil {
		return keychain{}, nil
	}
	var s corev1.Secret
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Name}, &s); err != nil {
		return nil, fmt.Errorf("the image pull Secret: %w", err)
	}
	return KeychainOf(&s)
}

// Cache holds the artifacts of images read before, by reference and pull Secret, and the contracts
// built from them, by reference. It is safe for concurrent use; its zero value is empty.
type Cache struct {
	mu        sync.Mutex
	artifacts map[string]*ociimage.Artifact
	contracts map[string]sdk.Contract
}

// Image reads the image ref, by digest, through n with the credentials kc, or answers it from the
// images read before with the same pull Secret pull of the namespace: an image named by digest
// stays what it was.
func (c *Cache) Image(ctx context.Context, n node.Client, ref, namespace string, pull *corev1.LocalObjectReference, kc authn.Keychain) (*ociimage.Artifact, error) {
	key := ref
	if pull != nil {
		key += " " + namespace + "/" + pull.Name
	}
	if a, ok := c.get(key); ok {
		return a, nil
	}
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	a, err := n.Image(ctx, ref, kc)
	if err != nil {
		return nil, err
	}
	c.add(key, a)
	return a, nil
}

// get returns the artifact held under key.
func (c *Cache) get(key string) (*ociimage.Artifact, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.artifacts[key]
	return a, ok
}

// add holds a under key, dropping another artifact when the cache is full.
func (c *Cache) add(key string, a *ociimage.Artifact) {
	c.mu.Lock()
	defer c.mu.Unlock()
	hold(&c.artifacts, key, a)
}

// Contract is the contract, with chalkos's layers, of the image ref, read through n with the
// credentials of the image pull Secret pull of the namespace, which r reads, as Image reads it. An
// image named by digest keeps its contract, which is built from its schemas once; two reconciles
// that miss it at once may both build it.
func (c *Cache) Contract(ctx context.Context, r client.Reader, n node.Client, ref, namespace string, pull *corev1.LocalObjectReference) (sdk.Contract, error) {
	kc, err := Keychain(ctx, r, namespace, pull)
	if err != nil {
		return sdk.Contract{}, err
	}
	a, err := c.Image(ctx, n, ref, namespace, pull, kc)
	if err != nil {
		return sdk.Contract{}, fmt.Errorf("reading the image: %w", err)
	}
	c.mu.Lock()
	ct, ok := c.contracts[a.Reference]
	c.mu.Unlock()
	if ok {
		return ct, nil
	}
	// Compiling the schemas takes long, so the cache is not locked meanwhile.
	ct, err = sdk.ArtifactContract(layers.All(), a)
	if err != nil {
		return sdk.Contract{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	hold(&c.contracts, a.Reference, ct)
	return ct, nil
}

// hold holds v under key in m, dropping another entry when m is full. The caller holds the
// cache's lock.
func hold[V any](m *map[string]V, key string, v V) {
	if *m == nil {
		*m = map[string]V{}
	}
	if _, ok := (*m)[key]; !ok && len(*m) >= maxArtifacts {
		for k := range *m {
			delete(*m, k)
			break
		}
	}
	(*m)[key] = v
}
