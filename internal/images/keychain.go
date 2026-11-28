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

package images

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
)

// keychain is the credentials of a kubernetes.io/dockerconfigjson Secret, by the registry host,
// or the host and a repository path, they apply to. A repository takes the credentials of the
// longest key it falls under; one under no key is read anonymously.
type keychain map[string]authn.AuthConfig

// KeychainOf reads the credentials of a kubernetes.io/dockerconfigjson Secret. Of two keys that
// name the same registry, such as docker.io and https://index.docker.io/v1/, the first in
// lexical order counts.
func KeychainOf(s *corev1.Secret) (keychain, error) {
	if s.Type != corev1.SecretTypeDockerConfigJson {
		return nil, fmt.Errorf("the Secret %s/%s is of type %s, not %s", s.Namespace, s.Name, s.Type, corev1.SecretTypeDockerConfigJson)
	}
	var file struct {
		Auths map[string]authn.AuthConfig `json:"auths"`
	}
	if err := json.Unmarshal(s.Data[corev1.DockerConfigJsonKey], &file); err != nil {
		return nil, fmt.Errorf("the Secret %s/%s: %w", s.Namespace, s.Name, err)
	}
	k := keychain{}
	for _, key := range slices.Sorted(maps.Keys(file.Auths)) {
		if n := normalize(key); n != "" {
			if _, ok := k[n]; !ok {
				k[n] = file.Auths[key]
			}
		}
	}
	return k, nil
}

// normalize is the registry host and repository path a docker config's key names, which may be
// a URL such as https://index.docker.io/v1/. Docker Hub's names are its registry's,
// index.docker.io.
func normalize(key string) string {
	key = strings.TrimPrefix(strings.TrimPrefix(key, "https://"), "http://")
	host, path, _ := strings.Cut(strings.TrimSuffix(key, "/"), "/")
	if path == "v1" || path == "v2" {
		path = ""
	}
	if host == "docker.io" || host == "registry-1.docker.io" {
		host = name.DefaultRegistry
	}
	if path == "" {
		return host
	}
	return host + "/" + path
}

// Resolve returns the credentials for the resource: those of the longest key that names its
// registry, or its registry and a path its repository falls under.
func (k keychain) Resolve(r authn.Resource) (authn.Authenticator, error) {
	target := normalize(r.RegistryStr())
	if repo, ok := r.(interface{ RepositoryStr() string }); ok {
		target += "/" + repo.RepositoryStr()
	}
	best := ""
	for key := range k {
		if (target == key || strings.HasPrefix(target, key+"/")) && len(key) > len(best) {
			best = key
		}
	}
	if best == "" {
		return authn.Anonymous, nil
	}
	return authn.FromConfig(k[best]), nil
}
