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
	"encoding/base64"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
)

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

// The keychain answers a repository's credentials by the longest key it falls under, names Docker
// Hub by any of its names, and answers anonymous credentials for other registries.
func TestKeychainOf(t *testing.T) {
	s := &corev1.Secret{
		Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths": {` +
			`"https://registry.example/v1/": ` + auth("registry") + `, ` +
			`"registry.example/team": ` + auth("team") + `, ` +
			`"docker.io": ` + auth("hub") + `, ` +
			`"https://index.docker.io/v1/": ` + auth("index") + `}}`)},
	}
	k, err := keychainOf(s)
	if err != nil {
		t.Fatal(err)
	}
	for repository, want := range map[string]string{
		"registry.example/chalkos":      "registry",
		"registry.example/team/chalkos": "team",
		"registry.example/teams/x":      "registry",
		"library/chalkos":               "hub",
		"registry-1.docker.io/x/y":      "hub",
		"other.example/chalkos":         "",
	} {
		if got := username(t, k, repository); got != want {
			t.Errorf("%s: user %q, want %q", repository, got, want)
		}
	}
	if _, err := keychainOf(&corev1.Secret{Type: corev1.SecretTypeOpaque}); err == nil {
		t.Error("an Opaque Secret read as a docker config")
	}
}
