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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
)

// testConfig reaches the tests' API server.
var testConfig *rest.Config

// testClient is a client of the test's API server, which reads uncached.
var testClient client.Client

// testScheme knows the kinds the tests create.
var testScheme = runtime.NewScheme()

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

// run starts an API server with Cluster API's core CRDs and the providers' own, runs the tests
// and stops it.
func run(m *testing.M) int {
	for _, add := range []func(*runtime.Scheme) error{
		clientgoscheme.AddToScheme, clusterv1.AddToScheme, bootstrapv1.AddToScheme, controlplanev1.AddToScheme,
	} {
		if err := add(testScheme); err != nil {
			panic(err)
		}
	}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/cluster-api").Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "find the cluster-api module: %v\n", err)
		return 1
	}
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(strings.TrimSpace(string(out)), "core", "config", "crd", "bases"),
			filepath.Join("..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "..", "controlplane", "config", "crd", "bases"),
		},
		ErrorIfCRDPathMissing: true,
		Scheme:                testScheme,
	}
	testConfig, err = env.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start the API server: %v\n", err)
		return 1
	}
	defer func() { _ = env.Stop() }()
	testClient, err = client.New(testConfig, client.Options{Scheme: testScheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to the API server: %v\n", err)
		return 1
	}
	return m.Run()
}
