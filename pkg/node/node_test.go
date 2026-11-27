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

package node_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
	"github.com/trevex/chalkos/pkg/chalkd"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/config"
	"github.com/trevex/chalkos/pkg/install"
	"github.com/trevex/chalkos/pkg/pki"
	"github.com/trevex/chalkos/pkg/sdk"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"
	"github.com/trevex/chalkos/pkg/sdk/sdktest"

	"github.com/trevex/cluster-api-provider-chalkos/internal/chalkostest"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

func generate(t *testing.T) clustersecrets.Secrets {
	t.Helper()
	s, err := clustersecrets.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// serveMaintenance serves a node in maintenance mode whose image trusts the OS CA of s.
func serveMaintenance(t *testing.T, s clustersecrets.Secrets, server *chalkd.Server) sdktest.Node {
	t.Helper()
	chalkostest.Chdir(t)
	return sdktest.Serve(t, server, s.OSCA, pki.CertKey{}, "", nil)
}

// serveInstalled serves the node name, installed into the cluster of s.
func serveInstalled(t *testing.T, s clustersecrets.Secrets, name string) sdktest.Node {
	t.Helper()
	chalkostest.Chdir(t)
	return sdktest.Serve(t, &chalkd.Server{Mode: nodev1.Mode_MODE_NORMAL}, s.OSCA, s.NodeCA, name, nil)
}

func TestMaintenanceReturnsTheFingerprint(t *testing.T) {
	s := generate(t)
	n := serveMaintenance(t, s, sdktest.Maintenance())
	m, err := node.New().Maintenance(context.Background(), n.Addr, s)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.Fingerprint() != n.Fingerprint {
		t.Errorf("fingerprint %s, want %s", m.Fingerprint(), n.Fingerprint)
	}
}

func TestMaintenanceAtAnInstalledNode(t *testing.T) {
	s := generate(t)
	n := serveInstalled(t, s, "cp-1")
	if _, err := node.New().Maintenance(context.Background(), n.Addr, s); !errors.Is(err, sdk.ErrInstalled) {
		t.Errorf("Maintenance = %v, want ErrInstalled", err)
	}
}

func TestInstallSendsTheNodeItsConfig(t *testing.T) {
	s := generate(t)
	var got install.Request
	server := sdktest.Maintenance()
	server.InPlace = func(_ context.Context, r install.Request, _ []byte) error { got = r; return nil }
	n := serveMaintenance(t, s, server)
	m, err := node.New().Maintenance(context.Background(), n.Addr, s)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	docs, err := config.Parse(strings.NewReader("apiVersion: storage.chalkos.dev/v1alpha1\nkind: SystemDisk\nmetadata: {name: system}\nspec: {disk: {serial: chalk-target}}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Install(context.Background(), sdk.InstallRequest{Node: "w-1", Documents: docs, Secrets: s}); err != nil {
		t.Fatal(err)
	}
	if string(got.CA) != s.OSCABundle() {
		t.Error("the node got another OS CA bundle")
	}
}

func TestInstallRefusal(t *testing.T) {
	s := generate(t)
	n := serveMaintenance(t, s, sdktest.Maintenance())
	m, err := node.New().Maintenance(context.Background(), n.Addr, s)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	docs, err := config.Parse(strings.NewReader("apiVersion: kubernetes.chalkos.dev/v1alpha1\nkind: Node\nmetadata: {name: default}\nspec: {type: bogus}\n"))
	if err != nil {
		t.Fatal(err)
	}
	err = m.Install(context.Background(), sdk.InstallRequest{Node: "w-1", Documents: docs, Secrets: s})
	if !sdk.Refused(err) {
		t.Errorf("Install = %v, want a refusal", err)
	}
}

func TestInstalled(t *testing.T) {
	s := generate(t)
	n := serveInstalled(t, s, "cp-1")
	c := node.New()
	info, err := c.Installed(context.Background(), n.Addr, s, "cp-1")
	if err != nil {
		t.Fatal(err)
	}
	if info.NodeName != "cp-1" {
		t.Errorf("node name %q, want cp-1", info.NodeName)
	}
	if _, err := c.Installed(context.Background(), n.Addr, s, "cp-2"); !errors.Is(err, sdk.ErrOtherNode) {
		t.Errorf("Installed as cp-2 = %v, want ErrOtherNode", err)
	}
}

func TestInstalledAtAMaintenanceNode(t *testing.T) {
	s := generate(t)
	n := serveMaintenance(t, s, sdktest.Maintenance())
	if _, err := node.New().Installed(context.Background(), n.Addr, s, "cp-1"); !errors.Is(err, sdk.ErrNotInstalled) {
		t.Errorf("Installed = %v, want ErrNotInstalled", err)
	}
}

// The Kubernetes calls reach the installed node by its name; a node without the Kubernetes layer
// says so.
func TestKubernetesCalls(t *testing.T) {
	s := generate(t)
	n := serveInstalled(t, s, "cp-1")
	c := node.New()
	if _, err := c.KubernetesStatus(context.Background(), n.Addr, s, "cp-1"); !errors.Is(err, sdkkubernetes.ErrNoKubernetes) {
		t.Errorf("KubernetesStatus = %v, want ErrNoKubernetes", err)
	}
	if err := c.Bootstrap(context.Background(), n.Addr, s, "cp-2"); !errors.Is(err, sdk.ErrOtherNode) {
		t.Errorf("Bootstrap as cp-2 = %v, want ErrOtherNode", err)
	}
}

// A machine whose image trusts another OS CA refuses the controllers' certificate.
func TestMaintenanceAtAMachineOfAnotherCluster(t *testing.T) {
	s, other := generate(t), generate(t)
	n := serveMaintenance(t, other, sdktest.Maintenance())
	if _, err := node.New().Maintenance(context.Background(), n.Addr, s); !errors.Is(err, sdk.ErrOSCARefused) {
		t.Errorf("Maintenance = %v, want ErrOSCARefused", err)
	}
}
