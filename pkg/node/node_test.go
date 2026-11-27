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
	"fmt"
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
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node/nodetest"
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

// A control plane that does not answer is one to move on from.
func TestRemovalCallsToAnUnreachableNode(t *testing.T) {
	s := generate(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	c := node.New()
	for call, err := range map[string]error{
		"EtcdLeave":        c.EtcdLeave(ctx, "127.0.0.1:1", s, "cp1"),
		"EtcdRemoveMember": c.EtcdRemoveMember(ctx, "127.0.0.1:1", s, "cp1", "cp2"),
	} {
		if !node.Unreachable(err) || !node.TryNextControlPlane(err) {
			t.Errorf("%s at a closed port = %v, want it unreachable", call, err)
		}
	}
	if _, err := c.RevokeNode(ctx, "127.0.0.1:1", s, "cp1", "w1", false); !node.Unreachable(err) {
		t.Errorf("RevokeNode at a closed port = %v, want it unreachable", err)
	}
}

// The removal calls reach the installed node by its name.
func TestRemovalCallsCheckTheName(t *testing.T) {
	s := generate(t)
	n := serveInstalled(t, s, "cp-1")
	c := node.New()
	ctx := context.Background()
	if err := c.EtcdLeave(ctx, n.Addr, s, "cp-2"); !errors.Is(err, sdk.ErrOtherNode) || !node.Unreachable(err) {
		t.Errorf("EtcdLeave as cp-2 = %v, want ErrOtherNode", err)
	}
	if _, err := c.EtcdMembers(ctx, n.Addr, s, "cp-2"); !errors.Is(err, sdk.ErrOtherNode) {
		t.Errorf("EtcdMembers as cp-2 = %v, want ErrOtherNode", err)
	}
	if _, err := c.PruneRevoked(ctx, n.Addr, s, "cp-2"); !errors.Is(err, sdk.ErrOtherNode) {
		t.Errorf("PruneRevoked as cp-2 = %v, want ErrOtherNode", err)
	}
}

// Another control plane may answer where one is unreachable or no etcd member, but not where it
// refused for etcd's quorum or a full revoked list.
func TestTryNextControlPlane(t *testing.T) {
	for err, want := range map[error]bool{
		nodetest.Unreachable("10.0.0.1"):          true,
		nodetest.EtcdUnavailable():                true,
		nodetest.NotEtcdMember():                  true,
		nodetest.QuorumRefused("cp2"):             false,
		nodetest.RevokedListFull("w1"):            false,
		context.DeadlineExceeded:                  true,
		fmt.Errorf("x: %w", sdk.ErrNotInstalled):  true,
		errors.New("the image has no such layer"): false,
	} {
		if got := node.TryNextControlPlane(err); got != want {
			t.Errorf("TryNextControlPlane(%v) = %v, want %v", err, got, want)
		}
	}
	if node.TryNextControlPlane(nil) {
		t.Error("TryNextControlPlane(nil) = true")
	}
}

// A node that answers was reached, also when it answers that its own etcd is unavailable.
func TestUnreachable(t *testing.T) {
	for err, want := range map[error]bool{
		nodetest.Unreachable("10.0.0.1"):          true,
		nodetest.EtcdUnavailable():                false,
		context.DeadlineExceeded:                  true,
		fmt.Errorf("x: %w", sdk.ErrNotInstalled):  true,
		fmt.Errorf("x: %w", sdk.ErrOtherNode):     true,
		nodetest.NotEtcdMember():                  false,
		errors.New("the image has no such layer"): false,
	} {
		if got := node.Unreachable(err); got != want {
			t.Errorf("Unreachable(%v) = %v, want %v", err, got, want)
		}
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

// A removal that names no member is refused before any call, at the node and at the Fake.
func TestRemoveNoMember(t *testing.T) {
	var s clustersecrets.Secrets
	for name, c := range map[string]node.Client{"node": node.New(), "fake": nodetest.New()} {
		if err := c.EtcdRemoveMember(t.Context(), "127.0.0.1:1", s, "cp1", ""); !errors.Is(err, node.ErrNoMember) {
			t.Errorf("%s: EtcdRemoveMember of no member = %v, want ErrNoMember", name, err)
		}
	}
}
