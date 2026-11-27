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

package nodetest

import (
	"context"
	"errors"
	"slices"
	"testing"

	"connectrpc.com/connect/v2"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	"github.com/trevex/chalkos/pkg/client"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/sdk"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// The Fake fails as the SDK does while a machine moves from maintenance mode to installed, and an
// install hook may install the machine.
func TestFakeFollowsTheMachine(t *testing.T) {
	ctx := context.Background()
	var s clustersecrets.Secrets
	f := New()
	f.Set("10.0.0.1:50000", Node{Fingerprint: "ab:cd"})
	f.Update("10.0.0.1:50000", func(n *Node) {
		n.Install = func(sdk.InstallRequest) error {
			f.Update("10.0.0.1:50000", func(n *Node) { n.Name = "cp-1" })
			return nil
		}
	})
	if _, err := f.Installed(ctx, "10.0.0.1:50000", s, "cp-1"); !errors.Is(err, sdk.ErrNotInstalled) {
		t.Errorf("Installed in maintenance mode = %v, want ErrNotInstalled", err)
	}
	m, err := f.Maintenance(ctx, "10.0.0.1:50000", s)
	if err != nil {
		t.Fatal(err)
	}
	if m.Fingerprint() != "ab:cd" {
		t.Errorf("fingerprint %s", m.Fingerprint())
	}
	if err := m.Install(ctx, sdk.InstallRequest{Node: "cp-1"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.Node("10.0.0.1:50000"); len(n.Installs) != 1 || n.Installs[0].Node != "cp-1" {
		t.Errorf("installs %v, want the one of cp-1", n.Installs)
	}
	if _, err := f.Maintenance(ctx, "10.0.0.1:50000", s); !errors.Is(err, sdk.ErrInstalled) {
		t.Errorf("Maintenance once installed = %v, want ErrInstalled", err)
	}
	if _, err := f.Installed(ctx, "10.0.0.1:50000", s, "cp-2"); !errors.Is(err, sdk.ErrOtherNode) {
		t.Errorf("Installed as cp-2 = %v, want ErrOtherNode", err)
	}
	if err := f.Bootstrap(ctx, "10.0.0.1:50000", s, "cp-1"); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.Node("10.0.0.1:50000"); n.Bootstraps != 1 {
		t.Errorf("%d bootstraps, want 1", n.Bootstraps)
	}
}

// The Fake records leaves, removals, revocations and prunes, answers them with the node's hooks,
// and fails them as sdk.DialInstalled does for a node that is not installed as the name.
func TestFakeRemovesNodes(t *testing.T) {
	ctx := context.Background()
	var s clustersecrets.Secrets
	f := New()
	members := []*kubernetesv1.EtcdMember{{Id: 0x1a, Name: "cp1"}, {Id: 0x2b, Name: "cp2"}, {Id: 0x3c, Name: "cp3"}}
	f.Set("10.0.0.1", Node{Name: "cp1", Members: members})
	f.Set("10.0.0.2", Node{Name: "cp2", EtcdLeave: func() error { return QuorumRefused("cp2") }})
	f.Set("10.0.0.3", Node{Name: "cp3", Members: members})
	if err := f.EtcdLeave(ctx, "10.0.0.2", s, "cp2"); connect.CodeOf(err) != connect.CodeFailedPrecondition || node.TryNextControlPlane(err) {
		t.Errorf("EtcdLeave refused by the hook = %v, want the quorum refusal", err)
	}
	if _, err := f.RevokeNode(ctx, "10.0.0.1", s, "cp1", "cp2", false); !errors.As(err, new(*sdkkubernetes.EtcdMemberError)) {
		t.Errorf("RevokeNode of an etcd member = %v, want an EtcdMemberError", err)
	}
	if err := f.EtcdRemoveMember(ctx, "10.0.0.1", s, "cp1", "cp1"); connect.CodeOf(err) != connect.CodeFailedPrecondition || !client.FromNode(err) {
		t.Errorf("EtcdRemoveMember of the node's own member = %v, want the node's refusal", err)
	}
	if err := f.EtcdRemoveMember(ctx, "10.0.0.1", s, "cp1", "cp2"); err != nil {
		t.Fatal(err)
	}
	if err := f.EtcdRemoveMember(ctx, "10.0.0.3", s, "cp3", "cp2"); connect.CodeOf(err) != connect.CodeNotFound || !client.FromNode(err) {
		t.Errorf("EtcdRemoveMember of a gone member through another node = %v, want the node's NotFound", err)
	}
	if resp, err := f.RevokeNode(ctx, "10.0.0.1", s, "cp1", "w1", false); err != nil || resp.GetRevokedAt() == nil {
		t.Fatalf("RevokeNode = %v, %v", resp, err)
	}
	if resp, err := f.PruneRevoked(ctx, "10.0.0.1", s, "cp1"); err != nil || resp.GetListCapacity() == 0 {
		t.Errorf("PruneRevoked = %v, %v", resp, err)
	}
	if err := f.EtcdLeave(ctx, "10.0.0.1", s, "cp1"); err != nil {
		t.Fatal(err)
	}
	if m, err := f.EtcdMembers(ctx, "10.0.0.3", s, "cp3"); err != nil || len(m) != 1 || m[0].GetName() != "cp3" {
		t.Errorf("EtcdMembers after the leave and the removal = %v, %v, want cp3", m, err)
	}
	if _, err := f.EtcdMembers(ctx, "10.0.0.1", s, "cp1"); !sdkkubernetes.NotEtcdMember(err) {
		t.Errorf("EtcdMembers of a node that left = %v, want NotEtcdMember", err)
	}
	if err := f.EtcdRemoveMember(ctx, "10.0.0.1", s, "cp1", "cp3"); !sdkkubernetes.NotEtcdMember(err) {
		t.Errorf("EtcdRemoveMember through a node that left = %v, want NotEtcdMember", err)
	}
	if err := f.EtcdLeave(ctx, "10.0.0.1", s, "cp1"); !sdkkubernetes.NotEtcdMember(err) {
		t.Errorf("EtcdLeave again = %v, want NotEtcdMember", err)
	}
	n, _ := f.Node("10.0.0.1")
	if n.Leaves != 2 || !n.Left || !slices.Equal(n.Removed, []string{"cp2"}) || !slices.Equal(n.Revoked, []string{"w1"}) || n.Prunes != 1 {
		t.Errorf("recorded %d leaves, left %v, removed %v, revoked %v, %d prunes", n.Leaves, n.Left, n.Removed, n.Revoked, n.Prunes)
	}
	if err := f.EtcdLeave(ctx, "10.0.0.1", s, "cp2"); !errors.Is(err, sdk.ErrOtherNode) {
		t.Errorf("EtcdLeave as another name = %v, want ErrOtherNode", err)
	}
	if err := f.EtcdLeave(ctx, "10.0.0.9", s, "cp9"); !node.Unreachable(err) || !node.TryNextControlPlane(err) {
		t.Errorf("EtcdLeave at a missing address = %v, want it unreachable", err)
	}
}

// A learner that has not started has no name: etcd finds it only by its ID in hex.
func TestFakeRemovesANamelessLearner(t *testing.T) {
	ctx := context.Background()
	var s clustersecrets.Secrets
	f := New()
	f.Set("10.0.0.1", Node{Name: "cp1", Members: []*kubernetesv1.EtcdMember{
		{Id: 0x1a, Name: "cp1"},
		{Id: 0xbeef, Learner: true, PeerUrls: []string{"https://10.0.0.3:2380"}},
	}})
	if err := f.EtcdRemoveMember(ctx, "10.0.0.1", s, "cp1", "cp3"); connect.CodeOf(err) != connect.CodeNotFound {
		t.Errorf("EtcdRemoveMember by the learner's node name = %v, want NotFound", err)
	}
	if m, _ := f.EtcdMembers(ctx, "10.0.0.1", s, "cp1"); len(m) != 2 {
		t.Errorf("members %v, want the learner still listed", m)
	}
	if err := f.EtcdRemoveMember(ctx, "10.0.0.1", s, "cp1", "BEEF"); err != nil {
		t.Fatal(err)
	}
	if n, _ := f.Node("10.0.0.1"); len(n.Members) != 1 || !slices.Equal(n.Removed, []string{"BEEF"}) {
		t.Errorf("members %v, removed %v, want the learner removed by its ID", n.Members, n.Removed)
	}
}

// Hooks answer in place of the Fake's model, and a refusal is not recorded.
func TestFakeHooksRefuse(t *testing.T) {
	ctx := context.Background()
	var s clustersecrets.Secrets
	f := New()
	f.Set("10.0.0.1", Node{
		Name:             "cp1",
		EtcdRemoveMember: func(member string) error { return QuorumRefused(member) },
		RevokeNode: func(node string, _ bool) (*kubernetesv1.RevokeNodeResponse, error) {
			return nil, RevokedListFull(node)
		},
		PruneRevoked: func() (*kubernetesv1.PruneRevokedResponse, error) {
			return &kubernetesv1.PruneRevokedResponse{Pruned: []string{"w0"}, ListLength: 10, ListCapacity: 90_000}, nil
		},
	})
	if err := f.EtcdRemoveMember(ctx, "10.0.0.1", s, "cp1", "cp2"); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("EtcdRemoveMember = %v, want the quorum refusal", err)
	}
	if _, err := f.RevokeNode(ctx, "10.0.0.1", s, "cp1", "w1", false); !sdkkubernetes.RevokedListFull(err) {
		t.Errorf("RevokeNode = %v, want RevokedListFull", err)
	}
	if resp, err := f.PruneRevoked(ctx, "10.0.0.1", s, "cp1"); err != nil || !slices.Equal(resp.GetPruned(), []string{"w0"}) {
		t.Errorf("PruneRevoked = %v, %v, want w0 pruned", resp, err)
	}
	if n, _ := f.Node("10.0.0.1"); len(n.Removed) != 0 || len(n.Revoked) != 0 || n.Prunes != 1 {
		t.Errorf("recorded removed %v, revoked %v, %d prunes", n.Removed, n.Revoked, n.Prunes)
	}
}

// The errors are the node's answers, which the SDK's tests tell apart.
func TestErrors(t *testing.T) {
	if err := NotEtcdMember(); !sdkkubernetes.NotEtcdMember(err) || !client.FromNode(err) || node.Unreachable(err) {
		t.Errorf("NotEtcdMember() = %v", err)
	}
	if err := QuorumRefused("cp2"); !client.FromNode(err) || node.TryNextControlPlane(err) {
		t.Errorf("QuorumRefused = %v", err)
	}
	if err := Unreachable("10.0.0.1"); client.FromNode(err) || !node.Unreachable(err) {
		t.Errorf("Unreachable = %v", err)
	}
}
