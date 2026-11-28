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

// Package nodetest is an in-memory node.Client for the controllers' tests.
package nodetest

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"

	"connectrpc.com/connect/v2"
	"github.com/google/go-containerregistry/pkg/authn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/sdk"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// Node is a machine the Fake serves at one address.
type Node struct {
	// Name is the name the machine is installed as; empty while it runs in maintenance mode.
	Name string
	// Fingerprint is that of the certificate the machine presents in maintenance mode.
	Fingerprint string
	// RootHash is the verity root hash, in hex, of the image the machine runs once installed.
	RootHash string
	// Err, when set, fails every call to the machine, as an unreachable one does.
	Err error
	// Install, when set, answers an install in place, which succeeds otherwise. The Fake calls it
	// without holding its lock, so it may call the Fake, for instance to install the machine.
	Install func(sdk.InstallRequest) error
	// Bootstrap, when set, answers a bootstrap, which succeeds otherwise. The Fake calls it
	// without holding its lock.
	Bootstrap func() error
	// Status is the node's Kubernetes status, which KubernetesStatus answers a copy of; nil answers
	// an empty one.
	Status *kubernetesv1.StatusResponse
	// EtcdLeave, when set, answers an EtcdLeave of the node in place of the Fake's model of etcd.
	// The Fake calls it without holding its lock.
	EtcdLeave func() error
	// EtcdRemoveMember, when set, answers an EtcdRemoveMember through the node in place of the
	// Fake's model of etcd. The Fake calls it without holding its lock.
	EtcdRemoveMember func(member string) error
	// Members are the etcd members the node lists; nil when the node is no etcd member, which
	// then refuses EtcdMembers and EtcdRemoveMember with NotEtcdMember, as chalkd does.
	//
	// Without hooks, the Fake models etcd with them: a removal finds the member by name or by ID
	// in hex, as etcd does, refuses the node's own, drops it from every node's Members, and fails
	// with connect.CodeNotFound when there is none; a learner that has not started has no name. A
	// leave drops the node's member from every node's Members, sets Left, and clears the node's
	// own Members; it is refused while the node's member is etcd's only voter, as chalkd's quorum
	// guard refuses it.
	Members []*kubernetesv1.EtcdMember
	// Left is set once the node left etcd; a later EtcdLeave is refused with NotEtcdMember, as
	// chalkd refuses it.
	Left bool
	// RevokeNode, when set, answers a RevokeNode through the node, which otherwise succeeds unless
	// the node to revoke is among Members and allowControlPlane is false. The Fake calls it without
	// holding its lock.
	RevokeNode func(node string, allowControlPlane bool) (*kubernetesv1.RevokeNodeResponse, error)
	// PruneRevoked, when set, answers a PruneRevoked through the node, which prunes nothing
	// otherwise. The Fake calls it without holding its lock.
	PruneRevoked func() (*kubernetesv1.PruneRevokedResponse, error)

	// Installs are the install requests the machine received, in order.
	Installs []sdk.InstallRequest
	// Bootstraps counts the bootstraps the node received.
	Bootstraps int
	// Leaves counts the EtcdLeave calls the node received.
	Leaves int
	// Removed are the members removed through the node, in order, as the calls named them.
	Removed []string
	// Revoked are the nodes revoked through the node, in order.
	Revoked []string
	// Prunes counts the PruneRevoked calls the node answered.
	Prunes int
}

// Fake is a node.Client of machines and images in memory, keyed by address and by reference. It
// is safe for concurrent use.
type Fake struct {
	mu     sync.Mutex
	nodes  map[string]*Node
	images map[string]*ociimage.Artifact
}

var _ node.Client = (*Fake)(nil)

// New returns a Fake without machines or images.
func New() *Fake {
	return &Fake{nodes: map[string]*Node{}, images: map[string]*ociimage.Artifact{}}
}

// Set serves n at addr, replacing the machine there.
func (f *Fake) Set(addr string, n Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[addr] = &n
}

// Update changes the machine at addr, adding one in maintenance mode when there is none.
func (f *Fake) Update(addr string, change func(*Node)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.nodes[addr]
	if !ok {
		n = &Node{}
		f.nodes[addr] = n
	}
	change(n)
}

// Node returns a copy of the machine at addr; ok is false when there is none.
func (f *Fake) Node(addr string) (n Node, ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.nodes[addr]
	if !ok {
		return Node{}, false
	}
	n = *p
	n.Installs = slices.Clone(p.Installs)
	n.Members = cloneMembers(p.Members)
	n.Removed = slices.Clone(p.Removed)
	n.Revoked = slices.Clone(p.Revoked)
	return n, true
}

// SetImage serves the artifact a at ref.
func (f *Fake) SetImage(ref string, a *ociimage.Artifact) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.images[ref] = a
}

// reach returns the machine at addr, or the error a call to it fails with.
func (f *Fake) reach(addr string) (*Node, error) {
	n, ok := f.nodes[addr]
	switch {
	case !ok:
		return nil, Unreachable(addr)
	case n.Err != nil:
		return nil, n.Err
	}
	return n, nil
}

// installed returns the machine at addr installed as name, failing as sdk.DialInstalled does.
func (f *Fake) installed(addr, name string) (*Node, error) {
	n, err := f.reach(addr)
	switch {
	case err != nil:
		return nil, err
	case n.Name == "":
		return nil, fmt.Errorf("%s: %w", addr, sdk.ErrNotInstalled)
	case n.Name != name:
		return nil, fmt.Errorf("%w: the node at %s is %s, not %s", sdk.ErrOtherNode, addr, n.Name, name)
	}
	return n, nil
}

// Maintenance returns the machine at addr while it runs in maintenance mode.
func (f *Fake) Maintenance(_ context.Context, addr string, _ clustersecrets.Secrets) (node.Maintenance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.reach(addr)
	switch {
	case err != nil:
		return nil, err
	case n.Name != "":
		return nil, sdk.ErrInstalled
	}
	return maintenance{f: f, addr: addr, fingerprint: n.Fingerprint}, nil
}

// Installed returns the Info of the machine at addr installed as name.
func (f *Fake) Installed(_ context.Context, addr string, _ clustersecrets.Secrets, name string) (*nodev1.InfoResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.installed(addr, name)
	if err != nil {
		return nil, err
	}
	rootHash, _ := hex.DecodeString(n.RootHash)
	return &nodev1.InfoResponse{Mode: nodev1.Mode_MODE_NORMAL, NodeName: name, RootHash: rootHash}, nil
}

// Image returns the artifact SetImage serves at ref.
func (f *Fake) Image(_ context.Context, ref string, _ authn.Keychain) (*ociimage.Artifact, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.images[ref]
	if !ok {
		return nil, fmt.Errorf("read %s: MANIFEST_UNKNOWN", ref)
	}
	return a, nil
}

// Bootstrap records a bootstrap of the machine at addr installed as name and answers it.
func (f *Fake) Bootstrap(_ context.Context, addr string, _ clustersecrets.Secrets, name string) error {
	f.mu.Lock()
	n, err := f.installed(addr, name)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	n.Bootstraps++
	hook := n.Bootstrap
	f.mu.Unlock()
	if hook != nil {
		return hook()
	}
	return nil
}

// KubernetesStatus returns the Status of the machine at addr installed as name.
func (f *Fake) KubernetesStatus(_ context.Context, addr string, _ clustersecrets.Secrets, name string) (*kubernetesv1.StatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.installed(addr, name)
	if err != nil {
		return nil, err
	}
	if n.Status == nil {
		return &kubernetesv1.StatusResponse{}, nil
	}
	return proto.CloneOf(n.Status), nil
}

// EtcdLeave records a leave of the machine at addr installed as name and answers it.
func (f *Fake) EtcdLeave(_ context.Context, addr string, _ clustersecrets.Secrets, name string) error {
	f.mu.Lock()
	n, err := f.installed(addr, name)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	n.Leaves++
	hook := n.EtcdLeave
	if hook == nil {
		defer f.mu.Unlock()
		if n.Left {
			return NotEtcdMember()
		}
		if lastVoter(n.Members, name) {
			return refusal(connect.CodeFailedPrecondition, "%s is etcd's last voter", name)
		}
		f.dropMembers(func(m *kubernetesv1.EtcdMember) bool { return m.GetName() == name })
		n.Left, n.Members = true, nil
		return nil
	}
	f.mu.Unlock()
	return hook()
}

// EtcdRemoveMember removes member through the machine at addr installed as name and records it;
// a refused removal is not recorded.
func (f *Fake) EtcdRemoveMember(_ context.Context, addr string, _ clustersecrets.Secrets, name, member string) error {
	if member == "" {
		return node.ErrNoMember
	}
	f.mu.Lock()
	n, err := f.installed(addr, name)
	if err != nil {
		f.mu.Unlock()
		return err
	}
	hook := n.EtcdRemoveMember
	if hook == nil {
		defer f.mu.Unlock()
		if n.Members == nil {
			return NotEtcdMember()
		}
		i := slices.IndexFunc(n.Members, func(m *kubernetesv1.EtcdMember) bool {
			return m.GetName() == member || fmt.Sprintf("%x", m.GetId()) == strings.ToLower(member)
		})
		switch {
		case i < 0:
			return refusal(connect.CodeNotFound, "etcd has no member %s", member)
		case n.Members[i].GetName() == name:
			return refusal(connect.CodeFailedPrecondition, "%s is this node's own member; chalkctl etcd leave %s takes the node out of etcd", name, name)
		}
		id := n.Members[i].GetId()
		f.dropMembers(func(m *kubernetesv1.EtcdMember) bool { return m.GetId() == id })
		n.Removed = append(n.Removed, member)
		return nil
	}
	f.mu.Unlock()
	if err := hook(member); err != nil {
		return err
	}
	f.Update(addr, func(n *Node) { n.Removed = append(n.Removed, member) })
	return nil
}

// lastVoter tells whether the member name is the only voter among members.
func lastVoter(members []*kubernetesv1.EtcdMember, name string) bool {
	self := false
	for _, m := range members {
		switch {
		case m.GetLearner():
		case m.GetName() == name:
			self = true
		default:
			return false
		}
	}
	return self
}

// dropMembers drops the members gone from etcd from every node's Members. The caller holds f.mu.
func (f *Fake) dropMembers(gone func(*kubernetesv1.EtcdMember) bool) {
	for _, n := range f.nodes {
		if n.Members != nil {
			n.Members = slices.DeleteFunc(slices.Clone(n.Members), gone)
		}
	}
}

// EtcdMembers returns a copy of the members of the machine at addr installed as name; a node
// without Members is no etcd member.
func (f *Fake) EtcdMembers(_ context.Context, addr string, _ clustersecrets.Secrets, name string) ([]*kubernetesv1.EtcdMember, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.installed(addr, name)
	if err != nil {
		return nil, err
	}
	if n.Members == nil {
		return nil, NotEtcdMember()
	}
	return cloneMembers(n.Members), nil
}

// RevokeNode revokes node through the machine at addr installed as name and records it; a refused
// revocation is not recorded.
func (f *Fake) RevokeNode(_ context.Context, addr string, _ clustersecrets.Secrets, name, node string, allowControlPlane bool) (*kubernetesv1.RevokeNodeResponse, error) {
	f.mu.Lock()
	n, err := f.installed(addr, name)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	hook := n.RevokeNode
	member := slices.ContainsFunc(n.Members, func(m *kubernetesv1.EtcdMember) bool { return m.GetName() != "" && m.GetName() == node })
	f.mu.Unlock()
	resp := &kubernetesv1.RevokeNodeResponse{RevokedAt: timestamppb.Now(), NodeDeleted: true, ListLength: 100, ListCapacity: 90_000}
	switch {
	case hook != nil:
		if resp, err = hook(node, allowControlPlane); err != nil {
			return nil, err
		}
	case member && !allowControlPlane:
		return nil, &sdkkubernetes.EtcdMemberError{Node: node}
	}
	f.Update(addr, func(n *Node) { n.Revoked = append(n.Revoked, node) })
	return resp, nil
}

// PruneRevoked counts a prune through the machine at addr installed as name and answers it.
func (f *Fake) PruneRevoked(_ context.Context, addr string, _ clustersecrets.Secrets, name string) (*kubernetesv1.PruneRevokedResponse, error) {
	f.mu.Lock()
	n, err := f.installed(addr, name)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	n.Prunes++
	hook := n.PruneRevoked
	f.mu.Unlock()
	if hook != nil {
		return hook()
	}
	return &kubernetesv1.PruneRevokedResponse{ListLength: 100, ListCapacity: 90_000}, nil
}

// cloneMembers is a deep copy of members.
func cloneMembers(members []*kubernetesv1.EtcdMember) []*kubernetesv1.EtcdMember {
	if members == nil {
		return nil
	}
	out := make([]*kubernetesv1.EtcdMember, len(members))
	for i, m := range members {
		out[i] = proto.CloneOf(m)
	}
	return out
}

// maintenance is the machine at addr as Maintenance returned it.
type maintenance struct {
	f           *Fake
	addr        string
	fingerprint string
}

func (m maintenance) Fingerprint() string { return m.fingerprint }

// Install records the request and answers it with the machine's Install hook.
func (m maintenance) Install(_ context.Context, r sdk.InstallRequest) error {
	m.f.mu.Lock()
	n, err := m.f.reach(m.addr)
	if err != nil {
		m.f.mu.Unlock()
		return err
	}
	n.Installs = append(n.Installs, r)
	hook := n.Install
	m.f.mu.Unlock()
	if hook != nil {
		return hook(r)
	}
	return nil
}

func (maintenance) Close() {}
