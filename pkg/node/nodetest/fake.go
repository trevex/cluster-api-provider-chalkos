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
	"fmt"
	"slices"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"google.golang.org/protobuf/proto"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/sdk"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// Node is a machine the Fake serves at one address.
type Node struct {
	// Name is the name the machine is installed as; empty while it runs in maintenance mode.
	Name string
	// Fingerprint is that of the certificate the machine presents in maintenance mode.
	Fingerprint string
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

	// Installs are the install requests the machine received, in order.
	Installs []sdk.InstallRequest
	// Bootstraps counts the bootstraps the node received.
	Bootstraps int
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
		return nil, fmt.Errorf("dial %s: connection refused", addr)
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
	if _, err := f.installed(addr, name); err != nil {
		return nil, err
	}
	return &nodev1.InfoResponse{Mode: nodev1.Mode_MODE_NORMAL, NodeName: name}, nil
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
