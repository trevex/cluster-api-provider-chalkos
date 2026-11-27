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

// Package node is what the controllers ask of chalkos nodes and images, over the chalkos SDK.
// Its errors are the SDK's, unwrapped, so callers test them with errors.Is against
// sdk.ErrInstalled, sdk.ErrNotInstalled, sdk.ErrOtherNode and sdk.ErrOSCARefused, and with
// sdk.Refused.
package node

import (
	"context"
	"errors"
	"slices"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/google/go-containerregistry/pkg/authn"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
	"github.com/trevex/chalkos/pkg/client"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	chalkoslayers "github.com/trevex/chalkos/pkg/layers"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"
)

// ClientName is the name the controllers' admin certificates carry, which chalkd's logs and
// errors show.
const ClientName = "cluster-api-provider-chalkos"

// Client is what the controllers ask of chalkos nodes and images.
type Client interface {
	// Maintenance connects to the machine at addr in maintenance mode, trusting its certificate
	// on first use, and returns it pinned to that certificate. A machine in normal mode fails
	// with sdk.ErrInstalled.
	Maintenance(ctx context.Context, addr string, s clustersecrets.Secrets) (Maintenance, error)
	// Installed connects to the node name at addr, installed into the cluster of s, and returns
	// its Info.
	Installed(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*nodev1.InfoResponse, error)
	// Image reads the image ref names, by digest, from its registry with the credentials kc
	// resolves: its header, contract and metadata.
	Image(ctx context.Context, ref string, kc authn.Keychain) (*ociimage.Artifact, error)
	// Bootstrap starts the cluster on its first control plane, the node name at addr.
	Bootstrap(ctx context.Context, addr string, s clustersecrets.Secrets, name string) error
	// KubernetesStatus is the Kubernetes status of the node name at addr; a node whose image has no
	// Kubernetes layer fails with sdkkubernetes.ErrNoKubernetes.
	KubernetesStatus(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*kubernetesv1.StatusResponse, error)
	// EtcdLeave takes the control plane name at addr out of etcd. It is refused while the voters
	// left would have no healthy quorum; a node that is no etcd member, or left etcd already,
	// refuses with the NotEtcdMember detail, which sdkkubernetes.NotEtcdMember tells. Only that
	// node can leave: another control plane answering in its place would be wrong, so callers
	// do not move on to the next control plane. The node waits for etcd without a bound of its
	// own, so ctx needs a deadline.
	EtcdLeave(ctx context.Context, addr string, s clustersecrets.Secrets, name string) error
	// EtcdRemoveMember removes the etcd member, a node's name or a member's ID in hex, through
	// the control plane name at addr, which refuses to remove its own member. It is refused while
	// the voters left would have no healthy quorum, and with connect.CodeFailedPrecondition when
	// the member went while the call ran. A member etcd does not list fails with
	// connect.CodeNotFound; a learner that has not started has no name yet, and is found only by
	// its ID. An empty member fails with ErrNoMember before any call. The node waits for etcd
	// without a bound of its own, so ctx needs a deadline.
	EtcdRemoveMember(ctx context.Context, addr string, s clustersecrets.Secrets, name, member string) error
	// EtcdMembers lists etcd's members, with their health, as the control plane name at addr sees
	// them. A node that is no etcd member refuses with the NotEtcdMember detail.
	EtcdMembers(ctx context.Context, addr string, s clustersecrets.Secrets, name string) ([]*kubernetesv1.EtcdMember, error)
	// RevokeNode revokes node through the control plane name at addr. Unless allowControlPlane, it
	// refuses an etcd member with *sdkkubernetes.EtcdMemberError; a full list of revoked nodes
	// fails as sdkkubernetes.RevokedListFull tells.
	RevokeNode(ctx context.Context, addr string, s clustersecrets.Secrets, name, node string, allowControlPlane bool) (*kubernetesv1.RevokeNodeResponse, error)
	// PruneRevoked has the control plane name at addr drop the revoked nodes nothing lets back in,
	// and returns the dropped names with the list's length and capacity after.
	PruneRevoked(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*kubernetesv1.PruneRevokedResponse, error)
}

// Maintenance is a machine in maintenance mode, pinned to the certificate it presented.
type Maintenance interface {
	// Fingerprint is the fingerprint of the certificate the machine presented.
	Fingerprint() string
	// Install installs the machine in place with the request's documents, image and secrets,
	// checked by all the layers the SDK knows.
	Install(ctx context.Context, r sdk.InstallRequest) error
	// Close closes the connection.
	Close()
}

// ErrNoMember is the error of an EtcdRemoveMember that names no member: etcd would find none, and
// the NotFound it answers would read as the member removed.
var ErrNoMember = errors.New("no etcd member to remove")

// New returns the Client that reaches nodes over the network.
func New() Client { return remote{} }

// remote reaches nodes over the network with an admin certificate issued for each call.
type remote struct{}

func (remote) Maintenance(ctx context.Context, addr string, s clustersecrets.Secrets) (Maintenance, error) {
	cert, err := sdk.AdminCertificate(s, ClientName, time.Now())
	if err != nil {
		return nil, err
	}
	conn, _, fingerprint, err := sdk.DialMaintenance(ctx, addr, sdk.Trust{Insecure: true}, cert)
	if err != nil {
		return nil, err
	}
	return maintenance{conn: conn, fingerprint: fingerprint}, nil
}

func (remote) Installed(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*nodev1.InfoResponse, error) {
	conn, info, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return nil, err
	}
	conn.Close()
	return info, nil
}

func (remote) Image(ctx context.Context, ref string, kc authn.Keychain) (*ociimage.Artifact, error) {
	return ociimage.Read(ctx, ref, kc)
}

func (remote) Bootstrap(ctx context.Context, addr string, s clustersecrets.Secrets, name string) error {
	conn, _, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = sdkkubernetes.Bootstrap(ctx, conn)
	return err
}

func (remote) KubernetesStatus(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*kubernetesv1.StatusResponse, error) {
	conn, info, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if !slices.Contains(info.Layers, chalkoslayers.Kubernetes) {
		return nil, sdkkubernetes.ErrNoKubernetes
	}
	return sdkkubernetes.Status(ctx, conn)
}

func (remote) EtcdLeave(ctx context.Context, addr string, s clustersecrets.Secrets, name string) error {
	conn, _, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return err
	}
	defer conn.Close()
	return sdkkubernetes.EtcdLeave(ctx, conn, false)
}

func (remote) EtcdRemoveMember(ctx context.Context, addr string, s clustersecrets.Secrets, name, member string) error {
	if member == "" {
		return ErrNoMember
	}
	conn, _, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = sdkkubernetes.EtcdRemoveMember(ctx, conn, member, false)
	return err
}

func (remote) EtcdMembers(ctx context.Context, addr string, s clustersecrets.Secrets, name string) ([]*kubernetesv1.EtcdMember, error) {
	conn, _, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := sdkkubernetes.Client(conn).EtcdMembers(ctx, &kubernetesv1.EtcdMembersRequest{})
	if err != nil {
		return nil, err
	}
	return resp.GetMembers(), nil
}

func (remote) RevokeNode(ctx context.Context, addr string, s clustersecrets.Secrets, name, node string, allowControlPlane bool) (*kubernetesv1.RevokeNodeResponse, error) {
	conn, _, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return sdkkubernetes.RevokeNode(ctx, conn, node, allowControlPlane)
}

func (remote) PruneRevoked(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*kubernetesv1.PruneRevokedResponse, error) {
	conn, _, err := dialInstalled(ctx, addr, s, name)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return sdkkubernetes.PruneRevoked(ctx, conn)
}

// Unreachable tells whether err says the node could not be reached as itself: it did not answer
// or not in time, runs in maintenance mode, or is another node. A node that answers Unavailable
// itself, as when its own etcd does not answer, was reached and is not unreachable.
func Unreachable(err error) bool {
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable:
		return !client.FromNode(err)
	case connect.CodeDeadlineExceeded:
		return true
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sdk.ErrNotInstalled) || errors.Is(err, sdk.ErrOtherNode)
}

// TryNextControlPlane tells whether another control plane may answer a request for the cluster,
// such as EtcdRemoveMember or RevokeNode, where one failed with err: it is unreachable or no etcd
// member.
func TryNextControlPlane(err error) bool {
	return err != nil && (Unreachable(err) || sdkkubernetes.TryNextControlPlane(err))
}

// dialInstalled connects to the installed node name at addr with a new admin certificate.
func dialInstalled(ctx context.Context, addr string, s clustersecrets.Secrets, name string) (*client.Conn, *nodev1.InfoResponse, error) {
	cert, err := sdk.AdminCertificate(s, ClientName, time.Now())
	if err != nil {
		return nil, nil, err
	}
	return sdk.DialInstalled(ctx, addr, s, name, cert)
}

// maintenance is a connection to a machine in maintenance mode.
type maintenance struct {
	conn        *client.Conn
	fingerprint string
}

func (m maintenance) Fingerprint() string { return m.fingerprint }

func (m maintenance) Install(ctx context.Context, r sdk.InstallRequest) error {
	r.Layers = layers.All()
	return sdk.Install(ctx, m.conn, r)
}

func (m maintenance) Close() { m.conn.Close() }
