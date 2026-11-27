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
	"slices"
	"time"

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
