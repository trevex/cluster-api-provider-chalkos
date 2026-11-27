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
	"testing"

	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/sdk"
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
