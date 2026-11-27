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

package machineutil

import (
	"slices"
	"strings"
	"testing"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// A long message is cut to MaxMessage bytes at a character's boundary.
func TestTruncate(t *testing.T) {
	long := strings.Repeat("ä", MaxMessage)
	got := Truncate(long)
	if len(got) > MaxMessage || !strings.HasSuffix(got, "…") || !strings.HasPrefix(long, strings.TrimSuffix(got, "…")) {
		t.Errorf("Truncate gives %d bytes: %q", len(got), got)
	}
	if Truncate("short") != "short" {
		t.Error("a short message is changed")
	}
}

// The first InternalIP is the Machine's address; other address types are skipped.
func TestInternalIP(t *testing.T) {
	m := &clusterv1.Machine{Status: clusterv1.MachineStatus{Addresses: clusterv1.MachineAddresses{
		{Type: clusterv1.MachineExternalIP, Address: "192.0.2.1"},
		{Type: clusterv1.MachineInternalIP, Address: "10.0.0.1"},
		{Type: clusterv1.MachineInternalIP, Address: "10.0.0.2"},
	}}}
	if got := InternalIP(m); got != "10.0.0.1" {
		t.Errorf("InternalIP = %q, want 10.0.0.1", got)
	}
	if got := InternalIP(&clusterv1.Machine{}); got != "" {
		t.Errorf("InternalIP of a Machine without addresses = %q, want empty", got)
	}
}

// InternalIPs are every InternalIP of the Machine, in order.
func TestInternalIPs(t *testing.T) {
	m := &clusterv1.Machine{Status: clusterv1.MachineStatus{Addresses: clusterv1.MachineAddresses{
		{Type: clusterv1.MachineInternalIP, Address: "10.0.0.1"},
		{Type: clusterv1.MachineExternalIP, Address: "192.0.2.1"},
		{Type: clusterv1.MachineInternalIP, Address: "10.0.0.2"},
	}}}
	if got := InternalIPs(m); !slices.Equal(got, []string{"10.0.0.1", "10.0.0.2"}) {
		t.Errorf("InternalIPs = %q", got)
	}
}
