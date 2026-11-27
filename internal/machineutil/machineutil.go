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

// Package machineutil holds what both providers' controllers need of a Machine and of their
// conditions.
package machineutil

import (
	"time"
	"unicode/utf8"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

const (
	// RetryAfter is how soon an object that waits for something no watch reports, such as a node
	// that is unreachable, is reconciled again.
	RetryAfter = 15 * time.Second

	// MaxMessage is the longest condition message, in bytes, a refusal or error gives.
	MaxMessage = 1024
)

// Truncate shortens a condition's message to MaxMessage bytes, at a character's boundary. The
// messages are chalkos's refusals and errors, which name the documents' fields but not the
// values of Secret documents: chalkos checks rules and validators against redacted documents.
func Truncate(message string) string {
	if len(message) <= MaxMessage {
		return message
	}
	cut := MaxMessage - len("…")
	for cut > 0 && !utf8.RuneStart(message[cut]) {
		cut--
	}
	return message[:cut] + "…"
}

// InternalIP is the Machine's first InternalIP; empty when it has none.
func InternalIP(m *clusterv1.Machine) string {
	for _, a := range m.Status.Addresses {
		if a.Type == clusterv1.MachineInternalIP {
			return a.Address
		}
	}
	return ""
}

// InternalIPs are all the Machine's InternalIPs, in order.
func InternalIPs(m *clusterv1.Machine) []string {
	var ips []string
	for _, a := range m.Status.Addresses {
		if a.Type == clusterv1.MachineInternalIP {
			ips = append(ips, a.Address)
		}
	}
	return ips
}
