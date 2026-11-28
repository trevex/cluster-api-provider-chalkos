//go:build e2e_ha

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

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// controlPlaneLabel is the label chalkd's approver sets on the Node of every control plane.
const controlPlaneLabel = "node-role.kubernetes.io/control-plane"

// TestHA scales the lab's control plane 1 → 3 → 1: each step waits until the Machines' nodes are
// Ready and etcd lists one healthy member per control plane, and the removed control planes are
// revoked, oldest first, their Nodes gone. It needs about 11 GiB for its VMs at the peak.
func TestHA(t *testing.T) {
	l := newLab(t)
	ctx := t.Context()
	start := time.Now()
	clusterKey := client.ObjectKey{Namespace: namespace, Name: clusterName}

	l.createCluster(ctx)
	l.waitForMachinesReady(ctx, 2, 20*time.Minute)
	t.Logf("one control plane and one worker are Ready after %s", since(start))

	// 1. Three control planes join, each an etcd voter.
	l.scaleControlPlane(ctx, 3)
	l.waitForMachinesReady(ctx, 4, 30*time.Minute)
	cps, err := l.machinesOf(ctx, true)
	if err != nil || len(cps) != 3 {
		t.Fatalf("%d control-plane Machines, %v", len(cps), err)
	}
	oldestFirst := sortedByCreation(cps)
	l.wantMembers(ctx, oldestFirst)
	l.checkHooks(ctx)
	t.Logf("three control planes after %s", since(start))

	// 2. The control plane scales back to one, removing the oldest Machine first, then the next.
	scaled := time.Now()
	l.scaleControlPlane(ctx, 1)
	l.waitFor(ctx, "one control-plane Machine", 30*time.Minute, func(ctx context.Context) (bool, string, error) {
		ms, err := l.machinesOf(ctx, true)
		if err != nil {
			return false, "", err
		}
		if err := deletedOldestFirst(oldestFirst, ms); err != nil {
			return false, "", err
		}
		var cp controlplanev1.ChalkosControlPlane
		if err := l.c.Get(ctx, clusterKey, &cp); err != nil {
			return false, "", err
		}
		return len(ms) == 1, fmt.Sprintf("%d control-plane Machines; %s", len(ms), conditionsSummary(cp.Status.Conditions, controlplanev1.ScalingDownCondition)), nil
	})
	left, err := l.machinesOf(ctx, true)
	if err != nil || len(left) != 1 || left[0].Name != oldestFirst[2].Name {
		t.Fatalf("the control planes left are %v (%v), want the newest %s", machineNames(left), err, oldestFirst[2].Name)
	}
	for _, gone := range oldestFirst[:2] {
		l.checkRemoved(ctx, gone.Name, left[0].Name)
	}
	l.wantMembers(ctx, left)
	l.checkHooks(ctx)

	// 3. The control plane's Node carries the control-plane label.
	c, state, err := l.workloadClient(ctx)
	if c == nil {
		t.Fatalf("the workload cluster: %s %v", state, err)
	}
	n := &corev1.Node{}
	if err := c.Get(ctx, client.ObjectKey{Name: left[0].Name}, n); err != nil {
		t.Fatal(err)
	}
	if _, ok := n.Labels[controlPlaneLabel]; !ok {
		t.Errorf("the control plane's Node has the labels %v, want %s among them", n.Labels, controlPlaneLabel)
	}
	t.Logf("the control plane scaled 3 → 1 in %s; back to one control plane after %s", since(scaled), since(start))
}

// scaleControlPlane sets the replicas of the ChalkosControlPlane lab.
func (l *lab) scaleControlPlane(ctx context.Context, n int32) {
	l.t.Helper()
	cp := &controlplanev1.ChalkosControlPlane{}
	if err := l.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: clusterName}, cp); err != nil {
		l.t.Fatal(err)
	}
	original := cp.DeepCopy()
	cp.Spec.Replicas = ptr.To(n)
	if err := l.c.Patch(ctx, cp, client.MergeFrom(original)); err != nil {
		l.t.Fatal(err)
	}
}

// wantMembers waits until etcd lists one healthy voting member per control-plane Machine, as the
// first of them sees it.
func (l *lab) wantMembers(ctx context.Context, cps []clusterv1.Machine) {
	l.t.Helper()
	l.waitFor(ctx, fmt.Sprintf("etcd to list %d healthy members", len(cps)), 5*time.Minute, func(ctx context.Context) (bool, string, error) {
		sec, err := l.secrets(ctx)
		if err != nil {
			return false, "", err
		}
		addr, err := l.chalkdAddr(ctx, cps[0].Name)
		if err != nil {
			return false, "", err
		}
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		members, err := node.New().EtcdMembers(ctx, addr, sec, cps[0].Name)
		if err != nil {
			return false, fmt.Sprintf("the etcd members of %s: %v", cps[0].Name, err), nil
		}
		var state []string
		healthy := 0
		for _, m := range members {
			state = append(state, fmt.Sprintf("%s learner=%v unhealthy=%q", m.GetName(), m.GetLearner(), m.GetUnhealthy()))
			if !m.GetLearner() && m.GetUnhealthy() == "" && slices.ContainsFunc(cps, func(cp clusterv1.Machine) bool { return cp.Name == m.GetName() }) {
				healthy++
			}
		}
		return len(members) == len(cps) && healthy == len(cps), strings.Join(state, "; "), nil
	})
}

// sortedByCreation is a copy of the Machines, oldest first, and by name among Machines made in
// the same second.
func sortedByCreation(ms []clusterv1.Machine) []clusterv1.Machine {
	return slices.SortedFunc(slices.Values(ms), func(a, b clusterv1.Machine) int {
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
}

// deletedOldestFirst fails when a Machine of oldestFirst is being deleted or gone, among the
// Machines that exist now, while an older one exists and is not being deleted.
func deletedOldestFirst(oldestFirst, now []clusterv1.Machine) error {
	kept := ""
	for _, m := range oldestFirst {
		i := slices.IndexFunc(now, func(n clusterv1.Machine) bool { return n.Name == m.Name })
		switch removing := i < 0 || !now[i].DeletionTimestamp.IsZero(); {
		case !removing && kept == "":
			kept = m.Name
		case removing && kept != "":
			return fmt.Errorf("the Machine %s is removed before the older %s", m.Name, kept)
		}
	}
	return nil
}

// machineNames are the Machines' names.
func machineNames(ms []clusterv1.Machine) []string {
	var names []string
	for _, m := range ms {
		names = append(names, m.Name)
	}
	return names
}
