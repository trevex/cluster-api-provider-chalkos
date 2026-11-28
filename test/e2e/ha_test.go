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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// controlPlaneLabel is the label chalkd's approver sets on the Node of every control plane.
const controlPlaneLabel = "node-role.kubernetes.io/control-plane"

// TestHA scales the lab's control plane 1 → 3, checks that a change of a fixed field is refused,
// scales the workers away, rolls the control plane with maxSurge 1 and with maxSurge 0, and scales
// it back to one. Each step waits until the Machines' nodes are Ready and etcd lists one healthy
// member per control plane, and the removed control planes are revoked, oldest first, their Nodes
// gone. It needs about 12 GiB for its VMs at the peak: four control planes while a roll surges.
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

	// 2. A change of the Cluster's domain, a field every node holds alike, is refused against the
	// released image's schema and creates no Machine; reverting it lifts the hold.
	l.checkFixedFieldRefused(ctx)

	// 3. The workers go, so that a roll's fourth control plane fits the runner.
	l.scaleWorkers(ctx, 0)
	l.waitFor(ctx, "no worker Machine", 15*time.Minute, func(ctx context.Context) (bool, string, error) {
		ms, err := l.machinesOf(ctx, false)
		return err == nil && len(ms) == 0, fmt.Sprintf("%d worker Machines", len(ms)), err
	})

	// 4. rollout.after replaces the three control planes oldest first, with maxSurge 1 and then 0.
	l.roll(ctx, 1)
	l.roll(ctx, 0)
	if cps, err = l.machinesOf(ctx, true); err != nil || len(cps) != 3 {
		t.Fatalf("%d control-plane Machines after the rolls, %v", len(cps), err)
	}
	oldestFirst = sortedByCreation(cps)

	// 5. The control plane scales back to one, removing the oldest Machine first, then the next.
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

	// 6. The control plane's Node carries the control-plane label.
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

// checkFixedFieldRefused sets the domain of the Cluster document in the ChalkosControlPlane's
// spec.files, a field every node of the cluster holds alike, and waits until RollingOut is True
// with the reason FixedFieldChanged while no Machine is created or deleted; then it reverts the
// change and waits until RollingOut is NotRollingOut again.
func (l *lab) checkFixedFieldRefused(ctx context.Context) {
	l.t.Helper()
	key := client.ObjectKey{Namespace: namespace, Name: clusterName}
	before, err := l.machinesOf(ctx, true)
	if err != nil {
		l.t.Fatal(err)
	}
	cp := &controlplanev1.ChalkosControlPlane{}
	if err := l.c.Get(ctx, key, cp); err != nil {
		l.t.Fatal(err)
	}
	original := cp.DeepCopy()
	changed := false
	for i, f := range cp.Spec.Files {
		var doc map[string]any
		if err := json.Unmarshal(f.Raw, &doc); err != nil {
			l.t.Fatal(err)
		}
		spec, ok := doc["spec"].(map[string]any)
		if doc["kind"] != "Cluster" || !ok {
			continue
		}
		spec["domain"] = "changed.example"
		raw, err := json.Marshal(doc)
		if err != nil {
			l.t.Fatal(err)
		}
		cp.Spec.Files[i] = runtime.RawExtension{Raw: raw}
		changed = true
	}
	if !changed {
		l.t.Fatal("the ChalkosControlPlane's files hold no Cluster document")
	}
	if err := l.c.Patch(ctx, cp, client.MergeFrom(original)); err != nil {
		l.t.Fatal(err)
	}
	l.waitFor(ctx, "the change of the Cluster's domain to be refused", 5*time.Minute, func(ctx context.Context) (bool, string, error) {
		ms, err := l.machinesOf(ctx, true)
		if err != nil {
			return false, "", err
		}
		if len(ms) != len(before) {
			return false, "", fmt.Errorf("%d control-plane Machines while a fixed field's change is held, want %d", len(ms), len(before))
		}
		c, err := l.rollingOut(ctx)
		if err != nil || c == nil {
			return false, "no RollingOut condition yet", err
		}
		return c.Status == metav1.ConditionTrue && c.Reason == controlplanev1.FixedFieldChangedReason && strings.Contains(c.Message, "spec.domain"),
			fmt.Sprintf("RollingOut %s %s: %s", c.Status, c.Reason, c.Message), nil
	})
	if err := l.c.Get(ctx, key, cp); err != nil {
		l.t.Fatal(err)
	}
	reverted := cp.DeepCopy()
	reverted.Spec.Files = original.Spec.Files
	if err := l.c.Patch(ctx, reverted, client.MergeFrom(cp)); err != nil {
		l.t.Fatal(err)
	}
	l.waitFor(ctx, "the hold to be lifted", 5*time.Minute, func(ctx context.Context) (bool, string, error) {
		c, err := l.rollingOut(ctx)
		if err != nil || c == nil {
			return false, "no RollingOut condition", err
		}
		return c.Reason == controlplanev1.NotRollingOutReason, fmt.Sprintf("RollingOut %s %s: %s", c.Status, c.Reason, c.Message), nil
	})
}

// roll sets spec.rollout.after to now and maxSurge, and waits until each of the three
// control-plane Machines is replaced, oldest first, by Machines whose nodes are Ready. At every
// poll the control plane has at most three Machines plus maxSurge, being deleted included, and
// etcd at least two healthy voting members whenever a control plane lists them. Each replaced
// Machine's Node is gone and revoked.
func (l *lab) roll(ctx context.Context, maxSurge int32) {
	l.t.Helper()
	start := time.Now()
	key := client.ObjectKey{Namespace: namespace, Name: clusterName}
	before, err := l.machinesOf(ctx, true)
	if err != nil || len(before) != 3 {
		l.t.Fatalf("%d control-plane Machines before the roll, %v", len(before), err)
	}
	oldestFirst := sortedByCreation(before)
	cp := &controlplanev1.ChalkosControlPlane{}
	if err := l.c.Get(ctx, key, cp); err != nil {
		l.t.Fatal(err)
	}
	original := cp.DeepCopy()
	cp.Spec.Rollout.After = metav1.Now()
	cp.Spec.Rollout.Strategy.RollingUpdate.MaxSurge = ptr.To(maxSurge)
	if err := l.c.Patch(ctx, cp, client.MergeFrom(original)); err != nil {
		l.t.Fatal(err)
	}
	most := 3 + int(maxSurge)
	l.waitFor(ctx, fmt.Sprintf("the control plane to roll with maxSurge %d", maxSurge), 75*time.Minute, func(ctx context.Context) (bool, string, error) {
		ms, err := l.machinesOf(ctx, true)
		if err != nil {
			return false, "", err
		}
		if len(ms) > most {
			return false, "", fmt.Errorf("%d control-plane Machines, more than %d", len(ms), most)
		}
		if err := deletedOldestFirst(oldestFirst, ms); err != nil {
			return false, "", err
		}
		voters, members, verr := l.healthyVoters(ctx, ms)
		if verr == nil && voters < 2 {
			return false, "", fmt.Errorf("etcd has %d healthy voting members: %s", voters, members)
		}
		c, err := l.rollingOut(ctx)
		if err != nil {
			return false, "", err
		}
		replaced := !slices.ContainsFunc(ms, func(m clusterv1.Machine) bool {
			return slices.ContainsFunc(oldestFirst, func(o clusterv1.Machine) bool { return o.Name == m.Name })
		})
		done := replaced && len(ms) == 3 && c != nil && c.Reason == controlplanev1.NotRollingOutReason
		state := fmt.Sprintf("%d control-plane Machines; etcd: %s", len(ms), members)
		if c != nil {
			state += fmt.Sprintf("; RollingOut %s %s: %s", c.Status, c.Reason, c.Message)
		}
		return done, state, nil
	})
	after := sortedByCreation(l.waitForMachinesReady(ctx, 3, 15*time.Minute))
	for _, gone := range oldestFirst {
		l.checkRemoved(ctx, gone.Name, after[0].Name)
	}
	l.wantMembers(ctx, after)
	l.checkHooks(ctx)
	l.t.Logf("rolled the control plane with maxSurge %d in %s", maxSurge, since(start))
}

// healthyVoters counts etcd's healthy voting members, as the oldest control-plane Machine of ms
// that is not being deleted and answers lists them, and describes the members.
func (l *lab) healthyVoters(ctx context.Context, ms []clusterv1.Machine) (int, string, error) {
	sec, err := l.secrets(ctx)
	if err != nil {
		return 0, "", err
	}
	var tried []string
	for _, m := range sortedByCreation(ms) {
		if !m.DeletionTimestamp.IsZero() {
			continue
		}
		addr, err := l.chalkdAddr(ctx, m.Name)
		if err != nil {
			return 0, "", err
		}
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		members, err := node.New().EtcdMembers(callCtx, addr, sec, m.Name)
		cancel()
		if err != nil {
			tried = append(tried, fmt.Sprintf("%s: %v", m.Name, err))
			continue
		}
		voters := 0
		var state []string
		for _, mem := range members {
			state = append(state, fmt.Sprintf("%s learner=%v unhealthy=%q", mem.GetName(), mem.GetLearner(), mem.GetUnhealthy()))
			if !mem.GetLearner() && mem.GetUnhealthy() == "" {
				voters++
			}
		}
		return voters, strings.Join(state, ", "), nil
	}
	return 0, strings.Join(tried, "; "), errors.New("no control plane lists etcd's members")
}

// rollingOut is the ChalkosControlPlane's RollingOut condition; nil before it has one.
func (l *lab) rollingOut(ctx context.Context) (*metav1.Condition, error) {
	cp := &controlplanev1.ChalkosControlPlane{}
	if err := l.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: clusterName}, cp); err != nil {
		return nil, err
	}
	return meta.FindStatusCondition(cp.Status.Conditions, controlplanev1.RollingOutCondition), nil
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
