//go:build e2e

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
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
)

func TestLab(t *testing.T) {
	l := newLab(t)
	ctx := t.Context()
	start := time.Now()

	clusterKey := client.ObjectKey{Namespace: namespace, Name: clusterName}
	var existing clusterv1.Cluster
	switch err := l.c.Get(ctx, clusterKey, &existing); {
	case err == nil:
		t.Fatalf("the Cluster %s/%s exists already; delete it first", namespace, clusterName)
	case !apierrors.IsNotFound(err):
		t.Fatal(err)
	}
	// Cleanups run last first: a failed run is diagnosed before its Cluster is deleted. Both run
	// after t.Context() is cancelled, so they bring their own.
	t.Cleanup(l.deleteCluster)
	t.Cleanup(func() {
		if t.Failed() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			l.diagnose(ctx)
		}
	})
	l.just("cluster")
	t.Logf("applied the cluster after %s", since(start))

	// 1. The control plane is initialized and available.
	cp := &controlplanev1.ChalkosControlPlane{}
	l.waitFor(ctx, "the control plane to be initialized and available", 20*time.Minute, func(ctx context.Context) (bool, string, error) {
		if err := l.c.Get(ctx, clusterKey, cp); err != nil {
			return false, "", err
		}
		return conditions.IsTrue(cp, controlplanev1.InitializedCondition) && conditions.IsTrue(cp, controlplanev1.AvailableCondition),
			conditionsSummary(cp.Status.Conditions, controlplanev1.InitializedCondition, controlplanev1.AvailableCondition), nil
	})
	t.Logf("the control plane is available after %s", since(start))

	// 2. The kubeconfig Secret reaches the cluster, whose nodes are the Machines, Ready and named
	// by their providerID.
	var machines clusterv1.MachineList
	l.waitFor(ctx, "both nodes to be Ready", 15*time.Minute, func(ctx context.Context) (bool, string, error) {
		if err := l.c.List(ctx, &machines, client.InNamespace(namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: clusterName}); err != nil {
			return false, "", err
		}
		if len(machines.Items) != 2 {
			return false, fmt.Sprintf("%d Machines", len(machines.Items)), nil
		}
		return l.nodesReady(ctx, machines.Items)
	})
	t.Logf("both nodes are Ready after %s", since(start))

	// 3. The bootstrap provider reports each machine installed, with the fingerprint it trusted in
	// maintenance mode and the image's root hash.
	for _, m := range machines.Items {
		var cfg bootstrapv1.ChalkosConfig
		if err := l.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, &cfg); err != nil {
			t.Fatal(err)
		}
		st := cfg.Status
		if !ptr.Deref(st.Installed, false) || len(st.MaintenanceFingerprints) == 0 || st.ImageRootHash == "" {
			t.Errorf("the ChalkosConfig %s: installed %v, %d maintenance fingerprints, image root hash %q; want installed, at least one fingerprint and a root hash",
				cfg.Name, ptr.Deref(st.Installed, false), len(st.MaintenanceFingerprints), st.ImageRootHash)
		}
	}

	// 4. The kubeconfig's client certificate expires within its TTL. The control plane is read
	// again: the kubeconfig may have been issued after the first wait read it.
	if err := l.c.Get(ctx, clusterKey, cp); err != nil {
		t.Fatal(err)
	}
	l.checkKubeconfigCert(cp)
	if t.Failed() {
		t.FailNow()
	}

	// 5. The workers scale 1 → 2 → 1. Every Machine carries its provider's pre-terminate hook, the
	// removed worker's node is revoked by the control plane, and its Node and Machine are gone.
	l.checkHooks(ctx)
	scaled := time.Now()
	l.scaleWorkers(ctx, 2)
	l.waitFor(ctx, "three nodes to be Ready", 15*time.Minute, func(ctx context.Context) (bool, string, error) {
		if err := l.c.List(ctx, &machines, client.InNamespace(namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: clusterName}); err != nil {
			return false, "", err
		}
		if len(machines.Items) != 3 {
			return false, fmt.Sprintf("%d Machines", len(machines.Items)), nil
		}
		return l.nodesReady(ctx, machines.Items)
	})
	t.Logf("three nodes are Ready after %s", since(start))
	l.checkHooks(ctx)
	before, err := l.machinesOf(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	cps, err := l.machinesOf(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(cps) != 1 {
		t.Fatalf("%d control-plane Machines, want 1", len(cps))
	}
	l.scaleWorkers(ctx, 1)
	var removed string
	l.waitFor(ctx, "a worker Machine to be gone", 15*time.Minute, func(ctx context.Context) (bool, string, error) {
		after, err := l.machinesOf(ctx, false)
		if err != nil {
			return false, "", err
		}
		if len(after) != 1 {
			return false, fmt.Sprintf("%d worker Machines", len(after)), nil
		}
		kept := slices.IndexFunc(before, func(m clusterv1.Machine) bool { return m.Name == after[0].Name })
		if len(before) != 2 || kept < 0 {
			return false, "", fmt.Errorf("the worker Machine %s is left, of %d before", after[0].Name, len(before))
		}
		removed = before[1-kept].Name
		return true, "", nil
	})
	l.checkRemoved(ctx, removed, cps[0].Name)
	l.checkHooks(ctx)
	t.Logf("the workers scaled 1 → 2 → 1 in %s; %s was removed and revoked", since(scaled), removed)

	// 6. Deleting the Cluster deletes the Machines, their infrastructure and VMs, the control plane
	// once its finalizer is removed, and the Cluster itself.
	deleted := time.Now()
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: clusterName}}
	if err := l.c.Delete(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	l.waitFor(ctx, "the cluster's objects to be gone", 10*time.Minute, func(ctx context.Context) (bool, string, error) {
		left := map[string]int{}
		for kind, list := range map[string]client.ObjectList{
			"Machines":         &clusterv1.MachineList{},
			"KubevirtMachines": unstructuredList(kubevirtMachineList),
			"VirtualMachines":  unstructuredList(virtualMachineList),
		} {
			opts := []client.ListOption{client.InNamespace(namespace)}
			if kind == "Machines" {
				opts = append(opts, client.MatchingLabels{clusterv1.ClusterNameLabel: clusterName})
			}
			if err := l.c.List(ctx, list, opts...); err != nil {
				return false, "", err
			}
			left[kind] = meta.LenList(list)
		}
		var deleting string
		var gone controlplanev1.ChalkosControlPlane
		switch err := l.c.Get(ctx, clusterKey, &gone); {
		case err == nil:
			left["ChalkosControlPlanes"] = 1
			deleting = conditionsSummary(gone.Status.Conditions, controlplanev1.DeletingCondition)
		case !apierrors.IsNotFound(err):
			return false, "", err
		}
		switch err := l.c.Get(ctx, clusterKey, &clusterv1.Cluster{}); {
		case err == nil:
			left["Clusters"] = 1
		case !apierrors.IsNotFound(err):
			return false, "", err
		}
		var parts []string
		for _, kind := range slices.Sorted(maps.Keys(left)) {
			if left[kind] > 0 {
				parts = append(parts, fmt.Sprintf("%d %s", left[kind], kind))
			}
		}
		state := strings.Join(parts, ", ")
		if deleting != "" {
			state += "; " + deleting
		}
		return len(parts) == 0, state, nil
	})
	t.Logf("the cluster was deleted in %s; the whole run took %s", since(deleted), since(start))
}
