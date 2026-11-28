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
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
)

// HookAnnotation is the pre-terminate hook every chalkos Machine carries. Cluster API keeps the
// Machine's infrastructure until the annotation is gone, so the provider that owns the Machine
// first takes its node out of etcd and revokes it. Removing the annotation by hand deletes the
// Machine without either.
const HookAnnotation = clusterv1.PreTerminateDeleteHookAnnotationPrefix + "/chalkos"

// The values of HookAnnotation, which name the provider that runs the hook.
const (
	ControlPlaneHookOwner = "control-plane-chalkos"
	BootstrapHookOwner    = "bootstrap-chalkos"
)

const (
	// HookRetry is how soon a hook step that could not finish, such as one refused for etcd's
	// quorum or one that found no control plane, is tried again.
	HookRetry = 30 * time.Second
	// HookCallTimeout bounds one call to a node while a hook runs.
	HookCallTimeout = 2 * time.Minute
	// HookPassTimeout bounds one pass of a hook, all its calls to nodes together, so that
	// several nodes that do not answer cannot hold a reconcile for long.
	HookPassTimeout = 3 * time.Minute
	// FullListPruneInterval is how long after the last prune a hook that finds the list of
	// revoked nodes full prunes it again before it retries the revocation. Within it, the hook
	// only waits: a prune is a write to the workload cluster, and a CA rotation, which lets a
	// prune drop nodes, takes longer than that.
	FullListPruneInterval = 5 * time.Minute
)

// HasHook tells whether the Machine carries HookAnnotation, of any value.
func HasHook(m *clusterv1.Machine) bool {
	_, ok := m.Annotations[HookAnnotation]
	return ok
}

// AddHook adds HookAnnotation with the owner's value to a Machine that is not being deleted and
// has none; a Machine that has it, of any value, is left alone. It tells whether it added it.
func AddHook(ctx context.Context, c client.Client, m *clusterv1.Machine, owner string) (bool, error) {
	if HasHook(m) || !m.DeletionTimestamp.IsZero() {
		return false, nil
	}
	original := m.DeepCopy()
	if m.Annotations == nil {
		m.Annotations = map[string]string{}
	}
	m.Annotations[HookAnnotation] = owner
	if err := c.Patch(ctx, m, client.MergeFrom(original)); err != nil {
		return false, fmt.Errorf("add the pre-terminate hook to the Machine %s: %w", m.Name, err)
	}
	return true, nil
}

// RemoveHook removes HookAnnotation from the Machine, which lets Cluster API delete its
// infrastructure. A Machine without it, or one that is gone, is left alone.
func RemoveHook(ctx context.Context, c client.Client, m *clusterv1.Machine) error {
	if !HasHook(m) {
		return nil
	}
	original := m.DeepCopy()
	delete(m.Annotations, HookAnnotation)
	if err := c.Patch(ctx, m, client.MergeFrom(original)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove the pre-terminate hook of the Machine %s: %w", m.Name, err)
	}
	return nil
}

// DeletingReason is the reason of the Machine's Deleting condition; empty without one.
func DeletingReason(m *clusterv1.Machine) string {
	if c := conditions.Get(m, clusterv1.MachineDeletingCondition); c != nil {
		return c.Reason
	}
	return ""
}

// WaitingForHook tells whether Cluster API waits for the Machine's pre-terminate hooks: the
// Machine is being deleted and its Deleting condition is true with the reason
// WaitingForPreTerminateHook. Cluster API gets there after the drain and the volume detach it
// runs, if any: it skips both while the Cluster or the control plane is being deleted, for the
// last control-plane Machine, for a Machine without a Node, when the Machine's annotations
// exclude them, and once their timeouts passed.
func WaitingForHook(m *clusterv1.Machine) bool {
	c := conditions.Get(m, clusterv1.MachineDeletingCondition)
	return !m.DeletionTimestamp.IsZero() && c != nil && c.Status == metav1.ConditionTrue &&
		c.Reason == clusterv1.MachineDeletingWaitingForPreTerminateHookReason
}

// ControlPlaneMachines are the control-plane Machines of the cluster, those not being deleted
// first, each group oldest first.
func ControlPlaneMachines(ctx context.Context, c client.Reader, cluster *clusterv1.Cluster) ([]clusterv1.Machine, error) {
	var list clusterv1.MachineList
	if err := c.List(ctx, &list, client.InNamespace(cluster.Namespace), client.MatchingLabels{
		clusterv1.ClusterNameLabel: cluster.Name, clusterv1.MachineControlPlaneLabel: "",
	}); err != nil {
		return nil, fmt.Errorf("list the cluster's control-plane Machines: %w", err)
	}
	slices.SortFunc(list.Items, func(a, b clusterv1.Machine) int {
		if da, db := !a.DeletionTimestamp.IsZero(), !b.DeletionTimestamp.IsZero(); da != db {
			if da {
				return 1
			}
			return -1
		}
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return list.Items, nil
}

// ClusterDeletionStarted passes the updates of a Cluster whose deletion started.
func ClusterDeletionStarted() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetDeletionTimestamp().IsZero() && !e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// Event records an event regarding the object, about related, with rec; a nil rec records
// nothing.
func Event(rec events.EventRecorder, regarding, related runtime.Object, eventtype, reason, action, note string, args ...any) {
	if rec != nil {
		rec.Eventf(regarding, related, eventtype, reason, action, note, args...)
	}
}
