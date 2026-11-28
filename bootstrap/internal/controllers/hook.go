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

package controllers

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

// reconcileHook runs the pre-terminate hook of a worker Machine. It adds the hook to a Machine
// that is not being deleted, and removes it from every Machine once the Cluster is being deleted;
// done is set when the reconcile ends here.
func (r *ChalkosConfigReconciler) reconcileHook(ctx context.Context, s *scope) (ctrl.Result, bool, error) {
	if !machineutil.HasHook(s.machine) && s.machine.DeletionTimestamp.IsZero() && s.cluster.DeletionTimestamp.IsZero() {
		// The cache may not have seen the Machine's deletion yet: the hook must not land on a
		// Machine being deleted, whose deletion may have passed the hook already.
		var fresh clusterv1.Machine
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(s.machine), &fresh); err != nil {
			return ctrl.Result{}, true, client.IgnoreNotFound(err)
		}
		s.machine = &fresh
	}
	m := s.machine
	switch {
	case !s.cluster.DeletionTimestamp.IsZero():
		// The whole cluster goes: nothing is revoked.
		if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
			return ctrl.Result{}, true, err
		}
		if !m.DeletionTimestamp.IsZero() {
			s.nodeRemoved(metav1.ConditionTrue, bootstrapv1.DoneReason, "The Cluster is being deleted; the node is not revoked")
		}
		return ctrl.Result{}, true, nil
	case m.DeletionTimestamp.IsZero():
		added, err := machineutil.AddHook(ctx, r.Client, m, machineutil.BootstrapHookOwner)
		if added {
			ctrl.LoggerFrom(ctx).Info("Added the pre-terminate hook")
		}
		return ctrl.Result{}, err != nil, err
	}
	return r.removeWorker(ctx, s)
}

// removeWorker runs the hook of a worker Machine being deleted: a Machine whose machine was never
// reached to be installed loses it at once; any other, once Cluster API waits for the hook, is
// revoked through the first control plane that does it, then loses it. Revoking earlier would
// deny the kubelet's writes while its pods are evicted. A pass that cannot finish keeps the hook
// and is tried again after machineutil.HookRetry; it writes the same status each time, so that
// the ChalkosConfig's watch does not start the next pass at once.
func (r *ChalkosConfigReconciler) removeWorker(ctx context.Context, s *scope) (ctrl.Result, bool, error) {
	m := s.machine
	if ptr.Deref(s.cfg.Status.Installed, false) {
		// The machine stays installed until its infrastructure goes.
		s.setReady(metav1.ConditionFalse, bootstrapv1.MachineDeletingReason, "The Machine is being deleted")
	} else {
		s.set(metav1.ConditionFalse, bootstrapv1.MachineDeletingReason, "The Machine is being deleted")
	}
	switch {
	case !machineutil.HasHook(m):
		// Done already, or removed by hand.
		return ctrl.Result{}, true, nil
	case !everReached(&s.cfg.Status):
		if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
			return ctrl.Result{}, true, err
		}
		s.nodeRemoved(metav1.ConditionTrue, bootstrapv1.DoneReason, "The machine was never reached to be installed; nothing is revoked")
		return ctrl.Result{}, true, nil
	case !machineutil.WaitingForHook(m):
		// The Machine's watch reconciles again once Cluster API waits for the hook.
		return ctrl.Result{}, true, nil
	}
	sec, ok, err := secrets.Get(ctx, r.APIReader, client.ObjectKeyFromObject(s.cluster))
	switch {
	case err != nil:
		return ctrl.Result{}, true, err
	case !ok:
		return r.hookWaits(ctx, s, bootstrapv1.WaitingForControlPlaneReason,
			fmt.Sprintf("The Cluster's secrets %s are gone, so no control plane can be reached", secrets.Name(s.cluster.Name)))
	}
	cps, err := machineutil.ControlPlaneMachines(ctx, r.Client, s.cluster)
	if err != nil {
		return ctrl.Result{}, true, err
	}
	// The calls to the nodes share one deadline; the writes to the management cluster do not.
	passCtx, cancel := context.WithTimeout(ctx, machineutil.HookPassTimeout)
	defer cancel()
	via, resp, reason, msg := r.revokeWorker(passCtx, s, cps, sec)
	if via == "" {
		return r.hookWaits(ctx, s, reason, msg)
	}
	if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
		return ctrl.Result{}, true, err
	}
	s.nodeRemoved(metav1.ConditionTrue, bootstrapv1.DoneReason,
		fmt.Sprintf("Revoked through %s; the list of revoked nodes is %d of %d characters long", via, resp.GetListLength(), resp.GetListCapacity()))
	machineutil.Event(r.Recorder, s.cfg, m, corev1.EventTypeNormal, "NodeRevoked", "RevokeNode", "Revoked the node %s through the control plane %s", m.Name, via)
	ctrl.LoggerFrom(ctx).Info("Revoked the node and removed the pre-terminate hook", "controlPlane", via)
	return ctrl.Result{}, true, nil
}

// everReached tells whether the bootstrap provider ever reached the machine to install it, which
// may have given it certificates: the status records it installed, an install, or a
// maintenance-mode fingerprint, which is written before the install starts.
func everReached(st *bootstrapv1.ChalkosConfigStatus) bool {
	return ptr.Deref(st.Installed, false) || st.LastInstallTime != nil || len(st.MaintenanceFingerprints) > 0
}

// revokeWorker revokes the worker's node through the first of the control planes cps that does,
// and returns that control plane's name and its answer. One that cannot be reached, or is no
// etcd member, such as one that left etcd, is passed over, and so is one that refuses otherwise;
// when none revokes the node, it returns the reason and message the hook waits with. A full list
// is pruned through the control plane that refused it, unless a hook of the Cluster's workers
// pruned it less than machineutil.FullListPruneInterval ago, and the revocation tried once more;
// when the list stays full, no other control plane is asked, as the list is the cluster's. The
// message tells each error as node.ConditionError does, so that a pass that waits as the one
// before writes nothing; the log has the errors in full.
func (r *ChalkosConfigReconciler) revokeWorker(ctx context.Context, s *scope, cps []clusterv1.Machine, sec clustersecrets.Secrets) (string, *kubernetesv1.RevokeNodeResponse, string, string) {
	worker := s.machine.Name
	var tried []string
	refused, pruned := false, false
	for i := range cps {
		cp := &cps[i]
		addr := machineutil.InternalIP(cp)
		if addr == "" {
			continue
		}
		resp, err := r.revoke(ctx, addr, sec, cp.Name, worker)
		if sdkkubernetes.RevokedListFull(err) && !pruned && r.claimPrune(client.ObjectKeyFromObject(s.cluster)) {
			// A full list may hold entries nothing lets back in any more: drop them and try once
			// more.
			r.prune(ctx, s, addr, sec, cp.Name)
			pruned = true
			resp, err = r.revoke(ctx, addr, sec, cp.Name, worker)
		}
		switch {
		case err == nil:
			return cp.Name, resp, "", ""
		case sdkkubernetes.RevokedListFull(err):
			// The message leaves out the prune, which only some passes make, so that it stays the
			// same.
			return "", nil, bootstrapv1.RevokedListFullReason, fmt.Sprintf(
				"%s; PruneRevoked made too little room; the Machine waits until there is room: rotate the cluster's CAs with chalkctl rotate os-ca and chalkctl rotate kubernetes-ca, "+
					"after which PruneRevoked drops the nodes revoked before them, or remove the annotation %s to delete the Machine without revoking its node",
				node.ConditionError(err), machineutil.HookAnnotation)
		case !node.TryNextControlPlane(err):
			refused = true
		}
		ctrl.LoggerFrom(ctx).Info("A control plane did not revoke the node", "controlPlane", cp.Name, "error", err.Error())
		tried = append(tried, fmt.Sprintf("%s: %s", cp.Name, node.ConditionError(err)))
	}
	switch {
	case len(tried) == 0:
		return "", nil, bootstrapv1.WaitingForControlPlaneReason, "No control-plane Machine has an address to revoke the node through"
	case refused:
		return "", nil, bootstrapv1.RevokingReason, "No control plane revoked the node: " + strings.Join(tried, "; ")
	}
	return "", nil, bootstrapv1.WaitingForControlPlaneReason, "No control plane revoked the node: " + strings.Join(tried, "; ")
}

// claimPrune tells whether a worker's hook that finds the Cluster's list of revoked nodes full
// prunes it, and if so records the prune: no hook of the Cluster's workers pruned it in the last
// machineutil.FullListPruneInterval. A full list stays full until the cluster's CAs are rotated,
// which takes longer than that; between prunes, the hook only retries the revocation. The control
// plane prunes the list too, daily and when its own hooks find it full.
func (r *ChalkosConfigReconciler) claimPrune(cluster client.ObjectKey) bool {
	now := r.clock().Now()
	r.prunesMu.Lock()
	defer r.prunesMu.Unlock()
	if last, ok := r.prunes[cluster]; ok && now.Before(last.Add(machineutil.FullListPruneInterval)) {
		return false
	}
	if r.prunes == nil {
		r.prunes = map[client.ObjectKey]time.Time{}
	}
	r.prunes[cluster] = now
	return true
}

// clock is the reconciler's clock.
func (r *ChalkosConfigReconciler) clock() clock.PassiveClock {
	if r.Clock == nil {
		return clock.RealClock{}
	}
	return r.Clock
}

// revoke revokes the node worker through the control plane name at addr, within
// machineutil.HookCallTimeout.
func (r *ChalkosConfigReconciler) revoke(ctx context.Context, addr string, sec clustersecrets.Secrets, name, worker string) (*kubernetesv1.RevokeNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	return r.Node.RevokeNode(ctx, addr, sec, name, worker, false)
}

// prune has the control plane name at addr prune its list of revoked nodes, within
// machineutil.HookCallTimeout. A failure is logged and recorded as an event, not returned: the
// revocation that follows tells whether there is room.
func (r *ChalkosConfigReconciler) prune(ctx context.Context, s *scope, addr string, sec clustersecrets.Secrets, name string) {
	callCtx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	resp, err := r.Node.PruneRevoked(callCtx, addr, sec, name)
	if err != nil {
		ctrl.LoggerFrom(ctx).Info("Pruning the revoked nodes failed", "controlPlane", name, "error", err.Error())
		machineutil.Event(r.Recorder, s.cfg, s.machine, corev1.EventTypeWarning, "PruneRevokedFailed", "PruneRevoked", "%s",
			machineutil.Truncate(fmt.Sprintf("Pruning the revoked nodes through %s: %s", name, node.ConditionError(err))))
		return
	}
	ctrl.LoggerFrom(ctx).Info("Pruned the revoked nodes", "controlPlane", name, "pruned", resp.GetPruned())
}

// hookWaits reports a hook pass that could not finish: the hook stays, a Warning event is
// recorded, and the pass is tried again after machineutil.HookRetry.
func (r *ChalkosConfigReconciler) hookWaits(ctx context.Context, s *scope, reason, message string) (ctrl.Result, bool, error) {
	s.nodeRemoved(metav1.ConditionFalse, reason, message)
	machineutil.Event(r.Recorder, s.cfg, s.machine, corev1.EventTypeWarning, reason, "RevokeNode", "%s", machineutil.Truncate(message))
	ctrl.LoggerFrom(ctx).Info("The pre-terminate hook waits", "reason", reason, "message", message)
	return ctrl.Result{RequeueAfter: machineutil.HookRetry}, true, nil
}
