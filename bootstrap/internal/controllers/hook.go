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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

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

// removeWorker runs the hook of a worker Machine being deleted: a machine never installed loses
// it at once; an installed one, once Cluster API waits for the hook, is revoked through the first
// control plane that answers, then loses it. Revoking earlier would deny the kubelet's writes
// while its pods are evicted. A step that cannot finish keeps the hook and is tried again after
// machineutil.HookRetry.
func (r *ChalkosConfigReconciler) removeWorker(ctx context.Context, s *scope) (ctrl.Result, bool, error) {
	m := s.machine
	switch {
	case !machineutil.HasHook(m):
		// Done already, or removed by hand.
		return ctrl.Result{}, true, nil
	case !ptr.Deref(s.cfg.Status.Installed, false):
		if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
			return ctrl.Result{}, true, err
		}
		s.set(metav1.ConditionFalse, bootstrapv1.MachineDeletingReason, "The Machine is being deleted")
		s.nodeRemoved(metav1.ConditionTrue, bootstrapv1.DoneReason, "The machine was never installed; nothing is revoked")
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
	s.nodeRemoved(metav1.ConditionFalse, bootstrapv1.RevokingReason, "Revoking the node")
	if err := s.checkpoint(ctx); err != nil {
		return ctrl.Result{}, true, err
	}
	var tried []string
	for i := range cps {
		cp := &cps[i]
		cpAddr := machineutil.InternalIP(cp)
		if cpAddr == "" {
			continue
		}
		resp, err := r.revoke(ctx, cpAddr, sec, cp.Name, m.Name)
		var pruned string
		if sdkkubernetes.RevokedListFull(err) {
			// A full list may hold entries nothing lets back in any more: drop them and try once
			// more.
			pruned = r.prune(ctx, s, cpAddr, sec, cp.Name)
			resp, err = r.revoke(ctx, cpAddr, sec, cp.Name, m.Name)
		}
		switch {
		case err == nil:
			if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
				return ctrl.Result{}, true, err
			}
			s.nodeRemoved(metav1.ConditionTrue, bootstrapv1.DoneReason,
				fmt.Sprintf("Revoked through %s; the list of revoked nodes is %d of %d characters long", cp.Name, resp.GetListLength(), resp.GetListCapacity()))
			machineutil.Event(r.Recorder, s.cfg, m, corev1.EventTypeNormal, "NodeRevoked", "RevokeNode", "Revoked the node %s through the control plane %s", m.Name, cp.Name)
			ctrl.LoggerFrom(ctx).Info("Revoked the node and removed the pre-terminate hook", "controlPlane", cp.Name)
			return ctrl.Result{}, true, nil
		case sdkkubernetes.RevokedListFull(err):
			return r.hookWaits(ctx, s, bootstrapv1.RevokedListFullReason, fmt.Sprintf(
				"%v; %s; the Machine waits until there is room: rotate the cluster's CAs with chalkctl rotate os-ca and chalkctl rotate kubernetes-ca, "+
					"after which PruneRevoked drops the nodes revoked before them, or remove the annotation %s to delete the Machine without revoking its node",
				err, pruned, machineutil.HookAnnotation))
		case node.TryNextControlPlane(err):
			tried = append(tried, fmt.Sprintf("%s: %v", cp.Name, err))
		default:
			return r.hookWaits(ctx, s, bootstrapv1.RevokingReason, fmt.Sprintf("Revoking the node through %s: %v", cp.Name, err))
		}
	}
	msg := "No control-plane Machine has an address to revoke the node through"
	if len(tried) > 0 {
		msg = "No control plane revoked the node: " + strings.Join(tried, "; ")
	}
	return r.hookWaits(ctx, s, bootstrapv1.WaitingForControlPlaneReason, msg)
}

// revoke revokes the node worker through the control plane name at addr, within
// machineutil.HookCallTimeout.
func (r *ChalkosConfigReconciler) revoke(ctx context.Context, addr string, sec clustersecrets.Secrets, name, worker string) (*kubernetesv1.RevokeNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	return r.Node.RevokeNode(ctx, addr, sec, name, worker, false)
}

// prune has the control plane name at addr prune its list of revoked nodes, within
// machineutil.HookCallTimeout, and describes the outcome. A failure is reported, not returned:
// the revocation that follows tells whether there is room.
func (r *ChalkosConfigReconciler) prune(ctx context.Context, s *scope, addr string, sec clustersecrets.Secrets, name string) string {
	callCtx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	resp, err := r.Node.PruneRevoked(callCtx, addr, sec, name)
	if err != nil {
		ctrl.LoggerFrom(ctx).Info("Pruning the revoked nodes failed", "controlPlane", name, "error", err.Error())
		machineutil.Event(r.Recorder, s.cfg, s.machine, corev1.EventTypeWarning, "PruneRevokedFailed", "PruneRevoked", "%s",
			machineutil.Truncate(fmt.Sprintf("Pruning the revoked nodes through %s: %v", name, err)))
		return fmt.Sprintf("pruning the list through %s failed: %v", name, err)
	}
	ctrl.LoggerFrom(ctx).Info("Pruned the revoked nodes", "controlPlane", name, "pruned", resp.GetPruned())
	return fmt.Sprintf("pruning the list through %s dropped %d nodes, which made too little room", name, len(resp.GetPruned()))
}

// hookWaits reports a hook step that could not finish: the hook stays, a Warning event is
// recorded, and the step is tried again after machineutil.HookRetry.
func (r *ChalkosConfigReconciler) hookWaits(ctx context.Context, s *scope, reason, message string) (ctrl.Result, bool, error) {
	s.nodeRemoved(metav1.ConditionFalse, reason, message)
	machineutil.Event(r.Recorder, s.cfg, s.machine, corev1.EventTypeWarning, reason, "RevokeNode", "%s", machineutil.Truncate(message))
	ctrl.LoggerFrom(ctx).Info("The pre-terminate hook waits", "reason", reason, "message", message)
	return ctrl.Result{RequeueAfter: machineutil.HookRetry}, true, nil
}
