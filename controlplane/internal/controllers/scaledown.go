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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	"github.com/trevex/chalkos/pkg/clustersecrets"

	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// scaleDown deletes the control plane's oldest Machine while it has more than Spec.Replicas asks
// for, one at a time. A step starts only once the control plane is initialized, no Machine is
// being deleted, every Machine joined, and a control plane's etcd lists one healthy voting member
// per Machine. The Machine's pre-terminate hook takes it out of etcd and revokes it; the next step
// waits until it is gone. Spec.Replicas is at least one, so the last Machine is never deleted.
func (r *ChalkosControlPlaneReconciler) scaleDown(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	n, want := len(s.machines), max(1, int(ptr.Deref(s.cp.Spec.Replicas, 1)))
	if n <= want || !s.initialized() || len(s.deleting) > 0 {
		// A Machine being deleted is reported by its hook; its deletion reconciles again.
		return ctrl.Result{}, nil
	}
	for i := range s.machines {
		ok, err := r.joined(ctx, s, &s.machines[i], sec)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !ok {
			s.scaleDownWaiting = fmt.Sprintf("Waiting for the Machine %s to join the cluster before scaling down from %d to %d replicas", s.machines[i].Name, n, want)
			return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
		}
	}
	if why := r.etcdProblem(ctx, s, sec); why != "" {
		s.scaleDownWaiting = fmt.Sprintf("Not scaling down from %d to %d replicas yet: %s", n, want, why)
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	m := &s.machines[0]
	// The UID keeps another Machine of the same name, created since the list, from being deleted.
	if err := r.Client.Delete(ctx, m, client.Preconditions{UID: &m.UID}); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete the Machine %s: %w", m.Name, err)
	}
	ctrl.LoggerFrom(ctx).Info("Scaling down: deleting the oldest Machine", "machine", m.Name, "replicas", n, "want", want)
	machineutil.Event(r.Recorder, s.cp, m, corev1.EventTypeNormal, "ScalingDown", "DeleteMachine",
		"Deleting the Machine %s to scale down from %d to %d replicas", m.Name, n, want)
	s.removing = fmt.Sprintf("the Machine %s is deleted", m.Name)
	// The Machine's deletion reconciles again.
	return ctrl.Result{}, nil
}

// etcdProblem says why etcd may not gain or lose a member now, as the first of the control
// plane's Machines that answers lists its members, those not being deleted first; empty when
// membersProblem finds nothing.
func (r *ChalkosControlPlaneReconciler) etcdProblem(ctx context.Context, s *scope, sec clustersecrets.Secrets) string {
	passCtx, cancel := context.WithTimeout(ctx, machineutil.HookPassTimeout)
	defer cancel()
	var tried []string
	for _, ms := range [][]clusterv1.Machine{s.machines, s.deleting} {
		for i := range ms {
			m := &ms[i]
			addr := machineutil.InternalIP(m)
			if addr == "" {
				continue
			}
			callCtx, cancel := context.WithTimeout(passCtx, machineutil.HookCallTimeout)
			members, err := r.Node.EtcdMembers(callCtx, addr, sec, m.Name)
			cancel()
			switch {
			case err == nil:
				return membersProblem(members, s.machines, s.deleting)
			case node.TryNextControlPlane(err):
				tried = append(tried, fmt.Sprintf("%s: %v", m.Name, err))
			default:
				return fmt.Sprintf("listing etcd's members through %s: %v", m.Name, err)
			}
		}
	}
	if len(tried) == 0 {
		return "no control-plane Machine has an InternalIP to list etcd's members through"
	}
	return "no control plane lists etcd's members: " + strings.Join(tried, "; ")
}

// membersProblem says what keeps etcd's members from being one healthy voting member per
// Machine; empty when nothing does. Each Machine not being deleted has a member, and each
// member is a Machine's; a Machine being deleted may still have one, or may have left etcd.
func membersProblem(members []*kubernetesv1.EtcdMember, machines, deleting []clusterv1.Machine) string {
	if len(members) < len(machines) || len(members) > len(machines)+len(deleting) {
		msg := fmt.Sprintf("etcd has %s for %s", plural(len(members), "member"), plural(len(machines), "Machine"))
		if len(deleting) > 0 {
			msg += fmt.Sprintf(" and %d being deleted", len(deleting))
		}
		return msg
	}
	names := map[string]bool{}
	for _, ms := range [][]clusterv1.Machine{machines, deleting} {
		for i := range ms {
			names[ms[i].Name] = true
		}
	}
	has := map[string]bool{}
	for _, mem := range members {
		name := mem.GetName()
		if name == "" {
			// A learner that never started has no name yet, only its ID.
			name = fmt.Sprintf("%x", mem.GetId())
		}
		switch {
		case mem.GetLearner():
			return fmt.Sprintf("the member %s is a learner", name)
		case mem.GetUnhealthy() != "":
			return fmt.Sprintf("the member %s is unhealthy: %s", name, mem.GetUnhealthy())
		case !names[mem.GetName()]:
			return fmt.Sprintf("the member %q is no Machine's", name)
		}
		has[name] = true
	}
	for i := range machines {
		if !has[machines[i].Name] {
			return fmt.Sprintf("the Machine %s has no etcd member", machines[i].Name)
		}
	}
	return ""
}
