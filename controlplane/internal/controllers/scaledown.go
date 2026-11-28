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
	"slices"
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

// scaleDown deletes the control plane's oldest Machine that is not up to date, or else its oldest
// Machine, while it has more than Spec.Replicas asks for, one at a time. A step starts only once
// the control plane is initialized, no Machine is being deleted or was created or deleted in this
// reconcile, and stepProblem finds nothing for the Machine. A Machine that has not joined goes
// without its join, as a replacement that never joins once its change is reverted. The Machine's
// pre-terminate hook takes it out of etcd and revokes it; the next step waits until it is gone.
// Spec.Replicas is at least one, so the last Machine is never deleted.
func (r *ChalkosControlPlaneReconciler) scaleDown(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	n, want := len(s.machines), max(1, int(ptr.Deref(s.cp.Spec.Replicas, 1)))
	if n <= want || !s.initialized() || len(s.deleting) > 0 || s.stepped {
		// A Machine being deleted is reported by its hook; its deletion reconciles again, as does
		// a Machine's creation.
		return ctrl.Result{}, nil
	}
	// A Machine that is not up to date goes first, which a roll with maxSurge 1 relies on.
	m := s.oldestOutdated()
	if m == nil {
		m = &s.machines[0]
	}
	why, err := r.stepProblem(ctx, s, sec, m)
	if err != nil {
		return ctrl.Result{}, err
	}
	if why != "" {
		s.scaleDownWaiting = why
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	if err := r.deleteMachine(ctx, m); err != nil {
		return ctrl.Result{}, err
	}
	ctrl.LoggerFrom(ctx).Info("Scaling down: deleting a Machine", "machine", m.Name, "outOfDate", s.outOfDate(m) != "", "replicas", n, "want", want)
	machineutil.Event(r.Recorder, s.cp, m, corev1.EventTypeNormal, "ScalingDown", "DeleteMachine",
		"Deleting the Machine %s to scale down from %d to %d replicas", m.Name, n, want)
	s.removing = fmt.Sprintf("the Machine %s is deleted", m.Name)
	s.stepped = true
	// The Machine's deletion reconciles again.
	return ctrl.Result{}, nil
}

// deleteMachine deletes the Machine m, which its pre-terminate hook then takes out of etcd. The
// UID keeps another Machine of the same name, created since the list, from being deleted.
func (r *ChalkosControlPlaneReconciler) deleteMachine(ctx context.Context, m *clusterv1.Machine) error {
	if err := r.Client.Delete(ctx, m, client.Preconditions{UID: &m.UID}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete the Machine %s: %w", m.Name, err)
	}
	return nil
}

// etcdProblem says why etcd may not gain or lose a member now, as the first of the control
// plane's Machines that answers lists its members, those not being deleted first; empty when
// membersProblem finds nothing. unjoined, when not nil, is a Machine that has not joined and is
// about to be deleted: it counts as being deleted, its learner is left out, which its hook
// removes, and the members are not listed through it.
func (r *ChalkosControlPlaneReconciler) etcdProblem(ctx context.Context, s *scope, sec clustersecrets.Secrets, unjoined *clusterv1.Machine) string {
	passCtx, cancel := context.WithTimeout(ctx, machineutil.HookPassTimeout)
	defer cancel()
	machines, deleting := s.machines, s.deleting
	if unjoined != nil {
		machines = slices.DeleteFunc(slices.Clone(machines), func(m clusterv1.Machine) bool { return m.Name == unjoined.Name })
	}
	var tried []string
	for _, ms := range [][]clusterv1.Machine{machines, deleting} {
		for i := range ms {
			m := &ms[i]
			addr := machineutil.InternalIP(m)
			if addr == "" {
				continue
			}
			callCtx, cancel := context.WithTimeout(passCtx, machineutil.HookCallTimeout)
			members, err := r.Node.EtcdMembers(callCtx, addr, sec, m.Name)
			cancel()
			if err != nil {
				ctrl.LoggerFrom(ctx).Info("Listing etcd's members failed", "through", m.Name, "error", err.Error())
			}
			switch {
			case err == nil:
				if unjoined != nil {
					return membersProblem(withoutLearnerOf(members, unjoined), machines, append(slices.Clone(deleting), *unjoined))
				}
				return membersProblem(members, machines, deleting)
			case node.TryNextControlPlane(err):
				tried = append(tried, fmt.Sprintf("%s: %s", m.Name, node.ConditionError(err)))
			default:
				return fmt.Sprintf("listing etcd's members through %s: %s", m.Name, node.ConditionError(err))
			}
		}
	}
	if len(tried) == 0 {
		return "no control-plane Machine has an InternalIP to list etcd's members through"
	}
	return "no control plane lists etcd's members: " + strings.Join(tried, "; ")
}

// withoutLearnerOf are the members without the Machine's learner, found by its name or, for a
// learner that never started and has no name yet, by its peer URL.
func withoutLearnerOf(members []*kubernetesv1.EtcdMember, m *clusterv1.Machine) []*kubernetesv1.EtcdMember {
	urls := etcdPeerURLs(m)
	return slices.DeleteFunc(slices.Clone(members), func(mem *kubernetesv1.EtcdMember) bool {
		switch {
		case !mem.GetLearner():
			return false
		case mem.GetName() != "":
			return mem.GetName() == m.Name
		}
		return slices.ContainsFunc(mem.GetPeerUrls(), func(u string) bool { return slices.Contains(urls, u) })
	})
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
			// Without etcd's error, whose text varies from one health check to the next.
			return fmt.Sprintf("the member %s is unhealthy", name)
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
