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

// scaleDown deletes a Machine that is not up to date, as oldest picks it, or else the control
// plane's oldest Machine, while it has more than Spec.Replicas asks for, one at a time. Among the
// Machines that are not up to date, one that etcd's member list shows no voting member of goes
// first. A step starts only once the control plane is initialized, no Machine is being deleted or
// was created or deleted in this reconcile, and stepProblem finds nothing for the Machine, whose
// own join is never waited for: a replacement that never joins goes once its change is reverted
// or fixed forward. The Machine's pre-terminate hook takes it out of etcd and revokes it; the next
// step waits until it is gone. Spec.Replicas is at least one, so the last Machine is never deleted.
func (r *ChalkosControlPlaneReconciler) scaleDown(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	n, want := len(s.machines), max(1, int(ptr.Deref(s.cp.Spec.Replicas, 1)))
	if n <= want || !s.initialized() || len(s.deleting) > 0 || s.stepped {
		// A Machine being deleted is reported by its hook; its deletion reconciles again, as does
		// a Machine's creation.
		return ctrl.Result{}, nil
	}
	// A Machine that is not up to date goes first, which a roll with maxSurge 1 relies on.
	m, err := r.oldest(ctx, s, sec, true)
	if err == nil && m == nil {
		m, err = r.oldest(ctx, s, sec, false)
	}
	if err != nil {
		return ctrl.Result{}, err
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

// etcdProblem says why etcd may not gain or lose a member now, as listMembers reads etcd's members;
// empty when membersProblem finds nothing. leaving, when not nil, is the Machine about to be
// deleted, which may hold an unhealthy voting member; the members are not listed through it. When
// it holds no vote, as noVote tells, it counts as being deleted and its learner is left out, which
// its hook removes.
func (r *ChalkosControlPlaneReconciler) etcdProblem(ctx context.Context, s *scope, sec clustersecrets.Secrets, leaving *clusterv1.Machine, noVote bool) string {
	machines, deleting := s.machines, s.deleting
	name := ""
	if leaving != nil {
		name = leaving.Name
		if noVote {
			machines = slices.DeleteFunc(slices.Clone(machines), func(m clusterv1.Machine) bool { return m.Name == leaving.Name })
			deleting = append(slices.Clone(deleting), *leaving)
		}
	}
	members, why := r.listMembers(ctx, s, sec, name)
	if why != "" {
		return why
	}
	if noVote {
		members = withoutLearnerOf(members, leaving)
	}
	return membersProblem(members, machines, deleting, name)
}

// memberList is etcd's member list as one Machine's node answered it.
type memberList struct {
	members []*kubernetesv1.EtcdMember
	err     error
}

// listMembers lists etcd's members through the first of the control plane's Machines other than
// except that answers, those not being deleted first, asking each node once per reconcile. why
// says, when none answers, what each answered; empty otherwise.
func (r *ChalkosControlPlaneReconciler) listMembers(ctx context.Context, s *scope, sec clustersecrets.Secrets, except string) (_ []*kubernetesv1.EtcdMember, why string) {
	passCtx, cancel := context.WithTimeout(ctx, machineutil.HookPassTimeout)
	defer cancel()
	var tried []string
	for _, ms := range [][]clusterv1.Machine{s.machines, s.deleting} {
		for i := range ms {
			m := &ms[i]
			addr := machineutil.InternalIP(m)
			if addr == "" || m.Name == except {
				continue
			}
			l, ok := s.memberLists[m.Name]
			if !ok {
				callCtx, cancel := context.WithTimeout(passCtx, machineutil.HookCallTimeout)
				l.members, l.err = r.Node.EtcdMembers(callCtx, addr, sec, m.Name)
				cancel()
				if l.err != nil {
					ctrl.LoggerFrom(ctx).Info("Listing etcd's members failed", "through", m.Name, "error", l.err.Error())
				}
				if s.memberLists == nil {
					s.memberLists = map[string]memberList{}
				}
				s.memberLists[m.Name] = l
			}
			switch {
			case l.err == nil:
				return l.members, ""
			case node.TryNextControlPlane(l.err):
				tried = append(tried, fmt.Sprintf("%s: %s", m.Name, node.ConditionError(l.err)))
			default:
				return nil, fmt.Sprintf("listing etcd's members through %s: %s", m.Name, node.ConditionError(l.err))
			}
		}
	}
	switch {
	case len(tried) > 0:
		return nil, "no control plane lists etcd's members: " + strings.Join(tried, "; ")
	case except != "":
		return nil, fmt.Sprintf("no control-plane Machine other than %s, which is to be deleted, has an InternalIP to list etcd's members through", except)
	}
	return nil, "no control-plane Machine has an InternalIP to list etcd's members through"
}

// holdsVote tells whether etcd's members, as listMembers reads them through the control plane's
// other Machines, hold a voting member of the Machine m, by its name or at its peer URL. A
// learner or no member is no vote. known is false when no other Machine lists the members.
func (r *ChalkosControlPlaneReconciler) holdsVote(ctx context.Context, s *scope, sec clustersecrets.Secrets, m *clusterv1.Machine) (vote, known bool) {
	members, why := r.listMembers(ctx, s, sec, m.Name)
	if why != "" {
		return false, false
	}
	urls := etcdPeerURLs(m)
	return slices.ContainsFunc(members, func(mem *kubernetesv1.EtcdMember) bool {
		return !mem.GetLearner() && (mem.GetName() == m.Name || slices.ContainsFunc(mem.GetPeerUrls(), func(u string) bool { return slices.Contains(urls, u) }))
	}), true
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
// leaving, when not empty, names the Machine the step deletes. Its voting member may be
// unhealthy, as Cluster API's own control plane allows, when every other member is a healthy
// voter of another Machine and those make a quorum of the members as they are.
func membersProblem(members []*kubernetesv1.EtcdMember, machines, deleting []clusterv1.Machine, leaving string) string {
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
	unhealthy := false
	for _, mem := range members {
		name := mem.GetName()
		if name == "" {
			// A learner that never started has no name yet, only its ID.
			name = fmt.Sprintf("%x", mem.GetId())
		}
		switch {
		case mem.GetLearner():
			return fmt.Sprintf("the member %s is a learner", name)
		case mem.GetUnhealthy() != "" && leaving != "" && mem.GetName() == leaving:
			unhealthy = true
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
	if !unhealthy {
		return ""
	}
	if i := slices.IndexFunc(deleting, func(m clusterv1.Machine) bool { return m.Name != leaving }); i >= 0 {
		return fmt.Sprintf("the member %s is unhealthy, and the Machine %s is being deleted", leaving, deleting[i].Name)
	}
	// Removing the unhealthy member changes etcd's membership, which commits only with a quorum of
	// the n members as they are. Every other member is a healthy voter here, so n-1 of them answer:
	// a quorum from three members on, but with two, one of them unhealthy, etcd has lost its quorum
	// and the removal could not commit. The n-1 healthy voters left are then a quorum of themselves.
	if n := len(members); n-1 < quorum(n) {
		return fmt.Sprintf("the member %s is unhealthy, and %d healthy of %d members are no quorum to remove it", leaving, n-1, n)
	}
	return ""
}

// quorum is the quorum of n etcd voters: a majority, n/2+1 with n/2 rounded down.
func quorum(n int) int { return n/2 + 1 }
