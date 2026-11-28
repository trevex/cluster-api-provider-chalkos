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
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	chalkoskubernetes "github.com/trevex/chalkos/pkg/kubernetes"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// addHooks gives the control plane's Machines that are not being deleted the hook, as those
// created before it existed lack it.
func (r *ChalkosControlPlaneReconciler) addHooks(ctx context.Context, s *scope) error {
	for i := range s.machines {
		added, err := machineutil.AddHook(ctx, r.Client, &s.machines[i], machineutil.ControlPlaneHookOwner)
		if err != nil {
			return err
		}
		if added {
			ctrl.LoggerFrom(ctx).Info("Added the pre-terminate hook", "machine", s.machines[i].Name)
		}
	}
	return nil
}

// removeAllHooks removes the hook of every Machine of the control plane: the whole Cluster is
// being deleted, so no Machine leaves etcd and none is revoked.
func (r *ChalkosControlPlaneReconciler) removeAllHooks(ctx context.Context, s *scope) error {
	for _, ms := range [][]clusterv1.Machine{s.machines, s.deleting} {
		for i := range ms {
			if err := machineutil.RemoveHook(ctx, r.Client, &ms[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// pruneInterval is how often the controller has a control plane prune the list of revoked nodes
// when no hook revoked one meanwhile.
const pruneInterval = 24 * time.Hour

// errNoAddress is the error of a Machine without an InternalIP to reach it at.
var errNoAddress = errors.New("the Machine has no InternalIP")

// reconcileHooks runs the pre-terminate hook of the control plane's Machines being deleted, one
// at a time, oldest deletion first. A Machine whose machine was never reached to be installed
// loses its hook at once. Once Cluster API waits for the hook, a Machine leaves etcd and is
// revoked through another control plane, and loses its hook. The last Machine of a control plane
// being deleted, or not initialized, leaves no etcd and is not revoked. A Machine of an
// initialized control plane whose peers hold no voting etcd member is etcd's last voter: it holds
// until a peer joined etcd as a voter, such as its replacement, which scaleUp creates. A held hook
// changes nothing, so the next Machine's hook runs meanwhile: a replacement that never joins and is
// deleted in turn must not wait for the Machine it was to replace. Every step can be repeated; one
// that cannot finish keeps the hook and is tried again after machineutil.HookRetry.
func (r *ChalkosControlPlaneReconciler) reconcileHooks(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	pending := slices.DeleteFunc(slices.Clone(s.deleting), func(m clusterv1.Machine) bool { return !machineutil.HasHook(&m) })
	slices.SortFunc(pending, func(a, b clusterv1.Machine) int {
		if c := a.DeletionTimestamp.Compare(b.DeletionTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	var held, heldReason string
	for i := range pending {
		res, hold, err := r.reconcileHook(ctx, s, &pending[i], sec)
		if err != nil {
			return res, err
		}
		if !hold {
			if s.removing == "" {
				// The hook finished: the conditions report the Machine that holds, if one does.
				s.removing, s.removingReason = held, heldReason
			}
			if held != "" {
				res = earliest(res, ctrl.Result{RequeueAfter: machineutil.HookRetry})
			}
			return res, nil
		}
		if held == "" {
			held, heldReason = s.removing, s.removingReason
		}
	}
	if held == "" {
		return ctrl.Result{}, nil
	}
	// No watch reports a peer that joined etcd as a voter; the requeue does.
	s.removing, s.removingReason = held, heldReason
	return ctrl.Result{RequeueAfter: machineutil.HookRetry}, nil
}

// reconcileHook runs one step of the pre-terminate hook of the Machine m, as reconcileHooks
// describes. hold is set when the Machine is etcd's last voter and waits for a peer to join etcd
// as one, having changed nothing.
func (r *ChalkosControlPlaneReconciler) reconcileHook(ctx context.Context, s *scope, m *clusterv1.Machine, sec clustersecrets.Secrets) (_ ctrl.Result, hold bool, _ error) {
	log := ctrl.LoggerFrom(ctx).WithValues("machine", m.Name)
	reached, err := r.everReached(ctx, m)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if !reached {
		log.Info("Removed the pre-terminate hook of a Machine whose machine was never reached")
		return ctrl.Result{}, false, machineutil.RemoveHook(ctx, r.Client, m)
	}
	if !machineutil.WaitingForHook(m) {
		// The Machines' watch reconciles again once Cluster API waits for the hook.
		s.removing = fmt.Sprintf("the Machine %s is drained before it leaves etcd", m.Name)
		return ctrl.Result{}, false, nil
	}
	var peers []clusterv1.Machine
	for _, ms := range [][]clusterv1.Machine{s.machines, s.deleting} {
		for _, p := range ms {
			if p.Name != m.Name {
				peers = append(peers, p)
			}
		}
	}
	// The calls to the nodes share one deadline; the writes to the management cluster do not.
	passCtx, cancel := context.WithTimeout(ctx, machineutil.HookPassTimeout)
	defer cancel()
	if s.initialized() && (len(peers) > 0 || s.cp.DeletionTimestamp.IsZero()) {
		voter, why, err := r.votingPeer(passCtx, s, m, peers, sec)
		if err != nil {
			return ctrl.Result{}, false, err
		}
		if !voter {
			s.removing, s.removingReason = lastVoterMessage(s, m, why), controlplanev1.ScalingDownReason
			return ctrl.Result{}, true, nil
		}
	}
	if len(peers) > 0 && (!r.leaveEtcd(passCtx, s, m, peers, sec) || !r.revokeControlPlane(passCtx, s, m, peers, sec)) {
		return ctrl.Result{RequeueAfter: machineutil.HookRetry}, false, nil
	}
	if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
		return ctrl.Result{}, false, err
	}
	if len(peers) == 0 {
		machineutil.Event(r.Recorder, s.cp, m, corev1.EventTypeNormal, "ControlPlaneRemoved", "RemoveHook",
			"The Machine %s is the last control plane; it does not leave etcd and is not revoked", m.Name)
	} else {
		machineutil.Event(r.Recorder, s.cp, m, corev1.EventTypeNormal, "ControlPlaneRemoved", "RemoveHook",
			"The Machine %s left etcd and its node was revoked", m.Name)
	}
	log.Info("Removed the pre-terminate hook", "last", len(peers) == 0)
	s.removing, s.removingReason = "", ""
	// The next Machine's hook, if any, runs at the next reconcile, which the Machine's patch starts.
	return ctrl.Result{}, false, nil
}

// votingPeer tells whether one of the peers of the Machine m holds a voting etcd member: a peer
// that is not being deleted joined the cluster, or a member that a peer's name names is started,
// healthy and no learner, as the first of m and its peers that lists etcd's members answers. why
// says, when none answers, what each answered; empty otherwise.
func (r *ChalkosControlPlaneReconciler) votingPeer(ctx context.Context, s *scope, m *clusterv1.Machine, peers []clusterv1.Machine, sec clustersecrets.Secrets) (voter bool, why string, _ error) {
	if len(peers) == 0 {
		return false, "", nil
	}
	names := map[string]bool{}
	for i := range peers {
		p := &peers[i]
		names[p.Name] = true
		if !s.isDeleting(p.Name) {
			ok, err := r.joined(ctx, s, p, sec)
			if err != nil || ok {
				return ok, "", err
			}
		}
	}
	var tried []string
	for _, c := range append([]clusterv1.Machine{*m}, peers...) {
		addr := machineutil.InternalIP(&c)
		if addr == "" {
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
		members, err := r.Node.EtcdMembers(callCtx, addr, sec, c.Name)
		cancel()
		if err != nil {
			ctrl.LoggerFrom(ctx).Info("Listing etcd's members failed", "machine", m.Name, "through", c.Name, "error", err.Error())
			tried = append(tried, fmt.Sprintf("%s: %s", c.Name, node.ConditionError(err)))
			continue
		}
		return slices.ContainsFunc(members, func(mem *kubernetesv1.EtcdMember) bool {
			return names[mem.GetName()] && !mem.GetLearner() && mem.GetUnhealthy() == ""
		}), "", nil
	}
	if len(tried) == 0 {
		tried = append(tried, "no control-plane Machine has an InternalIP")
	}
	return false, strings.Join(tried, "; "), nil
}

// lastVoterMessage is what the hook of the Machine m, etcd's last voter, holds for: its
// replacement, or the other Machines of a control plane being deleted. why is what the control
// planes answered when none listed etcd's members. A hold that may last for good names the
// break-glass.
func lastVoterMessage(s *scope, m *clusterv1.Machine, why string) string {
	breakGlass := fmt.Sprintf("removing the annotation %s deletes the Machine without taking it out of etcd and revoking it, which loses the cluster's etcd unless another member votes",
		machineutil.HookAnnotation)
	switch {
	case why != "":
		return fmt.Sprintf("the Machine %s waits for a control plane to list etcd's members, to tell whether another one votes in etcd: %s; %s", m.Name, why, breakGlass)
	case s.cp.DeletionTimestamp.IsZero():
		return fmt.Sprintf("the Machine %s waits for its replacement control plane to join etcd; %s", m.Name, breakGlass)
	}
	return fmt.Sprintf("the Machine %s is etcd's last voter and leaves after the other Machines", m.Name)
}

// everReached tells whether the bootstrap provider ever reached the Machine's machine to install
// it, which may have given it certificates: its ChalkosConfig records the machine installed, an
// install, or a maintenance-mode fingerprint. A Machine without its ChalkosConfig counts as
// reached, since nothing tells otherwise. It reads the ChalkosConfig uncached: an install the
// bootstrap controller recorded just now must count.
func (r *ChalkosControlPlaneReconciler) everReached(ctx context.Context, m *clusterv1.Machine) (bool, error) {
	var cfg bootstrapv1.ChalkosConfig
	err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, &cfg)
	switch {
	case apierrors.IsNotFound(err):
		return true, nil
	case err != nil:
		return false, err
	}
	st := cfg.Status
	return ptr.Deref(st.Installed, false) || st.LastInstallTime != nil || len(st.MaintenanceFingerprints) > 0, nil
}

// leaveEtcd takes the Machine's node out of etcd: the node leaves, or, when it cannot be reached
// as itself, a peer removes its member. A node that is no member, or a member etcd does not list,
// counts as out. It tells whether the node is out; otherwise s says what the step waits on.
func (r *ChalkosControlPlaneReconciler) leaveEtcd(ctx context.Context, s *scope, m *clusterv1.Machine, peers []clusterv1.Machine, sec clustersecrets.Secrets) bool {
	addr := machineutil.InternalIP(m)
	err := errNoAddress
	if addr != "" {
		callCtx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
		err = r.Node.EtcdLeave(callCtx, addr, sec, m.Name)
		cancel()
	}
	switch {
	case err == nil, sdkkubernetes.NotEtcdMember(err):
		return true
	case !errors.Is(err, errNoAddress) && !node.Unreachable(err):
		// Only the node itself may answer for its leave: another control plane is not asked. A node
		// whose own etcd member does not answer refuses to leave through the other members alone;
		// once someone removed that member through them, the node is no member and the leave done.
		ctrl.LoggerFrom(ctx).Info("The node does not leave etcd", "machine", m.Name, "error", err.Error())
		msg := fmt.Sprintf("the Machine %s does not leave etcd: %s", m.Name, node.ConditionError(err))
		if connect.CodeOf(err) == connect.CodeFailedPrecondition {
			msg += fmt.Sprintf("; when its own etcd member does not answer, remove the member through another control plane with chalkctl etcd remove-member %s, and the hook goes on", m.Name)
		}
		return r.hookWaits(s, m, "EtcdLeave", controlplanev1.ScalingDownReason, msg)
	}
	var tried []string
	for i := range peers {
		p := &peers[i]
		paddr := machineutil.InternalIP(p)
		if paddr == "" {
			continue
		}
		perr := r.removeMember(ctx, paddr, sec, p.Name, m)
		if perr == nil {
			ctrl.LoggerFrom(ctx).Info("Removed the etcd member of an unreachable Machine", "machine", m.Name, "through", p.Name, "error", err.Error())
			return true
		}
		ctrl.LoggerFrom(ctx).Info("A control plane did not remove the etcd member", "machine", m.Name, "through", p.Name, "error", perr.Error())
		tried = append(tried, fmt.Sprintf("%s: %s", p.Name, node.ConditionError(perr)))
		if connect.CodeOf(perr) == connect.CodeFailedPrecondition && !sdkkubernetes.NotEtcdMember(perr) {
			// An etcd member refused the removal, as for etcd's quorum: every other member guards
			// it the same way.
			break
		}
	}
	if len(tried) == 0 {
		tried = append(tried, "no other control-plane Machine has an InternalIP")
	}
	return r.hookWaits(s, m, "EtcdRemoveMember", controlplanev1.ScalingDownReason, fmt.Sprintf(
		"the Machine %s cannot be reached (%s), and no other control plane removes its etcd member: %s", m.Name, node.ConditionError(err), strings.Join(tried, "; ")))
}

// removeMember removes the etcd member of the Machine's node through the control plane via at
// addr. A member etcd does not list is removed already, unless etcd lists a learner without a
// name at the node's peer URL: a learner that never started, which only its ID finds. chalkd
// builds the peer URL from one of the node's addresses, not necessarily the Machine's first
// InternalIP, so each of the Machine's InternalIPs is matched.
func (r *ChalkosControlPlaneReconciler) removeMember(ctx context.Context, addr string, sec clustersecrets.Secrets, via string, m *clusterv1.Machine) error {
	callCtx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	err := r.Node.EtcdRemoveMember(callCtx, addr, sec, via, m.Name)
	if connect.CodeOf(err) != connect.CodeNotFound {
		return err
	}
	members, err := r.Node.EtcdMembers(callCtx, addr, sec, via)
	if err != nil {
		return fmt.Errorf("etcd lists no member %s, and listing its members to find an unstarted learner failed: %w", m.Name, err)
	}
	peerURLs := etcdPeerURLs(m)
	for _, mem := range members {
		if mem.GetName() != "" {
			continue
		}
		if len(peerURLs) == 0 {
			return fmt.Errorf("etcd lists a learner without a name, %x, and the Machine has no address to tell whether it is its own", mem.GetId())
		}
		if slices.ContainsFunc(mem.GetPeerUrls(), func(u string) bool { return slices.Contains(peerURLs, u) }) {
			err := r.Node.EtcdRemoveMember(callCtx, addr, sec, via, fmt.Sprintf("%x", mem.GetId()))
			if connect.CodeOf(err) == connect.CodeNotFound {
				return nil
			}
			return err
		}
	}
	return nil
}

// etcdPeerURLs are the etcd peer URLs chalkd may build for the Machine's node, one per InternalIP.
func etcdPeerURLs(m *clusterv1.Machine) []string {
	var urls []string
	for _, a := range machineutil.InternalIPs(m) {
		if ip, err := netip.ParseAddr(a); err == nil {
			urls = append(urls, chalkoskubernetes.EtcdPeerURL(ip))
		}
	}
	return urls
}

// revokeControlPlane revokes the Machine's node, a control plane, through the first peer that
// does, and then has that peer prune the list of revoked nodes. A full list is pruned, unless the
// last prune was less than machineutil.FullListPruneInterval ago, and the revocation tried again.
// It tells whether the node is revoked; otherwise s says what the step waits on.
func (r *ChalkosControlPlaneReconciler) revokeControlPlane(ctx context.Context, s *scope, m *clusterv1.Machine, peers []clusterv1.Machine, sec clustersecrets.Secrets) bool {
	var tried []string
	for i := range peers {
		p := &peers[i]
		addr := machineutil.InternalIP(p)
		if addr == "" {
			continue
		}
		_, err := r.revoke(ctx, addr, sec, p.Name, m.Name)
		pruned := false
		if sdkkubernetes.RevokedListFull(err) && r.mayPruneFullList(s) {
			// A full list may hold entries nothing lets back in any more: drop them and try once
			// more.
			r.prune(ctx, s, addr, sec, p.Name)
			pruned = true
			_, err = r.revoke(ctx, addr, sec, p.Name, m.Name)
		}
		switch {
		case err == nil:
			ctrl.LoggerFrom(ctx).Info("Revoked the node of the Machine", "machine", m.Name, "through", p.Name)
			if !pruned {
				r.prune(ctx, s, addr, sec, p.Name)
			}
			return true
		case sdkkubernetes.RevokedListFull(err):
			// The list is the cluster's: another control plane has no more room. The message
			// leaves out the prune, which only some passes make, so that it stays the same.
			return r.hookWaits(s, m, "RevokeNode", controlplanev1.RevokedListFullReason, fmt.Sprintf(
				"the node of the Machine %s is not revoked: %s; PruneRevoked made too little room; rotate the cluster's CAs with chalkctl rotate os-ca and chalkctl rotate kubernetes-ca, "+
					"after which PruneRevoked drops the nodes revoked before them, or remove the annotation %s to delete the Machine without revoking its node",
				m.Name, node.ConditionError(err), machineutil.HookAnnotation))
		}
		ctrl.LoggerFrom(ctx).Info("A control plane did not revoke the node", "machine", m.Name, "through", p.Name, "error", err.Error())
		tried = append(tried, fmt.Sprintf("%s: %s", p.Name, node.ConditionError(err)))
	}
	if len(tried) == 0 {
		tried = append(tried, "no other control-plane Machine has an InternalIP")
	}
	return r.hookWaits(s, m, "RevokeNode", controlplanev1.ScalingDownReason,
		fmt.Sprintf("no control plane revokes the node of the Machine %s: %s", m.Name, strings.Join(tried, "; ")))
}

// revoke revokes the control plane's node through the control plane via at addr, within
// machineutil.HookCallTimeout.
func (r *ChalkosControlPlaneReconciler) revoke(ctx context.Context, addr string, sec clustersecrets.Secrets, via, name string) (*kubernetesv1.RevokeNodeResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	return r.Node.RevokeNode(ctx, addr, sec, via, name, true)
}

// mayPruneFullList tells whether a hook that finds the list of revoked nodes full prunes it: the
// last prune, if any, was at least machineutil.FullListPruneInterval ago. Each prune records its
// time in the status, whose write reconciles again at once: without the interval, a full list
// would be pruned at every pass.
func (r *ChalkosControlPlaneReconciler) mayPruneFullList(s *scope) bool {
	last := s.cp.Status.LastRevokedPruneTime
	return last == nil || !r.clock().Now().Before(last.Add(machineutil.FullListPruneInterval))
}

// prune has the control plane name at addr prune the list of revoked nodes, within
// machineutil.HookCallTimeout, and records the attempt in LastRevokedPruneTime. A failure is
// reported in a log line and a Warning event, not returned: pruning is best effort.
func (r *ChalkosControlPlaneReconciler) prune(ctx context.Context, s *scope, addr string, sec clustersecrets.Secrets, name string) {
	callCtx, cancel := context.WithTimeout(ctx, machineutil.HookCallTimeout)
	defer cancel()
	s.cp.Status.LastRevokedPruneTime = ptr.To(metav1.NewTime(r.clock().Now()))
	resp, err := r.Node.PruneRevoked(callCtx, addr, sec, name)
	if err != nil {
		ctrl.LoggerFrom(ctx).Info("Pruning the revoked nodes failed", "controlPlane", name, "error", err.Error())
		machineutil.Event(r.Recorder, s.cp, nil, corev1.EventTypeWarning, "PruneRevokedFailed", "PruneRevoked", "%s",
			machineutil.Truncate(fmt.Sprintf("Pruning the revoked nodes through %s: %v", name, err)))
		return
	}
	ctrl.LoggerFrom(ctx).Info("Pruned the revoked nodes", "controlPlane", name, "pruned", resp.GetPruned(),
		"length", resp.GetListLength(), "capacity", resp.GetListCapacity())
}

// reconcilePrune has a control plane prune the list of revoked nodes once pruneInterval passed
// since the last attempt, through the first of the control plane's Machines that answers. The
// workers' revocations are pruned this way too. It returns when to prune next; a failure only
// waits for the next attempt.
func (r *ChalkosControlPlaneReconciler) reconcilePrune(ctx context.Context, s *scope, sec clustersecrets.Secrets) ctrl.Result {
	now := r.clock().Now()
	if last := s.cp.Status.LastRevokedPruneTime; last != nil {
		if wait := last.Add(pruneInterval).Sub(now); wait > 0 {
			return ctrl.Result{RequeueAfter: wait}
		}
	}
	passCtx, cancel := context.WithTimeout(ctx, machineutil.HookPassTimeout)
	defer cancel()
	var tried []string
	for i := range s.machines {
		m := &s.machines[i]
		addr := machineutil.InternalIP(m)
		if addr == "" {
			continue
		}
		callCtx, cancel := context.WithTimeout(passCtx, machineutil.HookCallTimeout)
		resp, err := r.Node.PruneRevoked(callCtx, addr, sec, m.Name)
		cancel()
		if err == nil {
			s.cp.Status.LastRevokedPruneTime = ptr.To(metav1.NewTime(now))
			ctrl.LoggerFrom(ctx).Info("Pruned the revoked nodes", "controlPlane", m.Name, "pruned", resp.GetPruned(),
				"length", resp.GetListLength(), "capacity", resp.GetListCapacity())
			return ctrl.Result{RequeueAfter: pruneInterval}
		}
		tried = append(tried, fmt.Sprintf("%s: %v", m.Name, err))
	}
	if len(tried) == 0 {
		// No Machine has an address yet: try again once one has.
		return ctrl.Result{}
	}
	s.cp.Status.LastRevokedPruneTime = ptr.To(metav1.NewTime(now))
	msg := "Pruning the revoked nodes failed: " + strings.Join(tried, "; ")
	ctrl.LoggerFrom(ctx).Info(msg)
	machineutil.Event(r.Recorder, s.cp, nil, corev1.EventTypeWarning, "PruneRevokedFailed", "PruneRevoked", "%s", machineutil.Truncate(msg))
	return ctrl.Result{RequeueAfter: pruneInterval}
}

// hookWaits reports a hook step that could not finish: the hook stays, the conditions say what it
// waits on, and a Warning event is recorded. It returns false, for the step's result.
func (r *ChalkosControlPlaneReconciler) hookWaits(s *scope, m *clusterv1.Machine, action, reason, message string) bool {
	s.removing, s.removingReason = message, reason
	machineutil.Event(r.Recorder, s.cp, m, corev1.EventTypeWarning, "PreTerminateHookWaits", action, "%s", machineutil.Truncate(message))
	return false
}
