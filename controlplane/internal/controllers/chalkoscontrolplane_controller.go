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

// Package controllers holds the control-plane provider's reconciler, which creates a cluster's
// control-plane Machines one at a time, bootstraps the cluster on the first over chalkd's API and
// keeps the cluster's kubeconfig Secret.
package controllers

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/finalizers"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/paused"
	"sigs.k8s.io/cluster-api/util/predicates"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	"github.com/trevex/chalkos/pkg/clustersecrets"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/conditionlog"
	"github.com/trevex/cluster-api-provider-chalkos/internal/images"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

const (
	// DefaultBootstrapTimeout is how long a bootstrap call may take when the reconciler names no
	// timeout. chalkd commits the node to the bootstrap before it waits for the cluster's add-ons,
	// so a call that times out goes on on the node, and the reconciler follows it through the
	// node's status.
	DefaultBootstrapTimeout = 2 * time.Minute
)

// controlPlaneKind is the ChalkosControlPlane's group, version and kind.
var controlPlaneKind = controlplanev1.GroupVersion.WithKind("ChalkosControlPlane")

// ChalkosControlPlaneReconciler creates the control-plane Machines of each ChalkosControlPlane
// one at a time, bootstraps the cluster on the first, and keeps the cluster's secrets and its
// kubeconfig Secret.
type ChalkosControlPlaneReconciler struct {
	// Client reads and writes the management cluster's objects, from the manager's cache.
	Client client.Client
	// APIReader reads uncached: the ChalkosControlPlane, its Machines and the ChalkosConfig of a
	// Machine whose hook runs, whose writes the next reconcile must see, and the infrastructure
	// templates and their CRDs and the cluster's secrets and kubeconfig Secrets, none of which the
	// controller watches.
	APIReader client.Reader
	// Node reaches the machines.
	Node node.Client
	// BootstrapTimeout bounds one bootstrap call; zero is DefaultBootstrapTimeout.
	BootstrapTimeout time.Duration
	// MaxConcurrentReconciles is how many ChalkosControlPlanes are reconciled at once; zero is one.
	MaxConcurrentReconciles int
	// WatchFilterValue, when set, limits the controller to objects whose
	// cluster.x-k8s.io/watch-filter label has this value.
	WatchFilterValue string
	// Clock tells the time the cluster's secrets and kubeconfig certificates are issued at; nil is
	// the real clock.
	Clock clock.PassiveClock
	// Recorder records the events of the control planes' pre-terminate hooks and of the pruning of
	// the revoked nodes; nil records none.
	Recorder events.EventRecorder

	// artifacts holds the images read before, whose contracts default status.observedFiles and
	// mark the fields the guard compares.
	artifacts images.Cache
	// contracts holds the contracts of the images read before.
	contracts contractCache
}

// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=chalkoscontrolplanes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=chalkoscontrolplanes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=chalkoscontrolplanes/finalizers,verbs=update
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=chalkosconfigs,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines/status,verbs=patch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machinepools,verbs=list
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=*,verbs=get;create;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;create;update
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// SetupWithManager registers the reconciler with mgr.
func (r *ChalkosControlPlaneReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	log := ctrl.LoggerFrom(ctx).WithValues("controller", "chalkoscontrolplane")
	scheme := mgr.GetScheme()
	return ctrl.NewControllerManagedBy(mgr).
		For(&controlplanev1.ChalkosControlPlane{}).
		Owns(&clusterv1.Machine{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: max(1, r.MaxConcurrentReconciles)}).
		WithEventFilter(predicates.ResourceHasFilterLabel(scheme, log, r.WatchFilterValue)).
		Watches(&bootstrapv1.ChalkosConfig{}, handler.EnqueueRequestsFromMapFunc(r.configToControlPlane)).
		Watches(&clusterv1.Cluster{}, handler.EnqueueRequestsFromMapFunc(clusterToControlPlane),
			builder.WithPredicates(predicates.Any(scheme, log,
				predicates.ClusterPausedTransitionsOrInfrastructureProvisioned(scheme, log),
				clusterEndpointOrControlPlaneChanged(),
				machineutil.ClusterDeletionStarted(),
			))).
		Complete(r)
}

// Reconcile moves the ChalkosControlPlane one step towards its Machines bootstrapped and joined,
// or, once it is deleted, towards its Machines deleted.
func (r *ChalkosControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, rerr error) {
	cp := &controlplanev1.ChalkosControlPlane{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, cp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if added, err := finalizers.EnsureFinalizer(ctx, r.Client, cp, controlplanev1.ChalkosControlPlaneFinalizer); err != nil || added {
		return ctrl.Result{}, err
	}
	cluster, err := util.GetOwnerCluster(ctx, r.Client, cp.ObjectMeta)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if cluster == nil {
		// A ChalkosControlPlane without its Cluster yet is reconciled once the Cluster owns it. One
		// deleted before then has no Machines: the controller creates them only for a Cluster.
		if cp.DeletionTimestamp.IsZero() {
			return ctrl.Result{}, nil
		}
		original := cp.DeepCopy()
		controllerutil.RemoveFinalizer(cp, controlplanev1.ChalkosControlPlaneFinalizer)
		return ctrl.Result{}, client.IgnoreNotFound(r.Client.Patch(ctx, cp, client.MergeFrom(original)))
	}
	if isPaused, requeue, err := paused.EnsurePausedCondition(ctx, r.Client, cluster, cp); err != nil || isPaused || requeue {
		return ctrl.Result{}, err
	}
	s := &scope{cp: cp, cluster: cluster, client: r.Client}
	if err := s.track(); err != nil {
		return ctrl.Result{}, err
	}
	if s.hash, err = specHash(cp); err != nil {
		return ctrl.Result{}, err
	}
	if after := cp.Spec.Rollout.After.Time; !after.IsZero() && !r.clock().Now().Before(after) {
		s.rolloutAfter = after
	}
	before := slices.Clone(cp.Status.Conditions)
	defer func() {
		setStatus(s)
		conditionlog.Changes(ctrl.LoggerFrom(ctx), before, cp.Status.Conditions,
			controlplanev1.InitializedCondition, controlplanev1.AvailableCondition,
			controlplanev1.ScalingUpCondition, controlplanev1.MachinesReadyCondition,
			controlplanev1.ScalingDownCondition, controlplanev1.RollingOutCondition, controlplanev1.DeletingCondition)
		if err := s.patch(ctx); err != nil {
			rerr = kerrors.NewAggregate([]error{rerr, err})
		}
	}()
	if !cp.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, s)
	}
	return r.reconcile(ctx, s)
}

// scope is what one reconcile works on.
type scope struct {
	cp      *controlplanev1.ChalkosControlPlane
	cluster *clusterv1.Cluster
	client  client.Client
	helper  *patch.Helper
	// machines are the control plane's Machines that are not being deleted, oldest first.
	machines []clusterv1.Machine
	// deleting are the control plane's Machines that are being deleted.
	deleting []clusterv1.Machine
	// listed is set once the Machines were listed.
	listed bool
	// hash is the ChalkosControlPlane's specHash.
	hash string
	// rolloutAfter is spec.rollout.after once it has passed; zero before and without one.
	rolloutAfter time.Time
	// statuses are the Kubernetes statuses of the Machines' nodes read in this reconcile, by
	// Machine name, so that each node is asked once.
	statuses map[string]nodeStatus
	// scaleUpWaiting is what the next Machine's creation waits for, such as a Machine's join;
	// empty when it waits for nothing.
	scaleUpWaiting string
	// scaleDownWaiting is why the next scale-down step may not start yet, for the ScalingDown
	// condition; empty when it may.
	scaleDownWaiting string
	// removing and removingReason are what the pre-terminate hook of the Machine it removes waits
	// on, for the ScalingDown and Deleting conditions; empty while no hook waits.
	removing, removingReason string
	// deletingReason and deletingMessage are the Deleting condition's reason and message while
	// the ChalkosControlPlane is being deleted; empty before its Machines were listed.
	deletingReason, deletingMessage string
	// held is why no Machine may be created, as the guard found; empty when nothing holds.
	// heldReason is the reason of a refused change, VersionSkewReason or FixedFieldChangedReason;
	// empty for a check that could not be made, which the conditions report as a wait.
	held, heldReason string
	// rollWaiting is what the roll's next step waits for, for the RollingOut condition; empty
	// when nothing.
	rollWaiting string
	// imageWaiting is why status.observedFiles is not recorded yet, as while the image cannot be
	// read; empty when nothing.
	imageWaiting string
}

// nodeStatus is a node's answer to KubernetesStatus.
type nodeStatus struct {
	status *kubernetesv1.StatusResponse
	err    error
}

// track starts tracking the ChalkosControlPlane's changes from its current state.
func (s *scope) track() error {
	h, err := patch.NewHelper(s.cp, s.client)
	s.helper = h
	return err
}

// patch writes the ChalkosControlPlane's changes.
func (s *scope) patch(ctx context.Context) error {
	return s.helper.Patch(ctx, s.cp,
		patch.WithOwnedConditions{Conditions: []string{
			controlplanev1.AvailableCondition, controlplanev1.InitializedCondition,
			controlplanev1.MachinesReadyCondition, controlplanev1.ScalingUpCondition,
			controlplanev1.ScalingDownCondition, controlplanev1.RollingOutCondition, controlplanev1.DeletingCondition,
		}},
		patch.WithStatusObservedGeneration{})
}

// checkpoint writes the ChalkosControlPlane's changes so far, before a call that must not be made
// on another Machine after a crash.
func (s *scope) checkpoint(ctx context.Context) error {
	if err := s.patch(ctx); err != nil {
		return err
	}
	return s.track()
}

// initialized tells whether the control plane was initialized.
func (s *scope) initialized() bool {
	return ptr.Deref(s.cp.Status.Initialization.ControlPlaneInitialized, false)
}

// notInitialized sets the Initialized condition to false with the reason and message.
func (s *scope) notInitialized(reason, message string) {
	conditions.Set(s.cp, metav1.Condition{
		Type: controlplanev1.InitializedCondition, Status: metav1.ConditionFalse,
		Reason: reason, Message: machineutil.Truncate(message),
	})
}

// machine returns the control plane's Machine of the name that is not being deleted; nil when it
// has none.
func (s *scope) machine(name string) *clusterv1.Machine {
	for i := range s.machines {
		if s.machines[i].Name == name {
			return &s.machines[i]
		}
	}
	return nil
}

// isDeleting tells whether the control plane's Machine of the name is being deleted.
func (s *scope) isDeleting(name string) bool {
	for i := range s.deleting {
		if s.deleting[i].Name == name {
			return true
		}
	}
	return false
}

// reconcile runs the steps of the control plane in order, returning at the first that waits.
func (r *ChalkosControlPlaneReconciler) reconcile(ctx context.Context, s *scope) (ctrl.Result, error) {
	if err := r.listMachines(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.syncMachines(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	if !s.cluster.DeletionTimestamp.IsZero() {
		// The Cluster's deletion deletes the ChalkosControlPlane once the workers are gone: no
		// Machine is created or bootstrapped meanwhile, and the kubeconfig is only renewed.
		if err := r.removeAllHooks(ctx, s); err != nil {
			return ctrl.Result{}, err
		}
		sec, ok, err := secrets.Get(ctx, r.APIReader, client.ObjectKeyFromObject(s.cluster))
		if err != nil {
			return ctrl.Result{}, err
		}
		renew, err := r.renewKubeconfig(ctx, s, sec, ok)
		return ctrl.Result{RequeueAfter: renew}, err
	}
	if err := r.addHooks(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	if !s.cluster.Spec.ControlPlaneEndpoint.IsValid() {
		// The Cluster's watch reconciles again once the endpoint is set.
		s.notInitialized(controlplanev1.NotInitializedReason, "The Cluster has no controlPlaneEndpoint yet")
		return ctrl.Result{}, nil
	}
	owner := metav1.OwnerReference{APIVersion: controlPlaneKind.GroupVersion().String(), Kind: controlPlaneKind.Kind, Name: s.cp.Name, UID: s.cp.UID}
	sec, err := secrets.Ensure(ctx, uncached{Client: r.Client, reader: r.APIReader}, client.ObjectKeyFromObject(s.cluster), owner, r.clock().Now())
	if err != nil {
		return ctrl.Result{}, err
	}
	hooks, err := r.reconcileHooks(ctx, s, sec)
	if err != nil {
		return ctrl.Result{}, err
	}
	guarded := r.guard(ctx, s)
	res, err := r.initialize(ctx, s, sec)
	if err != nil {
		return ctrl.Result{}, err
	}
	scale, err := r.scaleUp(ctx, s, sec)
	if err != nil {
		return ctrl.Result{}, err
	}
	res = earliest(earliest(res, scale), hooks)
	down, err := r.scaleDown(ctx, s, sec)
	if err != nil {
		return ctrl.Result{}, err
	}
	res = earliest(earliest(res, down), earliest(guarded, earliest(r.untilRolloutAfter(s), r.observeFiles(ctx, s))))
	if !s.initialized() {
		return res, nil
	}
	renew, err := r.reconcileKubeconfig(ctx, s, sec)
	if err != nil {
		return ctrl.Result{}, err
	}
	res = earliest(res, ctrl.Result{RequeueAfter: renew})
	return earliest(res, r.reconcilePrune(ctx, s, sec)), nil
}

// reconcileDelete waits for the cluster's workers to be deleted, then deletes the control plane's
// Machines and, once they are gone, removes the finalizer. The Machines' own finalizers delete
// their infrastructure and nodes first, after the pre-terminate hooks, which take each Machine
// but the last out of etcd and revoke it, one at a time; once the Cluster is being deleted no
// hook runs. The workers go first, as with Cluster API's own control plane, so that their nodes
// are drained while the cluster's API server still answers. The kubeconfig is renewed while the
// deletion waits, since draining the workers needs it.
func (r *ChalkosControlPlaneReconciler) reconcileDelete(ctx context.Context, s *scope) (ctrl.Result, error) {
	if err := r.listMachines(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	sec, ok, err := secrets.Get(ctx, r.APIReader, client.ObjectKeyFromObject(s.cluster))
	if err != nil {
		return ctrl.Result{}, err
	}
	var res ctrl.Result
	switch {
	case !s.cluster.DeletionTimestamp.IsZero():
		if err := r.removeAllHooks(ctx, s); err != nil {
			return ctrl.Result{}, err
		}
	case ok:
		if res, err = r.reconcileHooks(ctx, s, sec); err != nil {
			return ctrl.Result{}, err
		}
	case slices.ContainsFunc(s.deleting, func(m clusterv1.Machine) bool { return machineutil.HasHook(&m) }):
		s.removing = fmt.Sprintf("the Cluster's secrets %s are gone, so no Machine leaves etcd; remove the annotation %s from the Machines to delete them",
			secrets.Name(s.cluster.Name), machineutil.HookAnnotation)
	}
	renew, err := r.renewKubeconfig(ctx, s, sec, ok)
	if err != nil {
		return ctrl.Result{}, err
	}
	res = earliest(res, ctrl.Result{RequeueAfter: renew})
	workers, pools, err := r.countWorkers(ctx, s)
	if err != nil {
		return ctrl.Result{}, err
	}
	if workers+pools > 0 {
		s.deletingReason = controlplanev1.DeletingWaitingForWorkersDeletionReason
		s.deletingMessage = waitingForWorkersMessage(workers, pools)
		// No watch reports the workers' deletion; the requeue does.
		return earliest(ctrl.Result{RequeueAfter: machineutil.RetryAfter}, res), nil
	}
	for i := range s.machines {
		err := r.Client.Delete(ctx, &s.machines[i])
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("delete the Machine %s: %w", s.machines[i].Name, err)
		}
		ctrl.LoggerFrom(ctx).Info("Deleting the Machine", "machine", s.machines[i].Name)
	}
	if n := len(s.machines) + len(s.deleting); n > 0 {
		s.deletingReason = controlplanev1.DeletingWaitingForMachineDeletionReason
		s.deletingMessage = "Deleting " + plural(n, "Machine")
		if s.removing != "" {
			s.deletingMessage += "; " + s.removing
		}
		// The Machines' watch reconciles again as they go; the requeue covers a missed event.
		return earliest(ctrl.Result{RequeueAfter: machineutil.RetryAfter}, res), nil
	}
	ctrl.LoggerFrom(ctx).Info("The Machines are gone; removing the finalizer")
	s.deletingReason, s.deletingMessage = controlplanev1.DeletionCompletedReason, ""
	controllerutil.RemoveFinalizer(s.cp, controlplanev1.ChalkosControlPlaneFinalizer)
	return ctrl.Result{}, nil
}

// renewKubeconfig keeps the kubeconfig Secret of an initialized control plane while it or its
// Cluster is being deleted, from the cluster's secrets sec if they still exist (ok), and returns
// when to renew it next; zero when it is not kept.
func (r *ChalkosControlPlaneReconciler) renewKubeconfig(ctx context.Context, s *scope, sec clustersecrets.Secrets, ok bool) (time.Duration, error) {
	if !ok || !s.initialized() {
		return 0, nil
	}
	return r.reconcileKubeconfig(ctx, s, sec)
}

// initialize bootstraps the cluster on the first Machine once it is installed, and marks the
// control plane initialized once that node applied the cluster's add-ons.
//
// The Machine is recorded before the call, so that no later reconcile bootstraps another one.
// When the record is lost, as when the status write after the record fails, a later reconcile
// may call Bootstrap on the same first Machine again, which chalkd refuses on a node that is
// bootstrapped or holds etcd data, and on any node while the cluster's API server answers. The
// same guards make it safe to forget a recorded Machine that is gone before the control plane was
// initialized: its etcd went with it, and a new first Machine cannot start a second cluster
// beside one that answers.
func (r *ChalkosControlPlaneReconciler) initialize(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	if s.initialized() {
		return ctrl.Result{}, nil
	}
	if name := s.cp.Status.BootstrappedMachine; name != "" && s.machine(name) == nil {
		if s.isDeleting(name) {
			// The Machines' watch reconciles again once it is gone.
			s.notInitialized(controlplanev1.NotInitializedReason, fmt.Sprintf("The bootstrapped Machine %s is being deleted", name))
			return ctrl.Result{}, nil
		}
		s.cp.Status.BootstrappedMachine = ""
		s.notInitialized(controlplanev1.BootstrappedMachineGoneReason, fmt.Sprintf("The bootstrapped Machine %s is gone; a new first Machine is bootstrapped", name))
	}
	if len(s.machines) == 0 {
		return ctrl.Result{}, nil
	}

	var m *clusterv1.Machine
	if s.cp.Status.BootstrappedMachine == "" {
		m = &s.machines[0]
		if ok, err := r.installed(ctx, m); err != nil || !ok {
			// The ChalkosConfig's and the Machine's watches reconcile again once it is installed.
			s.notInitialized(controlplanev1.NotInitializedReason, fmt.Sprintf("Waiting for the Machine %s to be installed", m.Name))
			return ctrl.Result{}, err
		}
		s.cp.Status.BootstrappedMachine = m.Name
		s.notInitialized(controlplanev1.NotInitializedReason, fmt.Sprintf("Bootstrapping the cluster on the Machine %s", m.Name))
		if err := s.checkpoint(ctx); err != nil {
			return ctrl.Result{}, err
		}
		// Logged once: a retry of the call, while the node waits, is not logged again.
		ctrl.LoggerFrom(ctx).Info("Bootstrapping the cluster", "machine", m.Name)
		if res, done, err := r.bootstrap(ctx, s, m, sec); !done {
			return res, err
		}
	} else {
		m = s.machine(s.cp.Status.BootstrappedMachine)
		st, err := r.nodeStatus(ctx, s, m, sec)
		if err == nil && st.GetPhase() == kubernetesv1.Phase_PHASE_WAITING {
			// An earlier call failed before the node committed to the bootstrap, or was never made.
			// The node's status tells the outcome at the next reconcile.
			res, done, err := r.bootstrap(ctx, s, m, sec)
			if done {
				res = ctrl.Result{RequeueAfter: machineutil.RetryAfter}
			}
			return res, err
		}
	}
	st, err := r.nodeStatus(ctx, s, m, sec)
	if err != nil {
		s.notInitialized(controlplanev1.NotInitializedReason, fmt.Sprintf("Reading the status of the Machine %s: %v", m.Name, err))
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	if !st.GetAddonsApplied() {
		s.notInitialized(controlplanev1.NotInitializedReason, fmt.Sprintf("Waiting for the Machine %s to apply the cluster's add-ons: %s", m.Name, st.GetState()))
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	s.cp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	return ctrl.Result{}, nil
}

// bootstrap calls Bootstrap on the Machine. done is set when the node bootstraps or is
// bootstrapped: the call succeeded, timed out while the node goes on with it, or was refused
// because the node is bootstrapped already. Another refusal is reported in the Initialized
// condition and tried again later.
func (r *ChalkosControlPlaneReconciler) bootstrap(ctx context.Context, s *scope, m *clusterv1.Machine, sec clustersecrets.Secrets) (res ctrl.Result, done bool, err error) {
	timeout := r.BootstrapTimeout
	if timeout == 0 {
		timeout = DefaultBootstrapTimeout
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	log := ctrl.LoggerFrom(ctx).WithValues("machine", m.Name)
	err = r.Node.Bootstrap(callCtx, machineutil.InternalIP(m), sec, m.Name)
	delete(s.statuses, m.Name)
	switch {
	case err == nil:
		log.Info("The machine bootstrapped the cluster")
		return ctrl.Result{}, true, nil
	case sdkkubernetes.BootstrappedAlready(err):
		log.Info("The machine is bootstrapped already")
		return ctrl.Result{}, true, nil
	case ctx.Err() == nil && (connect.CodeOf(err) == connect.CodeDeadlineExceeded || errors.Is(err, context.DeadlineExceeded)):
		// A node that did not commit to the bootstrap still waits for it, and is called again.
		log.Info("The bootstrap call timed out; following the node's status")
		return ctrl.Result{}, true, nil
	case connect.CodeOf(err) == connect.CodeFailedPrecondition:
		s.notInitialized(controlplanev1.BootstrapRefusedReason, fmt.Sprintf("The Machine %s refused the bootstrap: %v", m.Name, err))
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, false, nil
	}
	s.notInitialized(controlplanev1.NotInitializedReason, fmt.Sprintf("Bootstrapping the Machine %s: %v", m.Name, err))
	return ctrl.Result{}, false, err
}

// nodeStatus is the Kubernetes status of the Machine's node, read once per reconcile.
func (r *ChalkosControlPlaneReconciler) nodeStatus(ctx context.Context, s *scope, m *clusterv1.Machine, sec clustersecrets.Secrets) (*kubernetesv1.StatusResponse, error) {
	if ns, ok := s.statuses[m.Name]; ok {
		return ns.status, ns.err
	}
	st, err := r.Node.KubernetesStatus(ctx, machineutil.InternalIP(m), sec, m.Name)
	if s.statuses == nil {
		s.statuses = map[string]nodeStatus{}
	}
	s.statuses[m.Name] = nodeStatus{status: st, err: err}
	return st, err
}

// installed tells whether the Machine's ChalkosConfig reports the machine installed and the
// Machine has an InternalIP to reach it at.
func (r *ChalkosControlPlaneReconciler) installed(ctx context.Context, m *clusterv1.Machine) (bool, error) {
	if machineutil.InternalIP(m) == "" {
		return false, nil
	}
	var cfg bootstrapv1.ChalkosConfig
	err := r.Client.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, &cfg)
	if err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return conditions.IsTrue(&cfg, bootstrapv1.ReadyCondition), nil
}

// joined tells whether the Machine's node is installed and an etcd member of the cluster: it
// bootstrapped the cluster or finished joining it. An unreachable node has not joined as far as
// the controller knows.
func (r *ChalkosControlPlaneReconciler) joined(ctx context.Context, s *scope, m *clusterv1.Machine, sec clustersecrets.Secrets) (bool, error) {
	if ok, err := r.installed(ctx, m); err != nil || !ok {
		return false, err
	}
	st, err := r.nodeStatus(ctx, s, m, sec)
	return err == nil && st.GetPhase() == kubernetesv1.Phase_PHASE_BOOTSTRAPPED, nil
}

// clock is the reconciler's clock.
func (r *ChalkosControlPlaneReconciler) clock() clock.PassiveClock {
	if r.Clock == nil {
		return clock.RealClock{}
	}
	return r.Clock
}

// earliest is the result of a and b that reconciles again first: the shorter of their non-zero
// RequeueAfter.
func earliest(a, b ctrl.Result) ctrl.Result {
	switch {
	case a.RequeueAfter == 0:
		return b
	case b.RequeueAfter == 0 || a.RequeueAfter < b.RequeueAfter:
		return a
	}
	return b
}

// uncached is a client that reads with reader and writes with its Client: secrets.Ensure must not
// read the Secret from a cache that does not hold it yet, or it would generate the secrets again,
// and an infrastructure template is read without an informer for its kind.
type uncached struct {
	client.Client
	reader client.Reader
}

// Get reads the object with the reader.
func (u uncached) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return u.reader.Get(ctx, key, obj, opts...)
}

// isControlPlaneRef tells whether ref names a ChalkosControlPlane.
func isControlPlaneRef(ref metav1.OwnerReference) bool {
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	return err == nil && gv.Group == controlPlaneKind.Group && ref.Kind == controlPlaneKind.Kind
}

// configToControlPlane maps a ChalkosConfig of a control-plane Machine to its ChalkosControlPlane:
// the ChalkosControlPlane owns it until its Machine takes it over, and then owns that Machine.
func (r *ChalkosControlPlaneReconciler) configToControlPlane(ctx context.Context, o client.Object) []ctrl.Request {
	for _, ref := range o.GetOwnerReferences() {
		if isControlPlaneRef(ref) {
			return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: o.GetNamespace(), Name: ref.Name}}}
		}
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil || gv.Group != clusterv1.GroupVersion.Group || ref.Kind != "Machine" {
			continue
		}
		var m clusterv1.Machine
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: o.GetNamespace(), Name: ref.Name}, &m); err != nil {
			return nil
		}
		if c := metav1.GetControllerOf(&m); c != nil && isControlPlaneRef(*c) {
			return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: o.GetNamespace(), Name: c.Name}}}
		}
	}
	return nil
}

// clusterToControlPlane maps a Cluster to its ChalkosControlPlane.
func clusterToControlPlane(_ context.Context, o client.Object) []ctrl.Request {
	c, ok := o.(*clusterv1.Cluster)
	if !ok {
		return nil
	}
	ref := c.Spec.ControlPlaneRef
	if ref.APIGroup != controlPlaneKind.Group || ref.Kind != controlPlaneKind.Kind {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: c.Namespace, Name: ref.Name}}}
}

// clusterEndpointOrControlPlaneChanged passes the updates of a Cluster whose controlPlaneEndpoint
// or controlPlaneRef changed.
func clusterEndpointOrControlPlaneChanged() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			old, ok := e.ObjectOld.(*clusterv1.Cluster)
			cur, ok2 := e.ObjectNew.(*clusterv1.Cluster)
			return ok && ok2 && (old.Spec.ControlPlaneEndpoint != cur.Spec.ControlPlaneEndpoint || old.Spec.ControlPlaneRef != cur.Spec.ControlPlaneRef)
		},
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
