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

// Package controllers holds the bootstrap provider's reconciler, which installs each Machine over
// chalkd's API.
package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
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
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/paused"
	"sigs.k8s.io/cluster-api/util/predicates"

	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/ociimage"
	"github.com/trevex/chalkos/pkg/sdk"
	sdkkubernetes "github.com/trevex/chalkos/pkg/sdk/layers/kubernetes"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/conditionlog"
	"github.com/trevex/cluster-api-provider-chalkos/internal/images"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

// dataSecretValue is the bootstrap data of every Machine: a marker, since the machine is
// installed over chalkd's API. The infrastructure provider passes it to the machine untouched.
const dataSecretValue = "# chalkos: installed over chalkd's API\n"

const (
	// maxFingerprints is how many maintenance fingerprints a ChalkosConfig's status keeps.
	maxFingerprints = 10
	// slotWait is how soon a ChalkosConfig that found every install slot taken tries again.
	slotWait = 30 * time.Second
	// verifyEvery is about how often an installed machine is asked whether it still runs as its
	// node with its image.
	verifyEvery = 10 * time.Minute
	// rebootGrace is how long after an accepted install the machine is given to reboot into it
	// before it may be installed again.
	rebootGrace = 2 * time.Minute
	// controlPlaneWait is how soon a worker that waits for the Cluster's control plane is
	// reconciled again in case the Cluster's watch missed the update that initialized it.
	controlPlaneWait = time.Minute
)

const (
	// DefaultInstallTimeout is how long an install may take when the reconciler names no timeout.
	DefaultInstallTimeout = 30 * time.Minute
	// DefaultMaxInstalls is how many installs run at once when the reconciler names no number.
	DefaultMaxInstalls = 10
)

// ChalkosConfigReconciler installs the Machine of each ChalkosConfig over chalkd's API, once its
// infrastructure, the cluster's secrets and, for a worker, the control plane are ready.
type ChalkosConfigReconciler struct {
	// Client reads and writes the management cluster's objects, from the manager's cache.
	Client client.Client
	// APIReader reads uncached: the ChalkosConfig, whose status the last reconcile may have
	// written after the cache's copy, and the cluster's secrets, the data and image pull Secrets,
	// which the controller does not watch.
	APIReader client.Reader
	// Node reaches the machines and reads their images.
	Node node.Client
	// InstallTimeout bounds one install; zero is DefaultInstallTimeout.
	InstallTimeout time.Duration
	// MaxInstalls is how many installs run at once; zero is DefaultMaxInstalls. An install holds
	// its reconcile for its whole length, so the controller runs twice as many reconciles, and
	// those that find every install slot taken try again later.
	MaxInstalls int
	// WatchFilterValue, when set, limits the controller to objects whose
	// cluster.x-k8s.io/watch-filter label has this value.
	WatchFilterValue string
	// Recorder records the events of the workers' pre-terminate hooks; nil records none.
	Recorder events.EventRecorder
	// Clock tells when a worker's hook last pruned a full list of revoked nodes; nil is the real
	// clock.
	Clock clock.PassiveClock

	slotsOnce sync.Once
	slots     chan struct{}
	artifacts images.Cache

	// prunes holds, per Cluster, when a worker's hook last had a control plane prune the full
	// list of revoked nodes. It lives only in memory: a restarted controller prunes once more.
	prunesMu sync.Mutex
	prunes   map[client.ObjectKey]time.Time
}

// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=chalkosconfigs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=chalkosconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=chalkosconfigs/finalizers,verbs=update
// +kubebuilder:rbac:groups=bootstrap.cluster.x-k8s.io,resources=chalkosconfigtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=controlplane.cluster.x-k8s.io,resources=chalkoscontrolplanes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;create
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// SetupWithManager registers the reconciler with mgr.
func (r *ChalkosConfigReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	log := ctrl.LoggerFrom(ctx).WithValues("controller", "chalkosconfig")
	return ctrl.NewControllerManagedBy(mgr).
		For(&bootstrapv1.ChalkosConfig{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2 * r.maxInstalls()}).
		WithEventFilter(predicates.ResourceHasFilterLabel(mgr.GetScheme(), log, r.WatchFilterValue)).
		Watches(&clusterv1.Machine{}, handler.EnqueueRequestsFromMapFunc(machineToConfig),
			builder.WithPredicates(machineChanged())).
		Watches(&clusterv1.Cluster{}, handler.EnqueueRequestsFromMapFunc(r.clusterToConfigs),
			builder.WithPredicates(predicates.Any(mgr.GetScheme(), log,
				predicates.ClusterPausedTransitions(mgr.GetScheme(), log),
				controlPlaneInitialized(),
				machineutil.ClusterDeletionStarted(),
				clusterInputsChanged(),
			))).
		Watches(&controlplanev1.ChalkosControlPlane{}, handler.EnqueueRequestsFromMapFunc(r.controlPlaneToConfigs),
			builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

// Reconcile moves the ChalkosConfig's Machine one step towards installed.
func (r *ChalkosConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (_ ctrl.Result, rerr error) {
	// A cached copy may lack the last install's time and so install the machine again.
	cfg := &bootstrapv1.ChalkosConfig{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, cfg); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	machine, err := util.GetOwnerMachine(ctx, r.Client, cfg.ObjectMeta)
	if err != nil || machine == nil {
		// A ChalkosConfig without its Machine yet is reconciled once the Machine owns it.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	cluster, err := util.GetClusterByName(ctx, r.Client, machine.Namespace, machine.Spec.ClusterName)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if isPaused, requeue, err := paused.EnsurePausedCondition(ctx, r.Client, cluster, cfg); err != nil || isPaused || requeue {
		return ctrl.Result{}, err
	}
	s := &scope{cfg: cfg, machine: machine, cluster: cluster, client: r.Client}
	if err := s.track(); err != nil {
		return ctrl.Result{}, err
	}
	before := slices.Clone(cfg.Status.Conditions)
	defer func() {
		conditionlog.Changes(ctrl.LoggerFrom(ctx), before, cfg.Status.Conditions, bootstrapv1.ReadyCondition)
		if err := s.patch(ctx); err != nil {
			rerr = kerrors.NewAggregate([]error{rerr, err})
		}
	}()
	return r.reconcile(ctx, s)
}

// scope is what one reconcile works on.
type scope struct {
	cfg     *bootstrapv1.ChalkosConfig
	machine *clusterv1.Machine
	cluster *clusterv1.Cluster
	client  client.Client
	helper  *patch.Helper
}

// track starts tracking the ChalkosConfig's changes from its current state.
func (s *scope) track() error {
	h, err := patch.NewHelper(s.cfg, s.client)
	s.helper = h
	return err
}

// patch writes the ChalkosConfig's changes.
func (s *scope) patch(ctx context.Context) error {
	return s.helper.Patch(ctx, s.cfg,
		patch.WithOwnedConditions{Conditions: []string{bootstrapv1.ReadyCondition, bootstrapv1.InstalledCondition, bootstrapv1.NodeRemovedCondition}},
		patch.WithStatusObservedGeneration{})
}

// checkpoint writes the ChalkosConfig's changes so far, before a call that may take long.
func (s *scope) checkpoint(ctx context.Context) error {
	if err := s.patch(ctx); err != nil {
		return err
	}
	return s.track()
}

// set sets the Ready and Installed conditions, which move together.
func (s *scope) set(status metav1.ConditionStatus, reason, message string) {
	s.setReady(status, reason, message)
	conditions.Set(s.cfg, metav1.Condition{Type: bootstrapv1.InstalledCondition, Status: status, Reason: reason, Message: machineutil.Truncate(message)})
}

// setReady sets the Ready condition alone, as for an installed machine that is unreachable for
// now but still installed.
func (s *scope) setReady(status metav1.ConditionStatus, reason, message string) {
	conditions.Set(s.cfg, metav1.Condition{Type: bootstrapv1.ReadyCondition, Status: status, Reason: reason, Message: machineutil.Truncate(message)})
}

// nodeRemoved sets the NodeRemoved condition.
func (s *scope) nodeRemoved(status metav1.ConditionStatus, reason, message string) {
	conditions.Set(s.cfg, metav1.Condition{Type: bootstrapv1.NodeRemovedCondition, Status: status, Reason: reason, Message: machineutil.Truncate(message)})
}

// refuse reports that the config or the image was refused, as reason says, for the inputs, which
// a retry does not change.
func (s *scope) refuse(reason, message, inputs string) {
	s.cfg.Status.RefusedInputs = inputs
	s.set(metav1.ConditionFalse, reason, message)
}

// bootImageRefused tells whether the image the machine booted refused the controller's
// certificate: an ImageRefused without inputs, which no change of the ChalkosConfig repairs.
func (s *scope) bootImageRefused() bool {
	c := conditions.Get(s.cfg, bootstrapv1.ReadyCondition)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == bootstrapv1.ImageRefusedReason && s.cfg.Status.RefusedInputs == ""
}

// reconcile runs the steps of an install in order, returning at the first that waits.
func (r *ChalkosConfigReconciler) reconcile(ctx context.Context, s *scope) (ctrl.Result, error) {
	if err := r.ensureDataSecret(ctx, s); err != nil {
		return ctrl.Result{}, err
	}
	if !util.IsControlPlaneMachine(s.machine) {
		if res, done, err := r.reconcileHook(ctx, s); done {
			return res, err
		}
	}
	installed := ptr.Deref(s.cfg.Status.Installed, false)
	if !installed && !s.machine.DeletionTimestamp.IsZero() {
		s.set(metav1.ConditionFalse, bootstrapv1.MachineDeletingReason, "The Machine is being deleted")
		return ctrl.Result{}, nil
	}
	if !installed && s.bootImageRefused() {
		return ctrl.Result{}, nil
	}
	addr := machineutil.InternalIP(s.machine)
	if !installed && (s.machine.Spec.ProviderID == "" || addr == "") {
		s.set(metav1.ConditionFalse, bootstrapv1.WaitingForInfrastructureReason, "The Machine has no providerID or InternalIP yet")
		return ctrl.Result{}, nil
	}
	if !installed && !util.IsControlPlaneMachine(s.machine) && !ptr.Deref(s.cluster.Status.Initialization.ControlPlaneInitialized, false) {
		s.set(metav1.ConditionFalse, bootstrapv1.WaitingForControlPlaneReason, "The Cluster's control plane is not initialized yet")
		return ctrl.Result{RequeueAfter: controlPlaneWait}, nil
	}
	sec, ok, err := secrets.Get(ctx, r.APIReader, client.ObjectKeyFromObject(s.cluster))
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case !ok && installed:
		return ctrl.Result{}, fmt.Errorf("the Cluster's secrets %s are gone", secrets.Name(s.cluster.Name))
	case !ok:
		s.set(metav1.ConditionFalse, bootstrapv1.WaitingForClusterSecretsReason, fmt.Sprintf("The Secret %s does not exist yet", secrets.Name(s.cluster.Name)))
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	if installed {
		return r.verify(ctx, s, addr, sec, true)
	}
	if t := s.cfg.Status.LastInstallTime; t != nil && time.Since(t.Time) < rebootGrace {
		return r.verify(ctx, s, addr, sec, false)
	}
	return r.install(ctx, s, addr, sec)
}

// ensureDataSecret creates the Machine's data Secret, a marker the infrastructure provider waits
// for, also again when it was deleted, and reports it in the status.
func (r *ChalkosConfigReconciler) ensureDataSecret(ctx context.Context, s *scope) error {
	key := client.ObjectKey{Namespace: s.cfg.Namespace, Name: s.cfg.Name}
	err := r.APIReader.Get(ctx, key, &corev1.Secret{})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err != nil {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace,
				Name:      key.Name,
				Labels:    map[string]string{clusterv1.ClusterNameLabel: s.cluster.Name},
			},
			Type: clusterv1.ClusterSecretType,
			Data: map[string][]byte{"value": []byte(dataSecretValue)},
		}
		if err := controllerutil.SetControllerReference(s.cfg, secret, r.Client.Scheme()); err != nil {
			return err
		}
		switch err := r.Client.Create(ctx, secret); {
		case err == nil:
			ctrl.LoggerFrom(ctx).Info("Created the bootstrap data Secret", "secret", key.Name)
		case !apierrors.IsAlreadyExists(err):
			return err
		}
	}
	s.cfg.Status.DataSecretName = key.Name
	s.cfg.Status.Initialization.DataSecretCreated = ptr.To(true)
	return nil
}

// errNoChalkosControlPlane refuses a Cluster whose control plane is not a ChalkosControlPlane,
// which holds the cluster's documents.
var errNoChalkosControlPlane = errors.New("a ChalkosConfig needs a ChalkosControlPlane: the Cluster's controlPlaneRef names none")

// install reads and checks the image, builds the documents and installs the machine at addr.
func (r *ChalkosConfigReconciler) install(ctx context.Context, s *scope, addr string, sec clustersecrets.Secrets) (ctrl.Result, error) {
	if !s.cluster.Spec.ControlPlaneEndpoint.IsValid() {
		s.set(metav1.ConditionFalse, bootstrapv1.WaitingForInfrastructureReason, "The Cluster has no controlPlaneEndpoint yet")
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	clusterFiles, cpErr := r.clusterFiles(ctx, s.cluster)
	if cpErr != nil && !errors.Is(cpErr, errNoChalkosControlPlane) {
		return ctrl.Result{}, cpErr
	}
	m := machineOf(s.machine, s.cluster)
	in, err := inputsDigest(m, s.cluster.Spec.ControlPlaneRef, clusterFiles, s.cfg.Spec.Files, string(s.cfg.Spec.Image), sec.OSCA.Certificate)
	if err != nil {
		return ctrl.Result{}, err
	}
	switch {
	case in == s.cfg.Status.RefusedInputs:
		// Refused before, and nothing it was refused for changed.
		return ctrl.Result{}, nil
	case cpErr != nil:
		s.refuse(bootstrapv1.ConfigRefusedReason, cpErr.Error(), in)
		return ctrl.Result{}, nil
	}
	kc, err := images.Keychain(ctx, r.APIReader, s.cfg.Namespace, s.cfg.Spec.ImagePullSecretRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	a, err := r.artifacts.Image(ctx, r.Node, string(s.cfg.Spec.Image), s.cfg.Namespace, s.cfg.Spec.ImagePullSecretRef, kc)
	if err != nil {
		s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason, fmt.Sprintf("Reading the image: %v", err))
		return ctrl.Result{}, err
	}
	if err := checkImage(a, sec, s.machine.Spec.Version); err != nil {
		s.refuse(bootstrapv1.ImageRefusedReason, err.Error(), in)
		return ctrl.Result{}, nil
	}
	docs, err := documents.For(m, clusterFiles, s.cfg.Spec.Files)
	if err != nil {
		s.refuse(bootstrapv1.ConfigRefusedReason, err.Error(), in)
		return ctrl.Result{}, nil
	}

	release, ok := r.takeSlot()
	if !ok {
		s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason, fmt.Sprintf("Waiting for one of the %d install slots", r.maxInstalls()))
		return ctrl.Result{RequeueAfter: slotWait}, nil
	}
	defer release()
	s.cfg.Status.ImageRootHash = a.Header.RootHash
	conn, err := r.Node.Maintenance(ctx, addr, sec)
	switch {
	case errors.Is(err, sdk.ErrInstalled):
		// An install finished before its status was written.
		return r.verify(ctx, s, addr, sec, false)
	case errors.Is(err, sdk.ErrOSCARefused):
		s.refuse(bootstrapv1.ImageRefusedReason, fmt.Sprintf("The booted image trusts another OS CA: %v", err), "")
		return ctrl.Result{}, nil
	case err != nil:
		s.set(metav1.ConditionFalse, bootstrapv1.NodeUnreachableReason, err.Error())
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	defer conn.Close()
	log := ctrl.LoggerFrom(ctx).WithValues("address", addr)
	if recordFingerprint(s.cfg, conn.Fingerprint(), time.Now()) {
		log.Info("Reached the machine in maintenance mode", "fingerprint", conn.Fingerprint())
	}
	s.cfg.Status.RefusedInputs = ""
	s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason, "The machine is installing")
	if err := s.checkpoint(ctx); err != nil {
		return ctrl.Result{}, err
	}

	timeout := r.InstallTimeout
	if timeout == 0 {
		timeout = DefaultInstallTimeout
	}
	installCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	log.Info("Installing the machine", "image", s.cfg.Spec.Image)
	err = conn.Install(installCtx, sdk.InstallRequest{
		Node:      s.machine.Name,
		Documents: docs,
		Secrets:   sec,
		Image:     string(s.cfg.Spec.Image),
		Keychain:  kc,
	})
	switch {
	case sdk.Refused(err):
		s.refuse(bootstrapv1.ConfigRefusedReason, err.Error(), in)
		return ctrl.Result{}, nil
	case errors.Is(err, sdk.ErrOSCARefused):
		s.refuse(bootstrapv1.ImageRefusedReason, err.Error(), in)
		return ctrl.Result{}, nil
	case sdk.InstalledAndReboots(err):
		// The machine accepted an install already and reboots into it.
		s.cfg.Status.LastInstallTime = ptr.To(metav1.Now())
		s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason, "The machine is installed and reboots")
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	case err != nil:
		s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason, fmt.Sprintf("The install failed: %v", err))
		return ctrl.Result{}, err
	}
	log.Info("The machine accepted the install and reboots into it")
	s.cfg.Status.LastInstallTime = ptr.To(metav1.Now())
	return r.verify(ctx, s, addr, sec, false)
}

// verify asks the machine at addr whether it answers as its node with the image it installed,
// which marks it installed. A machine that was installed and answers in maintenance mode again,
// or one that runs another image, fell back, which a MachineHealthCheck repairs by replacing it:
// it is never installed again.
func (r *ChalkosConfigReconciler) verify(ctx context.Context, s *scope, addr string, sec clustersecrets.Secrets, wasInstalled bool) (ctrl.Result, error) {
	unreachable := s.set
	if wasInstalled {
		unreachable = s.setReady
	}
	if addr == "" {
		unreachable(metav1.ConditionFalse, bootstrapv1.NodeUnreachableReason, "The Machine has no InternalIP")
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	info, err := r.Node.Installed(ctx, addr, sec, s.machine.Name)
	switch {
	case err == nil:
		s.cfg.Status.Installed = ptr.To(true)
		if got, want := hex.EncodeToString(info.GetRootHash()), s.cfg.Status.ImageRootHash; want != "" && got != want {
			s.set(metav1.ConditionFalse, bootstrapv1.ImageFellBackReason, fmt.Sprintf("The machine runs the image of root hash %s, not the %s it installed", got, want))
			return ctrl.Result{}, nil
		}
		s.set(metav1.ConditionTrue, bootstrapv1.InstalledReason, "")
		return ctrl.Result{RequeueAfter: wait.Jitter(verifyEvery, 0.2)}, nil
	case errors.Is(err, sdk.ErrNotInstalled) && wasInstalled:
		s.set(metav1.ConditionFalse, bootstrapv1.FellBackToMaintenanceReason, "The installed machine answers in maintenance mode")
		return ctrl.Result{}, nil
	case errors.Is(err, sdk.ErrNotInstalled):
		s.set(metav1.ConditionFalse, bootstrapv1.InstallingReason, "The machine does not answer as its node yet")
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	unreachable(metav1.ConditionFalse, bootstrapv1.NodeUnreachableReason, err.Error())
	return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
}

// maxInstalls is how many installs run at once.
func (r *ChalkosConfigReconciler) maxInstalls() int {
	if r.MaxInstalls > 0 {
		return r.MaxInstalls
	}
	return DefaultMaxInstalls
}

// takeSlot takes an install slot, if one is free, and returns its release.
func (r *ChalkosConfigReconciler) takeSlot() (release func(), ok bool) {
	r.slotsOnce.Do(func() { r.slots = make(chan struct{}, r.maxInstalls()) })
	select {
	case r.slots <- struct{}{}:
		return func() { <-r.slots }, true
	default:
		return nil, false
	}
}

// clusterFiles are the spec.files of the Cluster's ChalkosControlPlane: the cluster-wide
// documents. A Cluster with another control plane fails with errNoChalkosControlPlane.
func (r *ChalkosConfigReconciler) clusterFiles(ctx context.Context, cluster *clusterv1.Cluster) ([]runtime.RawExtension, error) {
	ref := cluster.Spec.ControlPlaneRef
	if ref.APIGroup != controlplanev1.GroupVersion.Group || ref.Kind != "ChalkosControlPlane" {
		return nil, errNoChalkosControlPlane
	}
	var cp controlplanev1.ChalkosControlPlane
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: cluster.Namespace, Name: ref.Name}, &cp); err != nil {
		return nil, fmt.Errorf("the Cluster's ChalkosControlPlane: %w", err)
	}
	return cp.Spec.Files, nil
}

// inputsDigest is the SHA-256, in hex, of the canonical JSON of what an install of the machine is
// made from, which a refusal stands for.
func inputsDigest(m documents.Machine, cp clusterv1.ContractVersionedObjectReference, clusterFiles, poolFiles []runtime.RawExtension, image, osCA string) (string, error) {
	canonical := func(raws []runtime.RawExtension) ([]any, error) {
		out := make([]any, len(raws))
		for i, raw := range raws {
			if err := json.Unmarshal(raw.Raw, &out[i]); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	cluster, err := canonical(clusterFiles)
	if err != nil {
		return "", err
	}
	pool, err := canonical(poolFiles)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(struct {
		Machine      documents.Machine
		ControlPlane clusterv1.ContractVersionedObjectReference
		ClusterFiles []any
		PoolFiles    []any
		Image        string
		OSCA         string
	}{m, cp, cluster, pool, image, osCA})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// checkImage refuses an image that trusts another OS CA than the secrets' or whose kubelet is not
// of the Machine's version.
func checkImage(a *ociimage.Artifact, sec clustersecrets.Secrets, version string) error {
	if err := sdk.CheckImageOSCA(a.Metadata, sec); err != nil {
		return err
	}
	img, ok, err := sdkkubernetes.ImageOf(a.Contract)
	switch {
	case err != nil:
		return fmt.Errorf("the image's Kubernetes facts: %w", err)
	case !ok:
		return fmt.Errorf("the image %s has no Kubernetes layer", a.Reference)
	case version != "" && img.Version != strings.TrimPrefix(version, "v"):
		return fmt.Errorf("the image's kubelet is of release %s, the Machine's version is %s", img.Version, version)
	}
	return nil
}

// machineOf is what the documents of the Machine are generated from.
func machineOf(m *clusterv1.Machine, cluster *clusterv1.Cluster) documents.Machine {
	typ := documents.Worker
	if util.IsControlPlaneMachine(m) {
		typ = documents.ControlPlane
	}
	network := cluster.Spec.ClusterNetwork
	return documents.Machine{
		Name:         m.Name,
		Type:         typ,
		ProviderID:   m.Spec.ProviderID,
		Endpoint:     documents.Endpoint(cluster.Spec.ControlPlaneEndpoint.Host, cluster.Spec.ControlPlaneEndpoint.Port),
		PodCIDRs:     network.Pods.CIDRBlocks,
		ServiceCIDRs: network.Services.CIDRBlocks,
		Version:      m.Spec.Version,
	}
}

// recordFingerprint appends the fingerprint seen at now unless it is the last one recorded,
// keeping the newest maxFingerprints. It tells whether it appended the fingerprint.
func recordFingerprint(cfg *bootstrapv1.ChalkosConfig, fingerprint string, now time.Time) bool {
	fps := cfg.Status.MaintenanceFingerprints
	if len(fps) > 0 && fps[len(fps)-1].SHA256 == fingerprint {
		return false
	}
	fps = append(fps, bootstrapv1.Fingerprint{SHA256: fingerprint, SeenAt: metav1.NewTime(now)})
	cfg.Status.MaintenanceFingerprints = slices.Clone(fps[max(0, len(fps)-maxFingerprints):])
	return true
}

// machineToConfig maps a Machine to its ChalkosConfig.
func machineToConfig(_ context.Context, o client.Object) []ctrl.Request {
	m, ok := o.(*clusterv1.Machine)
	if !ok || !isChalkosConfig(m.Spec.Bootstrap.ConfigRef) {
		return nil
	}
	return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}}}
}

// clusterToConfigs maps a Cluster to the ChalkosConfigs of its Machines.
func (r *ChalkosConfigReconciler) clusterToConfigs(ctx context.Context, o client.Object) []ctrl.Request {
	return r.configsOf(ctx, client.ObjectKeyFromObject(o))
}

// controlPlaneToConfigs maps a ChalkosControlPlane to the ChalkosConfigs of its owner Cluster's
// Machines.
func (r *ChalkosConfigReconciler) controlPlaneToConfigs(ctx context.Context, o client.Object) []ctrl.Request {
	for _, ref := range o.GetOwnerReferences() {
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err == nil && ref.Kind == "Cluster" && gv.Group == clusterv1.GroupVersion.Group {
			return r.configsOf(ctx, client.ObjectKey{Namespace: o.GetNamespace(), Name: ref.Name})
		}
	}
	return nil
}

// configsOf are the ChalkosConfigs of the Machines of the cluster.
func (r *ChalkosConfigReconciler) configsOf(ctx context.Context, cluster client.ObjectKey) []ctrl.Request {
	var machines clusterv1.MachineList
	if err := r.Client.List(ctx, &machines, client.InNamespace(cluster.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name}); err != nil {
		return nil
	}
	var requests []ctrl.Request
	for i := range machines.Items {
		requests = append(requests, machineToConfig(ctx, &machines.Items[i])...)
	}
	return requests
}

// isChalkosConfig tells whether ref names a ChalkosConfig.
func isChalkosConfig(ref clusterv1.ContractVersionedObjectReference) bool {
	return ref.IsDefined() && ref.APIGroup == bootstrapv1.GroupVersion.Group && ref.Kind == "ChalkosConfig"
}

// machineChanged passes the creation and deletion of a Machine, and the updates that change what
// a reconcile reads of it: its providerID, addresses, version, labels, bootstrap data Secret,
// infrastructure's readiness, deletion, pre-terminate hook and Deleting condition's reason. The
// periodic verify of an installed machine covers any other change.
func machineChanged() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			old, ok := e.ObjectOld.(*clusterv1.Machine)
			cur, ok2 := e.ObjectNew.(*clusterv1.Machine)
			if !ok || !ok2 {
				return false
			}
			return old.Spec.ProviderID != cur.Spec.ProviderID ||
				!slices.Equal(old.Status.Addresses, cur.Status.Addresses) ||
				old.Spec.Version != cur.Spec.Version ||
				!maps.Equal(old.Labels, cur.Labels) ||
				!ptr.Equal(old.Spec.Bootstrap.DataSecretName, cur.Spec.Bootstrap.DataSecretName) ||
				!ptr.Equal(old.Status.Initialization.InfrastructureProvisioned, cur.Status.Initialization.InfrastructureProvisioned) ||
				!old.DeletionTimestamp.Equal(cur.DeletionTimestamp) ||
				old.Annotations[machineutil.HookAnnotation] != cur.Annotations[machineutil.HookAnnotation] ||
				machineutil.DeletingReason(old) != machineutil.DeletingReason(cur)
		},
	}
}

// controlPlaneInitialized passes the updates of a Cluster whose
// status.initialization.controlPlaneInitialized turned true, which a worker's install waits for.
// Cluster API's ClusterControlPlaneInitialized predicate watches the Cluster's condition instead,
// which may turn true in an earlier update than the field the reconcile reads.
func controlPlaneInitialized() predicate.Funcs {
	initialized := func(o client.Object) bool {
		c, ok := o.(*clusterv1.Cluster)
		return ok && ptr.Deref(c.Status.Initialization.ControlPlaneInitialized, false)
	}
	return predicate.Funcs{
		UpdateFunc:  func(e event.UpdateEvent) bool { return !initialized(e.ObjectOld) && initialized(e.ObjectNew) },
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// clusterInputsChanged passes the updates of a Cluster that change the fields an install reads
// of it: spec.controlPlaneEndpoint, spec.clusterNetwork and spec.controlPlaneRef. A refusal's
// inputs include them, so such an update retries a refused install.
func clusterInputsChanged() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			old, ok := e.ObjectOld.(*clusterv1.Cluster)
			cur, ok2 := e.ObjectNew.(*clusterv1.Cluster)
			if !ok || !ok2 {
				return false
			}
			return old.Spec.ControlPlaneEndpoint != cur.Spec.ControlPlaneEndpoint ||
				old.Spec.ControlPlaneRef != cur.Spec.ControlPlaneRef ||
				!equality.Semantic.DeepEqual(old.Spec.ClusterNetwork, cur.Spec.ClusterNetwork)
		},
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
