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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"

	"github.com/trevex/chalkos/pkg/clustersecrets"
	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
)

// hashedSpec is what specHash hashes: what a control-plane Machine is made from and cannot change
// once it exists.
type hashedSpec struct {
	Version           string    `json:"version"`
	Image             string    `json:"image"`
	Files             []any     `json:"files"`
	InfrastructureRef hashedRef `json:"infrastructureRef"`
}

// hashedRef is the infrastructure template's reference as specHash hashes it.
type hashedRef struct {
	APIGroup string `json:"apiGroup"`
	Kind     string `json:"kind"`
	Name     string `json:"name"`
}

// specHash is the SHA-256, in hex, of the canonical JSON of the ChalkosControlPlane's version,
// image, files and infrastructure template, which SpecHashAnnotation records on each Machine.
func specHash(cp *controlplanev1.ChalkosControlPlane) (string, error) {
	files, err := canonicalFiles(cp.Spec.Files)
	if err != nil {
		return "", err
	}
	ref := cp.Spec.MachineTemplate.Spec.InfrastructureRef
	data, err := json.Marshal(hashedSpec{
		Version: cp.Spec.Version, Image: string(cp.Spec.Image), Files: files,
		InfrastructureRef: hashedRef{APIGroup: ref.APIGroup, Kind: ref.Kind, Name: ref.Name},
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalFiles are the files decoded with their numbers exact, so that json.Marshal writes each
// alike whatever the order of its keys and its spacing.
func canonicalFiles(raws []runtime.RawExtension) ([]any, error) {
	out := make([]any, len(raws))
	for i, raw := range raws {
		d := json.NewDecoder(bytes.NewReader(raw.Raw))
		d.UseNumber()
		if err := d.Decode(&out[i]); err != nil {
			return nil, fmt.Errorf("spec.files[%d]: %w", i, err)
		}
	}
	return out, nil
}

// outOfDate says why the Machine is not of the control plane's spec; empty when it is.
func (s *scope) outOfDate(m *clusterv1.Machine) string {
	switch hash, ok := m.Annotations[controlplanev1.SpecHashAnnotation]; {
	case !ok && m.Spec.Version != s.cp.Spec.Version:
		return fmt.Sprintf("The Machine carries no %s and is of version %s, the control plane of %s", controlplanev1.SpecHashAnnotation, m.Spec.Version, s.cp.Spec.Version)
	case !ok:
		return fmt.Sprintf("The Machine carries no %s, and its ChalkosConfig is missing or of another image than the control plane's", controlplanev1.SpecHashAnnotation)
	case hash != s.hash:
		return "The Machine was made from another version, image, files or infrastructure template than the control plane's"
	case !s.rolloutAfter.IsZero() && m.CreationTimestamp.Before(&metav1.Time{Time: s.rolloutAfter}):
		return "The Machine was created before spec.rollout.after, " + s.rolloutAfter.UTC().Format(time.RFC3339)
	}
	return ""
}

// untilRolloutAfter reconciles again when spec.rollout.after passes, which makes the Machines
// created before it out of date.
func (r *ChalkosControlPlaneReconciler) untilRolloutAfter(s *scope) ctrl.Result {
	after := s.cp.Spec.Rollout.After.Time
	if now := r.clock().Now(); !after.IsZero() && now.Before(after) {
		return ctrl.Result{RequeueAfter: after.Sub(now)}
	}
	return ctrl.Result{}
}

// syncMachines brings each Machine that is not being deleted in line with the ChalkosControlPlane
// where that needs no new Machine: the machine template's labels and annotations, the spec hash
// of a Machine made before Machines carried one, and its UpToDate condition. Labels and
// annotations the template no longer names stay.
func (r *ChalkosControlPlaneReconciler) syncMachines(ctx context.Context, s *scope) error {
	labels := machineLabels(s.cp, s.cluster.Name)
	annotations := templateAnnotations(s.cp)
	for i := range s.machines {
		m := &s.machines[i]
		h, err := patch.NewHelper(m, r.Client)
		if err != nil {
			return err
		}
		changed, err := r.adopt(ctx, s, m)
		if err != nil {
			return err
		}
		changed = setMeta(&m.Labels, labels) || changed
		changed = setMeta(&m.Annotations, annotations) || changed
		want := metav1.Condition{Type: clusterv1.MachineUpToDateCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineUpToDateReason}
		if why := s.outOfDate(m); why != "" {
			want.Status, want.Reason, want.Message = metav1.ConditionFalse, clusterv1.MachineNotUpToDateReason, why
		}
		if got := conditions.Get(m, want.Type); got == nil || got.Status != want.Status || got.Reason != want.Reason || got.Message != want.Message {
			conditions.Set(m, want)
			changed = true
		}
		if !changed {
			continue
		}
		if err := h.Patch(ctx, m, patch.WithOwnedConditions{Conditions: []string{clusterv1.MachineUpToDateCondition}}); err != nil {
			return fmt.Errorf("update the Machine %s: %w", m.Name, err)
		}
	}
	return nil
}

// setMeta sets each key of want in *have to its value, and tells whether it changed any.
func setMeta(have *map[string]string, want map[string]string) bool {
	changed := false
	for k, v := range want {
		if cur, ok := (*have)[k]; ok && cur == v {
			continue
		}
		if *have == nil {
			*have = map[string]string{}
		}
		(*have)[k] = v
		changed = true
	}
	return changed
}

// adopt stamps the spec hash on a Machine that carries none, made by a provider before Machines
// carried one, and on its ChalkosConfig, when the Machine is of the control plane's version and
// its ChalkosConfig of its image: nothing else a Machine is made from could change before. It
// stamps today's hash even when spec.files changed while an older provider ran, which is not
// told from the Machine. It tells whether it stamped the Machine, which the caller writes.
func (r *ChalkosControlPlaneReconciler) adopt(ctx context.Context, s *scope, m *clusterv1.Machine) (bool, error) {
	if _, ok := m.Annotations[controlplanev1.SpecHashAnnotation]; ok || m.Spec.Version != s.cp.Spec.Version {
		return false, nil
	}
	var cfg bootstrapv1.ChalkosConfig
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, &cfg); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	if cfg.Spec.Image != s.cp.Spec.Image {
		return false, nil
	}
	original := cfg.DeepCopy()
	metav1.SetMetaDataAnnotation(&cfg.ObjectMeta, controlplanev1.SpecHashAnnotation, s.hash)
	if err := r.Client.Patch(ctx, &cfg, client.MergeFrom(original)); err != nil {
		return false, fmt.Errorf("stamp the ChalkosConfig %s: %w", cfg.Name, err)
	}
	metav1.SetMetaDataAnnotation(&m.ObjectMeta, controlplanev1.SpecHashAnnotation, s.hash)
	ctrl.LoggerFrom(ctx).Info("Adopted a Machine made before Machines carried the spec hash", "machine", m.Name)
	return true, nil
}

// observeFiles records in status.observedFiles the cluster-wide documents every Machine was made
// from, with the defaults of the image's contract written out, once a Machine exists and none is
// out of date or being deleted: at the first Machine's creation and when a roll finished. Without
// the contract nothing is recorded, and it returns when to try again. A pending change of a fixed
// field, as through the Cluster's clusterNetwork or controlPlaneEndpoint, which the spec hash
// does not cover, keeps the record. A first Machine created while the image cannot be read is
// recorded later, from the Cluster as it is then: a change of the Cluster in between goes unseen.
func (r *ChalkosControlPlaneReconciler) observeFiles(ctx context.Context, s *scope) ctrl.Result {
	if len(s.machines) == 0 || len(s.deleting) > 0 {
		return ctrl.Result{}
	}
	for i := range s.machines {
		if s.outOfDate(&s.machines[i]) != "" {
			return ctrl.Result{}
		}
	}
	log := ctrl.LoggerFrom(ctx)
	docs, err := documents.ClusterWide(s.cluster, s.cp.Spec.Version, s.cp.Spec.Files)
	if err != nil {
		// The guard reports it; a change of the files or of the Cluster reconciles again.
		log.V(1).Info("Not recording the observed files", "reason", err.Error())
		return ctrl.Result{}
	}
	c, err := r.contract(ctx, s.cp)
	if err != nil {
		// The ScalingUp condition reports it.
		s.imageWaiting = fmt.Sprintf("the image %s cannot be read, so status.observedFiles is not recorded yet", s.cp.Spec.Image)
		log.V(1).Info("Not recording the observed files yet: reading the image's contract", "reason", err.Error())
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}
	}
	if observed := s.cp.Status.ObservedFiles; observed != "" {
		// A pending change of a fixed field keeps the record; the guard reports it.
		if changes, err := documents.FixedChanges(c, observed, docs); err != nil || len(changes) > 0 {
			return ctrl.Result{}
		}
	}
	text, err := documents.Observed(c, docs)
	if err != nil {
		log.Info("Not recording the observed files", "reason", err.Error())
		return ctrl.Result{}
	}
	s.cp.Status.ObservedFiles = text
	return ctrl.Result{}
}

// versionSkew says why the Machines cannot move to version in one step: Kubernetes upgrades a
// control plane one minor at a time and never downgrades a minor. It is empty when they can, as
// for a change of the patch version alone.
func versionSkew(version string, machines []clusterv1.Machine) string {
	want, err := utilversion.ParseSemantic(version)
	if err != nil {
		return fmt.Sprintf("spec.version %s is no Kubernetes version: %v", version, err)
	}
	for i := range machines {
		m := &machines[i]
		if m.Spec.Version == "" {
			return fmt.Sprintf("The Machine %s has no spec.version; set it to the Kubernetes version the Machine runs", m.Name)
		}
		have, err := utilversion.ParseSemantic(m.Spec.Version)
		if err != nil {
			return fmt.Sprintf("The Machine %s is of version %s, which is no Kubernetes version; set its spec.version to the version the Machine runs", m.Name, m.Spec.Version)
		}
		switch {
		case want.Major() != have.Major():
			return fmt.Sprintf("spec.version %s is of another major version than the Machine %s, of %s; Kubernetes upgrades one minor at a time", version, m.Name, m.Spec.Version)
		case want.Minor() < have.Minor():
			return fmt.Sprintf("spec.version %s is of an older minor than the Machine %s, of %s; Kubernetes does not downgrade a minor, so keep spec.version at v%d.%d or newer", version, m.Name, m.Spec.Version, have.Major(), have.Minor())
		case want.Minor() > have.Minor()+1:
			return fmt.Sprintf("spec.version %s is more than one minor newer than the Machine %s, of %s; upgrade one minor at a time, to v%d.%d first", version, m.Name, m.Spec.Version, have.Major(), have.Minor()+1)
		}
	}
	return ""
}

// guard records in s why the control plane may create no Machine now: a version its Machines,
// being deleted included, cannot reach in one step; spec.files that, with the fields filled from
// the Cluster, make no control-plane Machine's documents; or cluster-wide documents that change,
// against status.observedFiles, a field every node of the cluster holds alike, as the contract of
// spec.image marks it. Without observed files there is nothing to compare. An image that cannot
// be read holds, and is read again, only while a Machine is wanted: while the control plane has
// fewer Machines than spec.replicas or a Machine is not up to date. Otherwise observeFiles
// reports it, once no Machine is being deleted.
func (r *ChalkosControlPlaneReconciler) guard(ctx context.Context, s *scope) ctrl.Result {
	// A Machine being deleted runs until it is gone, so a new Machine must reach it too.
	if msg := versionSkew(s.cp.Spec.Version, append(slices.Clone(s.machines), s.deleting...)); msg != "" {
		s.held, s.heldReason = msg, controlplanev1.VersionSkewReason
		return ctrl.Result{}
	}
	// reconcile reaches the guard only once the Cluster has an endpoint, so an error here is one
	// of the files or of the Cluster's network, which a change of either reconciles again.
	docs, err := documents.ClusterWide(s.cluster, s.cp.Spec.Version, s.cp.Spec.Files)
	if err != nil {
		s.held, s.heldReason = "spec.files cannot be made into a control-plane Machine's documents: "+err.Error(), controlplanev1.InvalidFilesReason
		return ctrl.Result{}
	}
	observed := s.cp.Status.ObservedFiles
	if observed == "" {
		return ctrl.Result{}
	}
	c, err := r.contract(ctx, s.cp)
	if err != nil {
		if !s.machineWanted() {
			return ctrl.Result{}
		}
		ctrl.LoggerFrom(ctx).V(1).Info("Not checking the cluster-wide documents yet: reading the image's contract", "reason", err.Error())
		// The message stays alike at every attempt; the error itself is logged.
		s.held = fmt.Sprintf("the image %s cannot be read, so the cluster-wide documents are not checked against it yet", s.cp.Spec.Image)
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}
	}
	if text, err := documents.Observed(c, docs); err == nil && text == observed {
		return ctrl.Result{}
	}
	changes, err := documents.FixedChanges(c, observed, docs)
	switch {
	case err != nil:
		// Clearing the record or a change of the files reconciles again.
		s.held = fmt.Sprintf("status.observedFiles cannot be compared with the cluster-wide documents: %v; if the record is at fault, "+
			"clear it, and the next Machine is made without the check: kubectl patch chalkoscontrolplane %s -n %s --subresource=status --type=merge -p '{\"status\":{\"observedFiles\":null}}'",
			err, s.cp.Name, s.cp.Namespace)
		s.heldReason = controlplanev1.InvalidFilesReason
	case len(changes) > 0:
		s.held, s.heldReason = documents.DescribeFixed(changes), controlplanev1.FixedFieldChangedReason
	}
	return ctrl.Result{}
}

// machineWanted tells whether the control plane wants a new Machine: it has fewer Machines than
// spec.replicas asks for, or a Machine is not up to date.
func (s *scope) machineWanted() bool {
	if len(s.machines) < int(ptr.Deref(s.cp.Spec.Replicas, 1)) {
		return true
	}
	return s.anyOutOfDate()
}

// surge is spec.rollout.strategy.rollingUpdate.maxSurge: how many Machines above spec.replicas
// the control plane runs while it replaces one, 1 by default.
func surge(cp *controlplanev1.ChalkosControlPlane) int {
	return int(ptr.Deref(cp.Spec.Rollout.Strategy.RollingUpdate.MaxSurge, 1))
}

// contract is the contract of the ChalkosControlPlane's image, which the Machines are built from,
// read with its image pull Secret.
func (r *ChalkosControlPlaneReconciler) contract(ctx context.Context, cp *controlplanev1.ChalkosControlPlane) (sdk.Contract, error) {
	return r.artifacts.Contract(ctx, r.APIReader, r.Node, string(cp.Spec.Image), cp.Namespace, cp.Spec.ImagePullSecretRef)
}

// stepProblem says why etcd may not gain or lose a member by a scale-down or roll step now: a
// Machine that has not joined the cluster, or etcd's members as etcdProblem reads them; empty when
// nothing does. leaving, when not nil, is the Machine the step deletes. When it has not joined,
// it holds no vote in etcd, so its join is not waited for and etcdProblem leaves it out, as
// Cluster API's own control plane does: a replacement that never joins can go once its change
// is reverted. Every other Machine must have joined.
func (r *ChalkosControlPlaneReconciler) stepProblem(ctx context.Context, s *scope, sec clustersecrets.Secrets, leaving *clusterv1.Machine) (string, error) {
	var unjoined *clusterv1.Machine
	if leaving != nil {
		ok, err := r.joined(ctx, s, leaving, sec)
		if err != nil {
			return "", err
		}
		if !ok {
			unjoined = leaving
		}
	}
	for i := range s.machines {
		m := &s.machines[i]
		if unjoined != nil && m.Name == unjoined.Name {
			continue
		}
		ok, err := r.joined(ctx, s, m, sec)
		if err != nil {
			return "", err
		}
		if !ok {
			return fmt.Sprintf("waiting for the Machine %s to join the cluster", m.Name), nil
		}
	}
	return r.etcdProblem(ctx, s, sec, unjoined), nil
}

// anyOutOfDate tells whether one of the control plane's Machines that are not being deleted is not
// up to date.
func (s *scope) anyOutOfDate() bool {
	return slices.ContainsFunc(s.machines, func(m clusterv1.Machine) bool { return s.outOfDate(&m) != "" })
}

// oldest is the oldest of the control plane's Machines that are not being deleted, as oldestOf
// orders them, or with outOfDate the oldest of those that are not up to date; nil when there is
// none.
func (r *ChalkosControlPlaneReconciler) oldest(ctx context.Context, s *scope, sec clustersecrets.Secrets, outOfDate bool) (*clusterv1.Machine, error) {
	var ms []*clusterv1.Machine
	for i := range s.machines {
		if !outOfDate || s.outOfDate(&s.machines[i]) != "" {
			ms = append(ms, &s.machines[i])
		}
	}
	return oldestOf(ms, func(m *clusterv1.Machine) (bool, error) { return r.joined(ctx, s, m, sec) })
}

// oldestOf is the first of ms, which are sorted by creation time, then by name. The API server
// records creation times to the second, so among the Machines created in the same second as the
// first, the first that joined is taken: one that has not joined yet is the newer in effect. nil
// when ms is empty.
func oldestOf(ms []*clusterv1.Machine, joined func(*clusterv1.Machine) (bool, error)) (*clusterv1.Machine, error) {
	switch {
	case len(ms) == 0:
		return nil, nil
	case len(ms) == 1 || !ms[1].CreationTimestamp.Equal(&ms[0].CreationTimestamp):
		// No tie, and no need to ask whether the Machine joined.
		return ms[0], nil
	}
	for _, m := range ms {
		if !m.CreationTimestamp.Equal(&ms[0].CreationTimestamp) {
			break
		}
		ok, err := joined(m)
		if err != nil {
			return nil, err
		}
		if ok {
			return m, nil
		}
	}
	return ms[0], nil
}

// rollOut takes the roll's next step while a Machine is not up to date and the control plane has
// as many Machines as spec.replicas asks for, none being deleted. With maxSurge 1 it creates an
// up-to-date Machine; once that one joined, scaleDown deletes the oldest Machine that is not up to
// date. With maxSurge 0 it deletes that Machine first; scaleUp creates its replacement once it is
// gone. A step starts only when the control plane is initialized, no guard holds, no other step
// was taken in this reconcile, stepProblem finds nothing and the image accepts the documents the
// new Machine installs with.
func (r *ChalkosControlPlaneReconciler) rollOut(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	want := int(ptr.Deref(s.cp.Spec.Replicas, 1))
	switch {
	case !s.anyOutOfDate() || s.held != "" || s.stepped || !s.initialized():
		// RollingOut reports a guard; a step taken and the initialization reconcile again.
		return ctrl.Result{}, nil
	case len(s.machines) < want:
		s.rollWaiting = s.scaleUpWaiting
		return ctrl.Result{}, nil
	case len(s.deleting) > 0:
		// The Machine's deletion reconciles again.
		s.rollWaiting = fmt.Sprintf("waiting for the Machine %s, being deleted, to go", s.deleting[0].Name)
		return ctrl.Result{}, nil
	case len(s.machines) > want:
		s.rollWaiting = s.scaleDownWaiting
		return ctrl.Result{}, nil
	}
	old, err := r.oldest(ctx, s, sec, true)
	if err != nil {
		return ctrl.Result{}, err
	}
	// With maxSurge 0 the step deletes old, which may go without its join; a Machine created
	// waits for every other to have joined.
	var leaving *clusterv1.Machine
	if surge(s.cp) == 0 {
		leaving = old
	}
	why, err := r.stepProblem(ctx, s, sec, leaving)
	if err != nil {
		return ctrl.Result{}, err
	}
	if why != "" {
		s.rollWaiting = why
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
	}
	if res := r.checkReplacement(ctx, s, old); s.held != "" {
		return res, nil
	}
	log := ctrl.LoggerFrom(ctx)
	if surge(s.cp) > 0 {
		m, err := r.createMachine(ctx, s)
		if err != nil {
			return ctrl.Result{}, err
		}
		log.Info("Rolling out: created an up-to-date Machine", "machine", m.Name, "replaces", old.Name)
		machineutil.Event(r.Recorder, s.cp, m, corev1.EventTypeNormal, "RollingOut", "CreateMachine",
			"Creating the Machine %s to replace %s, which is not up to date", m.Name, old.Name)
		s.machines = append(s.machines, *m)
		s.stepped = true
		return ctrl.Result{}, nil
	}
	if err := r.deleteMachine(ctx, old); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("Rolling out: deleting the oldest Machine that is not up to date", "machine", old.Name)
	machineutil.Event(r.Recorder, s.cp, old, corev1.EventTypeNormal, "RollingOut", "DeleteMachine",
		"Deleting the Machine %s, which is not up to date; its replacement is created once it is gone", old.Name)
	s.removing = fmt.Sprintf("the Machine %s is deleted to be replaced", old.Name)
	s.stepped = true
	// The Machine's deletion reconciles again.
	return ctrl.Result{}, nil
}

// checkReplacement holds the roll, in s.held, unless spec.image accepts the documents that the
// Machine replacing old installs with, as the image's contract checks them before an install:
// with maxSurge 0 old goes first, and must not go while no replacement can install. A refusal
// holds with InvalidFilesReason until the files or the image change; a check that could not be
// made, as while the image cannot be read, holds without a reason and returns when to try again.
func (r *ChalkosControlPlaneReconciler) checkReplacement(ctx context.Context, s *scope, old *clusterv1.Machine) ctrl.Result {
	log := ctrl.LoggerFrom(ctx)
	c, err := r.contract(ctx, s.cp)
	if err != nil {
		log.V(1).Info("Not rolling out yet: reading the image's contract", "reason", err.Error())
		// The message stays alike at every attempt; the error itself is logged.
		s.held = fmt.Sprintf("the image %s cannot be read, so the documents of the Machine to replace %s with are not checked against it yet", s.cp.Spec.Image, old.Name)
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}
	}
	// The replacement is checked under the name of the Machine it replaces: its own is made at
	// its creation.
	docs, err := documents.For(documents.ClusterMachine(s.cluster, old.Name, documents.ControlPlane, s.cp.Spec.Version), s.cp.Spec.Files, nil)
	if err != nil {
		s.held, s.heldReason = "spec.files cannot be made into a control-plane Machine's documents: "+err.Error(), controlplanev1.InvalidFilesReason
		return ctrl.Result{}
	}
	_, err = sdk.Check(ctx, c, old.Name, sdk.NodeDocuments(layers.All(), docs))
	switch {
	case err == nil:
	case sdk.Refused(err):
		s.held = fmt.Sprintf("the image %s refuses the documents the Machine to replace %s with would install with: %v", s.cp.Spec.Image, old.Name, err)
		s.heldReason = controlplanev1.InvalidFilesReason
	default:
		log.V(1).Info("Not rolling out yet: checking the documents against the image", "reason", err.Error())
		s.held = fmt.Sprintf("the documents of the Machine to replace %s with could not be checked against the image %s yet", old.Name, s.cp.Spec.Image)
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}
	}
	return ctrl.Result{}
}
