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
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"

	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/images"
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
		return fmt.Sprintf("The Machine carries no %s, and its ChalkosConfig is of another image than the control plane's", controlplanev1.SpecHashAnnotation)
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
	annotations := s.cp.Spec.MachineTemplate.ObjectMeta.Annotations
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
// tells whether it stamped the Machine, which the caller writes.
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
// the contract nothing is recorded, and it returns when to try again.
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
		// The ChalkosControlPlane's or the Cluster's change reconciles again.
		log.Info("Not recording the observed files", "reason", err.Error())
		return ctrl.Result{}
	}
	c, err := r.contract(ctx, s.cp)
	if err != nil {
		log.Info("Not recording the observed files yet: reading the image's contract", "reason", err.Error())
		return ctrl.Result{RequeueAfter: machineutil.RetryAfter}
	}
	text, err := documents.Observed(c, docs)
	if err != nil {
		log.Info("Not recording the observed files", "reason", err.Error())
		return ctrl.Result{}
	}
	s.cp.Status.ObservedFiles = text
	return ctrl.Result{}
}

// contract is the contract of the ChalkosControlPlane's image, which the Machines are built from,
// read with its image pull Secret.
func (r *ChalkosControlPlaneReconciler) contract(ctx context.Context, cp *controlplanev1.ChalkosControlPlane) (sdk.Contract, error) {
	kc, err := images.Keychain(ctx, r.APIReader, cp.Namespace, cp.Spec.ImagePullSecretRef)
	if err != nil {
		return sdk.Contract{}, err
	}
	a, err := r.artifacts.Image(ctx, r.Node, string(cp.Spec.Image), cp.Namespace, cp.Spec.ImagePullSecretRef, kc)
	if err != nil {
		return sdk.Contract{}, fmt.Errorf("reading the image: %w", err)
	}
	return r.contracts.of(a.Reference, func() (sdk.Contract, error) { return sdk.ArtifactContract(layers.All(), a) })
}

// maxContracts is how many contracts a contractCache holds.
const maxContracts = 64

// contractCache holds the contracts of images read before, by reference: an image named by digest
// keeps its contract, which is built from its schemas once. It is safe for concurrent use; its
// zero value is empty.
type contractCache struct {
	mu        sync.Mutex
	contracts map[string]sdk.Contract
}

// of returns the contract held for ref, or builds it with build and holds it, dropping the others
// when the cache is full.
func (c *contractCache) of(ref string, build func() (sdk.Contract, error)) (sdk.Contract, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ct, ok := c.contracts[ref]; ok {
		return ct, nil
	}
	ct, err := build()
	if err != nil {
		return sdk.Contract{}, err
	}
	if c.contracts == nil || len(c.contracts) >= maxContracts {
		c.contracts = map[string]sdk.Contract{}
	}
	c.contracts[ref] = ct
	return ct, nil
}
