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
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/labels/format"

	"github.com/trevex/chalkos/pkg/clustersecrets"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
)

// selectorLabels are the labels that select the control plane's Machines. The control plane's
// name is hashed when it is no valid label value, as Cluster API's own control plane does.
func selectorLabels(cp *controlplanev1.ChalkosControlPlane, cluster string) map[string]string {
	return map[string]string{
		clusterv1.ClusterNameLabel:             cluster,
		clusterv1.MachineControlPlaneNameLabel: format.MustFormatValue(cp.Name),
	}
}

// machineLabels are the labels of the control plane's Machines and of their bootstrap configs and
// infrastructure: the template's, those that mark them as the control plane's, and the control
// plane's watch filter label, so that the controller watching it sees them too.
func machineLabels(cp *controlplanev1.ChalkosControlPlane, cluster string) map[string]string {
	labels := maps.Clone(cp.Spec.MachineTemplate.ObjectMeta.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	maps.Copy(labels, selectorLabels(cp, cluster))
	labels[clusterv1.MachineControlPlaneLabel] = ""
	if v, ok := cp.Labels[clusterv1.WatchLabel]; ok {
		labels[clusterv1.WatchLabel] = v
	}
	return labels
}

// templateAnnotations are the machine template's annotations without those the controller sets
// itself, the spec hash and the pre-terminate hook, which the template cannot override.
func templateAnnotations(cp *controlplanev1.ChalkosControlPlane) map[string]string {
	annotations := maps.Clone(cp.Spec.MachineTemplate.ObjectMeta.Annotations)
	delete(annotations, controlplanev1.SpecHashAnnotation)
	delete(annotations, machineutil.HookAnnotation)
	return annotations
}

// listMachines lists the control plane's Machines uncached, so that a Machine created by the last
// reconcile is never created again, and splits them into those being deleted and the others.
func (r *ChalkosControlPlaneReconciler) listMachines(ctx context.Context, s *scope) error {
	var list clusterv1.MachineList
	if err := r.APIReader.List(ctx, &list, client.InNamespace(s.cp.Namespace), client.MatchingLabels(selectorLabels(s.cp, s.cluster.Name))); err != nil {
		return err
	}
	slices.SortFunc(list.Items, func(a, b clusterv1.Machine) int {
		if c := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	s.machines, s.deleting = nil, nil
	for _, m := range list.Items {
		if m.DeletionTimestamp.IsZero() {
			s.machines = append(s.machines, m)
		} else {
			s.deleting = append(s.deleting, m)
		}
	}
	s.listed = true
	return nil
}

// scaleUp creates the control plane's next Machine while it has fewer than it asks for: the first
// at once, every other only once the control plane is initialized and each Machine it has joined
// the cluster, so that etcd grows one member at a time. A Machine being deleted is not counted,
// so that it is replaced, but while one is, etcd must list only healthy voting members of the
// Machines: the replacement of a single control plane joins its etcd, while one of several that
// is down and still a member keeps etcd from growing by a member it may lose quorum with.
func (r *ChalkosControlPlaneReconciler) scaleUp(ctx context.Context, s *scope, sec clustersecrets.Secrets) (ctrl.Result, error) {
	if len(s.machines) >= int(ptr.Deref(s.cp.Spec.Replicas, 1)) {
		return ctrl.Result{}, nil
	}
	switch {
	case !s.initialized() && len(s.machines) > 0:
		return ctrl.Result{}, nil
	case !s.initialized() && s.isDeleting(s.cp.Status.BootstrappedMachine):
		// A new first Machine waits until the bootstrapped one, and its etcd, are gone.
		return ctrl.Result{}, nil
	case s.initialized():
		for i := range s.machines {
			ok, err := r.joined(ctx, s, &s.machines[i], sec)
			if err != nil || !ok {
				s.scaleUpWaiting = fmt.Sprintf("waiting for the Machine %s to join the cluster", s.machines[i].Name)
				return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, err
			}
		}
		if len(s.deleting) > 0 {
			if why := r.etcdProblem(ctx, s, sec); why != "" {
				s.scaleUpWaiting = "waiting while a Machine is deleted: " + why
				return ctrl.Result{RequeueAfter: machineutil.RetryAfter}, nil
			}
		}
	}
	m, err := r.createMachine(ctx, s)
	if err != nil {
		return ctrl.Result{}, err
	}
	ctrl.LoggerFrom(ctx).Info("Created a control-plane Machine", "machine", m.Name,
		"infrastructure", m.Spec.InfrastructureRef.Kind+"/"+m.Spec.InfrastructureRef.Name, "config", m.Spec.Bootstrap.ConfigRef.Name)
	s.machines = append(s.machines, *m)
	return ctrl.Result{}, nil
}

// createMachine creates a control-plane Machine with its infrastructure, cloned from the machine
// template's, and its ChalkosConfig, all of the same name. When one of them cannot be created, the
// others are deleted again.
func (r *ChalkosControlPlaneReconciler) createMachine(ctx context.Context, s *scope) (*clusterv1.Machine, error) {
	cp := s.cp
	name := fmt.Sprintf("%s-%s", cp.Name, utilrand.String(5))
	labels := machineLabels(cp, s.cluster.Name)
	annotations := templateAnnotations(cp)
	// The Machine controller makes the Machine the controller of its bootstrap config and
	// infrastructure; until then the ChalkosControlPlane owns them.
	owner := metav1.OwnerReference{APIVersion: controlPlaneKind.GroupVersion().String(), Kind: controlPlaneKind.Kind, Name: cp.Name, UID: cp.UID}

	// The template and its CRD are read uncached: the controller watches neither.
	c := uncached{Client: r.Client, reader: r.APIReader}
	ref := cp.Spec.MachineTemplate.Spec.InfrastructureRef
	template, err := external.GetObjectFromContractVersionedRef(ctx, c, ref, cp.Namespace)
	if err != nil {
		return nil, fmt.Errorf("the machine template's infrastructure: %w", err)
	}
	infra, infraRef, err := external.CreateFromTemplate(ctx, &external.CreateFromTemplateInput{
		Client: c,
		TemplateRef: &corev1.ObjectReference{
			APIVersion: template.GetAPIVersion(), Kind: template.GetKind(), Namespace: template.GetNamespace(), Name: template.GetName(),
		},
		Namespace:   cp.Namespace,
		Name:        name,
		ClusterName: s.cluster.Name,
		OwnerRef:    &owner,
		Labels:      labels,
		Annotations: annotations,
	})
	if err != nil {
		return nil, fmt.Errorf("clone the machine template's infrastructure: %w", err)
	}

	// The ChalkosConfig and the Machine carry the spec hash they are made from.
	cfgAnnotations := maps.Clone(annotations)
	if cfgAnnotations == nil {
		cfgAnnotations = map[string]string{}
	}
	cfgAnnotations[controlplanev1.SpecHashAnnotation] = s.hash
	cfg := &bootstrapv1.ChalkosConfig{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cp.Namespace, Name: name, Labels: labels, Annotations: cfgAnnotations,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
		Spec: bootstrapv1.ChalkosConfigSpec{Image: cp.Spec.Image, ImagePullSecretRef: cp.Spec.ImagePullSecretRef.DeepCopy()},
	}
	if err := r.Client.Create(ctx, cfg); err != nil {
		return nil, r.cleanUp(ctx, fmt.Errorf("create the ChalkosConfig: %w", err), infra)
	}

	// Only the Machine carries the pre-terminate hook.
	machineAnnotations := maps.Clone(cfgAnnotations)
	machineAnnotations[machineutil.HookAnnotation] = machineutil.ControlPlaneHookOwner
	deletion := cp.Spec.MachineTemplate.Spec.Deletion
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cp.Namespace, Name: name, Labels: labels, Annotations: machineAnnotations,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cp, controlPlaneKind)},
		},
		Spec: clusterv1.MachineSpec{
			ClusterName: s.cluster.Name,
			Version:     cp.Spec.Version,
			Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{
				APIGroup: bootstrapv1.GroupVersion.Group, Kind: "ChalkosConfig", Name: name,
			}},
			InfrastructureRef: infraRef,
			Deletion: clusterv1.MachineDeletionSpec{
				NodeDrainTimeoutSeconds:        deletion.NodeDrainTimeoutSeconds,
				NodeVolumeDetachTimeoutSeconds: deletion.NodeVolumeDetachTimeoutSeconds,
				NodeDeletionTimeoutSeconds:     deletion.NodeDeletionTimeoutSeconds,
			},
		},
	}
	if err := r.Client.Create(ctx, m); err != nil {
		return nil, r.cleanUp(ctx, fmt.Errorf("create the Machine: %w", err), infra, cfg)
	}
	return m, nil
}

// cleanUp deletes the objects created for a Machine that could not be created, and returns err
// with the errors of their deletion.
func (r *ChalkosControlPlaneReconciler) cleanUp(ctx context.Context, err error, objs ...client.Object) error {
	errs := []error{err}
	for _, o := range objs {
		if err := r.Client.Delete(ctx, o); err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete %s %s: %w", o.GetObjectKind().GroupVersionKind().Kind, o.GetName(), err))
		}
	}
	return kerrors.NewAggregate(errs)
}

// countWorkers counts the cluster's Machines that are not control-plane Machines, and its
// MachinePools. They are listed uncached: the controller watches neither. A management cluster
// without the MachinePool CRD has no MachinePools.
func (r *ChalkosControlPlaneReconciler) countWorkers(ctx context.Context, s *scope) (machines, pools int, err error) {
	inCluster := []client.ListOption{client.InNamespace(s.cp.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: s.cluster.Name}}
	var ms clusterv1.MachineList
	if err := r.APIReader.List(ctx, &ms, inCluster...); err != nil {
		return 0, 0, fmt.Errorf("list the cluster's Machines: %w", err)
	}
	for i := range ms.Items {
		if !util.IsControlPlaneMachine(&ms.Items[i]) {
			machines++
		}
	}
	var mps clusterv1.MachinePoolList
	if err := r.APIReader.List(ctx, &mps, inCluster...); err != nil && !meta.IsNoMatchError(err) {
		return 0, 0, fmt.Errorf("list the cluster's MachinePools: %w", err)
	}
	return machines, len(mps.Items), nil
}

// waitingForWorkersMessage is the Deleting condition's message while the deletion waits for the
// worker Machines and MachinePools.
func waitingForWorkersMessage(machines, pools int) string {
	var waiting []string
	if machines > 0 {
		waiting = append(waiting, plural(machines, "worker Machine"))
	}
	if pools > 0 {
		waiting = append(waiting, plural(pools, "MachinePool"))
	}
	return "Waiting for " + strings.Join(waiting, " and ") + " to be deleted"
}

// plural is n and the noun, which takes an s unless n is 1.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
