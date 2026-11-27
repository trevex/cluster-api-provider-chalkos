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

	ctrl "sigs.k8s.io/controller-runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
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
