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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
)

// reconcileHook runs the pre-terminate hook of a worker Machine. It adds the hook to a Machine
// that is not being deleted, and removes it from every Machine once the Cluster is being deleted;
// done is set when the reconcile ends here.
func (r *ChalkosConfigReconciler) reconcileHook(ctx context.Context, s *scope) (ctrl.Result, bool, error) {
	m := s.machine
	switch {
	case !s.cluster.DeletionTimestamp.IsZero():
		// The whole cluster goes: nothing is revoked.
		if err := machineutil.RemoveHook(ctx, r.Client, m); err != nil {
			return ctrl.Result{}, true, err
		}
		if !m.DeletionTimestamp.IsZero() {
			s.nodeRemoved(metav1.ConditionTrue, bootstrapv1.DoneReason, "The Cluster is being deleted; the node is not revoked")
		}
		return ctrl.Result{}, true, nil
	case m.DeletionTimestamp.IsZero():
		added, err := machineutil.AddHook(ctx, r.Client, m, machineutil.BootstrapHookOwner)
		if added {
			ctrl.LoggerFrom(ctx).Info("Added the pre-terminate hook")
		}
		return ctrl.Result{}, err != nil, err
	}
	return r.removeWorker(ctx, s)
}

// removeWorker runs the hook of a worker Machine being deleted. It does nothing yet, so the
// reconcile goes on as for any Machine being deleted.
func (r *ChalkosConfigReconciler) removeWorker(_ context.Context, _ *scope) (ctrl.Result, bool, error) {
	return ctrl.Result{}, false, nil
}
