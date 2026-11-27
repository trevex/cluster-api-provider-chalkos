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

// Command controlplane is the chalkos control-plane provider's manager: it creates the control-plane
// Machines of each ChalkosControlPlane one at a time and bootstraps the cluster on the first.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/trevex/cluster-api-provider-chalkos/controlplane/internal/controllers"
	"github.com/trevex/cluster-api-provider-chalkos/internal/manager"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// The metrics endpoint authenticates and authorizes its clients through the API server.
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create

func main() {
	var (
		opts             manager.Options
		concurrency      int
		bootstrapTimeout time.Duration
	)
	opts.BindFlags(flag.CommandLine)
	flag.IntVar(&concurrency, "concurrency", 10, "How many ChalkosControlPlanes are reconciled at once.")
	flag.DurationVar(&bootstrapTimeout, "bootstrap-timeout", controllers.DefaultBootstrapTimeout, "How long one bootstrap call may take.")
	flag.Parse()
	err := manager.Run(opts, "control-plane-chalkos", func(ctx context.Context, mgr ctrl.Manager) error {
		r := &controllers.ChalkosControlPlaneReconciler{
			Client:                  mgr.GetClient(),
			APIReader:               mgr.GetAPIReader(),
			Node:                    node.New(),
			BootstrapTimeout:        bootstrapTimeout,
			MaxConcurrentReconciles: concurrency,
			WatchFilterValue:        opts.WatchFilter,
		}
		return r.SetupWithManager(ctx, mgr)
	})
	if err != nil {
		os.Exit(1)
	}
}
