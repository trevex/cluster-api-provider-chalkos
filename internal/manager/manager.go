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

// Package manager runs a provider's controller manager: the flags, the scheme, the metrics and
// health endpoints and the leader election both providers share.
package manager

import (
	"context"
	"flag"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
)

// Options are the flags both managers take.
type Options struct {
	// Namespace is the namespace whose objects the manager reconciles; empty is every namespace.
	Namespace string
	// MetricsAddr is the metrics endpoint's address; "0" disables it.
	MetricsAddr string
	// ProbeAddr is the health probe endpoint's address.
	ProbeAddr string
	// LeaderElect elects a leader among the manager's replicas.
	LeaderElect bool
	// WatchFilter, when set, limits the controllers to objects whose cluster.x-k8s.io/watch-filter
	// label has this value.
	WatchFilter string

	zap zap.Options
}

// BindFlags registers the options' flags, and the logger's, on fs.
func (o *Options) BindFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.Namespace, "namespace", "", "The namespace whose objects the manager reconciles; empty reconciles every namespace.")
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", ":8443", "The address the metrics endpoint binds to, over HTTPS for authenticated and authorized clients; 0 disables it.")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	fs.BoolVar(&o.LeaderElect, "leader-elect", false, "Elect a leader among the manager's replicas, so one reconciles at a time.")
	fs.StringVar(&o.WatchFilter, "watch-filter", "", "When set, reconcile only objects whose cluster.x-k8s.io/watch-filter label has this value.")
	o.zap.BindFlags(fs)
}

// Run creates a manager with the options and the leader election lease leaderElectionID, sets
// up its controllers with setup, and runs it until the process is signalled to stop. It logs
// what failed before it returns the error.
func Run(o Options, leaderElectionID string, setup func(context.Context, ctrl.Manager) error) error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&o.zap)))
	log := ctrl.Log.WithName("setup")
	err := run(o, leaderElectionID, setup)
	if err != nil {
		log.Error(err, "run the manager")
	}
	return err
}

func run(o Options, leaderElectionID string, setup func(context.Context, ctrl.Manager) error) error {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, clusterv1.AddToScheme, bootstrapv1.AddToScheme, controlplanev1.AddToScheme} {
		if err := add(scheme); err != nil {
			return fmt.Errorf("register the API types: %w", err)
		}
	}
	var cacheOpts cache.Options
	if o.Namespace != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{o.Namespace: {}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache:  cacheOpts,
		Metrics: metricsserver.Options{
			BindAddress:    o.MetricsAddr,
			SecureServing:  true,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		HealthProbeBindAddress: o.ProbeAddr,
		LeaderElection:         o.LeaderElect,
		LeaderElectionID:       leaderElectionID,
		// A stopping manager gives up its lease, so the next leader takes over at once.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		return fmt.Errorf("create the manager: %w", err)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := setup(ctx, mgr); err != nil {
		return fmt.Errorf("set up the controllers: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("add the health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("add the readiness check: %w", err)
	}
	ctrl.Log.WithName("setup").Info("starting the manager")
	return mgr.Start(ctx)
}
