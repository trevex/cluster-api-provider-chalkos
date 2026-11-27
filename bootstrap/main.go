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

// Command bootstrap is the chalkos bootstrap provider's manager: it installs each Machine of a
// ChalkosConfig over chalkd's API.
package main

import (
	"flag"
	"os"
	"time"

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
	"github.com/trevex/cluster-api-provider-chalkos/bootstrap/internal/controllers"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
)

// The metrics endpoint authenticates and authorizes its clients through the API server.
// +kubebuilder:rbac:groups=authentication.k8s.io,resources=tokenreviews,verbs=create
// +kubebuilder:rbac:groups=authorization.k8s.io,resources=subjectaccessreviews,verbs=create

func main() {
	var (
		namespace      string
		metricsAddr    string
		probeAddr      string
		leaderElect    bool
		concurrency    int
		installTimeout time.Duration
		watchFilter    string
	)
	flag.StringVar(&namespace, "namespace", "", "The namespace whose objects the manager reconciles; empty reconciles every namespace.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8443", "The address the metrics endpoint binds to, over HTTPS for authenticated and authorized clients; 0 disables it.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Elect a leader among the manager's replicas, so one reconciles at a time.")
	flag.IntVar(&concurrency, "concurrency", 10, "How many machines install at once; twice as many ChalkosConfigs are reconciled at once.")
	flag.DurationVar(&installTimeout, "install-timeout", controllers.DefaultInstallTimeout, "How long one install may take.")
	flag.StringVar(&watchFilter, "watch-filter", "", "When set, reconcile only objects whose cluster.x-k8s.io/watch-filter label has this value.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, clusterv1.AddToScheme, bootstrapv1.AddToScheme, controlplanev1.AddToScheme} {
		if err := add(scheme); err != nil {
			log.Error(err, "register the API types")
			os.Exit(1)
		}
	}
	var cacheOpts cache.Options
	if namespace != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{namespace: {}}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Cache:  cacheOpts,
		Metrics: metricsserver.Options{
			BindAddress:    metricsAddr,
			SecureServing:  true,
			FilterProvider: filters.WithAuthenticationAndAuthorization,
		},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "bootstrap-chalkos",
		// A stopping manager gives up its lease, so the next leader takes over at once.
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		log.Error(err, "create the manager")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	r := &controllers.ChalkosConfigReconciler{
		Client:           mgr.GetClient(),
		APIReader:        mgr.GetAPIReader(),
		Node:             node.New(),
		InstallTimeout:   installTimeout,
		MaxInstalls:      concurrency,
		WatchFilterValue: watchFilter,
	}
	if err := r.SetupWithManager(ctx, mgr); err != nil {
		log.Error(err, "set up the ChalkosConfig controller")
		os.Exit(1)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "add the health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "add the readiness check")
		os.Exit(1)
	}
	log.Info("starting the manager")
	if err := mgr.Start(ctx); err != nil {
		log.Error(err, "run the manager")
		os.Exit(1)
	}
}
