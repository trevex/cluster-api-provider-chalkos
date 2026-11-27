//go:build e2e || e2e_ha

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

// Package e2e runs the lab's scenario against the KubeVirt lab: it creates the workload cluster
// lab of one control plane and one worker, checks that its nodes join and that the providers
// report them installed, scales the workers to two and back to one, checks that the removed
// worker's node is revoked and its Node gone, and deletes the cluster again. The lab must run
// already, with the providers built from the checkout or the released ones, as after
//
//	just -f lab/justfile create kubevirt providers-local
//	just -f lab/justfile create kubevirt providers
//
// and the test skips otherwise. A run that stops early deletes the cluster too, unless E2E_KEEP=1.
package e2e

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"

	bootstrapv1 "github.com/trevex/cluster-api-provider-chalkos/bootstrap/api/v1alpha1"
	controlplanev1 "github.com/trevex/cluster-api-provider-chalkos/controlplane/api/v1alpha1"
	"github.com/trevex/cluster-api-provider-chalkos/internal/machineutil"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/node"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/secrets"
)

const (
	// namespace and clusterName name the lab's workload cluster, as lab/cluster.yaml.tmpl does.
	namespace   = "default"
	clusterName = "lab"
	// defaultKubeconfigTTL is the kubeconfig's validity when the control plane names none.
	defaultKubeconfigTTL = 24 * time.Hour
	// poll is how often a wait looks again.
	poll = 10 * time.Second
	// kindNode is the lab's kind node, whose NodePorts the host reaches on the kind network.
	kindNode = "chalkos-lab-control-plane"
	// chalkdPort is the port chalkd listens on.
	chalkdPort int32 = 50000
)

// providers are the deployments the lab runs, by namespace and name, which must be Available.
var providers = [][2]string{
	{"capi-system", "capi-controller-manager"},
	{"capk-system", "capk-controller-manager"},
	{"bootstrap-chalkos-system", "bootstrap-chalkos-controller-manager"},
	{"control-plane-chalkos-system", "control-plane-chalkos-controller-manager"},
}

// The kinds of the infrastructure provider and of KubeVirt, read unstructured.
var (
	kubevirtMachineList = schema.GroupVersionKind{Group: "infrastructure.cluster.x-k8s.io", Version: "v1alpha1", Kind: "KubevirtMachineList"}
	virtualMachineList  = schema.GroupVersionKind{Group: "kubevirt.io", Version: "v1", Kind: "VirtualMachineList"}
)

// lab is the lab's management cluster as the test sees it.
type lab struct {
	t *testing.T
	// root is the repository's root directory.
	root string
	// kubeconfig is the kind cluster's kubeconfig.
	kubeconfig string
	c          client.Client
}

// deleteCluster deletes the Cluster lab, unless E2E_KEEP=1 keeps it to look at, and waits up to
// ten minutes until it is gone. It runs as a cleanup, so that no run leaves its VMs behind.
func (l *lab) deleteCluster() {
	if os.Getenv("E2E_KEEP") == "1" {
		l.t.Logf("E2E_KEEP=1: keeping the Cluster %s/%s", namespace, clusterName)
		return
	}
	ctx := context.Background()
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: clusterName}}
	if err := l.c.Delete(ctx, cluster); err != nil {
		if !apierrors.IsNotFound(err) {
			l.t.Errorf("deleting the Cluster: %v", err)
		}
		return
	}
	l.waitFor(ctx, "the Cluster to be gone", 10*time.Minute, func(ctx context.Context) (bool, string, error) {
		err := l.c.Get(ctx, client.ObjectKeyFromObject(cluster), &clusterv1.Cluster{})
		if apierrors.IsNotFound(err) {
			return true, "", nil
		}
		return false, "the Cluster exists", err
	})
}

// newLab connects to the lab's management cluster and skips the test unless the lab runs with all
// its providers available.
func newLab(t *testing.T) *lab {
	// The clients log nothing the test needs.
	ctrllog.SetLogger(logr.Discard())
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig := filepath.Join(root, ".lab", "kind.kubeconfig")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Skipf("the lab does not run (%v); run: just -f lab/justfile create kubevirt providers-local", err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, clusterv1.AddToScheme, bootstrapv1.AddToScheme, controlplanev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range providers {
		var d appsv1.Deployment
		err := c.Get(t.Context(), client.ObjectKey{Namespace: p[0], Name: p[1]}, &d)
		if err != nil || !deploymentAvailable(&d) {
			t.Skipf("the lab's provider %s/%s is not available (%v); run: just -f lab/justfile create kubevirt providers-local", p[0], p[1], err)
		}
	}
	return &lab{t: t, root: root, kubeconfig: kubeconfig, c: c}
}

// deploymentAvailable tells whether the Deployment's Available condition is true.
func deploymentAvailable(d *appsv1.Deployment) bool {
	for _, c := range d.Status.Conditions {
		if c.Type == appsv1.DeploymentAvailable {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// nodesReady tells whether the workload cluster has one Ready node per Machine, of the Machine's
// name and with its providerID.
func (l *lab) nodesReady(ctx context.Context, machines []clusterv1.Machine) (bool, string, error) {
	c, state, err := l.workloadClient(ctx)
	if c == nil {
		return false, state, err
	}
	var nodes corev1.NodeList
	if err := c.List(ctx, &nodes); err != nil {
		return false, fmt.Sprintf("listing the nodes: %v", err), nil
	}
	byName := map[string]corev1.Node{}
	for _, n := range nodes.Items {
		byName[n.Name] = n
	}
	var waiting []string
	for _, m := range machines {
		n, ok := byName[m.Name]
		switch {
		case !ok:
			waiting = append(waiting, m.Name+" has no node")
		case n.Spec.ProviderID == "":
			// The kubelet or the cloud provider sets the providerID a moment after the node registers.
			waiting = append(waiting, m.Name+" has no providerID yet")
		case n.Spec.ProviderID != "kubevirt://"+m.Name:
			return false, "", fmt.Errorf("the node %s has the providerID %q, want %q", n.Name, n.Spec.ProviderID, "kubevirt://"+m.Name)
		case !nodeReady(&n):
			waiting = append(waiting, m.Name+" is not Ready")
		}
	}
	if len(nodes.Items) != len(machines) {
		waiting = append(waiting, fmt.Sprintf("%d nodes for %d Machines", len(nodes.Items), len(machines)))
	}
	return len(waiting) == 0, strings.Join(waiting, "; "), nil
}

// nodeReady tells whether the node's Ready condition is true.
func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// checkKubeconfigCert checks that the client certificate of the kubeconfig Secret is valid now,
// expires within the control plane's kubeconfig TTL, and is the one its status reports.
func (l *lab) checkKubeconfigCert(cp *controlplanev1.ChalkosControlPlane) {
	t := l.t
	var s corev1.Secret
	if err := l.c.Get(t.Context(), client.ObjectKey{Namespace: namespace, Name: clusterName + "-kubeconfig"}, &s); err != nil {
		t.Fatal(err)
	}
	cfg, err := clientcmd.Load(s.Data["value"])
	if err != nil {
		t.Fatalf("the kubeconfig Secret: %v", err)
	}
	if len(cfg.AuthInfos) != 1 {
		t.Fatalf("the kubeconfig has %d users, want 1", len(cfg.AuthInfos))
	}
	var data []byte
	for _, u := range cfg.AuthInfos {
		data = u.ClientCertificateData
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatal("the kubeconfig's user has no client certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	ttl := defaultKubeconfigTTL
	if secs := cp.Spec.Kubeconfig.TTLSeconds; secs != nil {
		ttl = time.Duration(*secs) * time.Second
	}
	if now := time.Now(); !cert.NotAfter.After(now) || cert.NotAfter.After(now.Add(ttl)) {
		t.Errorf("the kubeconfig's certificate expires at %s, want within %s of now", cert.NotAfter, ttl)
	}
	if got := cp.Status.KubeconfigExpiresAt; got == nil || !got.Time.Equal(cert.NotAfter) {
		t.Errorf("status.kubeconfigExpiresAt is %v, the certificate expires at %s", got, cert.NotAfter)
	}
}

// waitFor calls check every poll until it is done, fails or timeout passes, and logs what check
// reports whenever that changes. check gets a context that ends with the timeout.
func (l *lab) waitFor(ctx context.Context, what string, timeout time.Duration, check func(context.Context) (done bool, state string, err error)) {
	l.t.Helper()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var last string
	for {
		done, state, err := check(ctx)
		if err != nil && ctx.Err() == nil {
			l.t.Fatalf("waiting for %s: %v", what, err)
		}
		if done {
			return
		}
		if state != last && err == nil {
			l.t.Logf("waiting for %s: %s", what, state)
			last = state
		}
		select {
		case <-ctx.Done():
			l.t.Fatalf("%s did not happen within %s; last: %s", what, timeout, last)
		case <-time.After(poll):
		}
	}
}

// just runs the lab's recipe and fails the test when it fails.
func (l *lab) just(recipe string) {
	l.t.Helper()
	if out, err := l.run(l.t.Context(), "just", "-f", filepath.Join(l.root, "lab", "justfile"), recipe); err != nil {
		l.t.Fatalf("just %s: %v\n%s", recipe, err, out)
	}
}

// run runs the command against the lab's management cluster and returns its combined output.
func (l *lab) run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = l.root
	cmd.Env = append(os.Environ(), "KUBECONFIG="+l.kubeconfig)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// diagnose logs the cluster's state and the providers' recent logs.
func (l *lab) diagnose(ctx context.Context) {
	const conds = `{{range .items}}{{.kind}}/{{.metadata.name}}{{"\n"}}{{range .status.conditions}}  {{.type}}={{.status}} {{.reason}}: {{.message}}{{"\n"}}{{end}}{{end}}`
	cmds := [][]string{
		{"clusterctl", "describe", "cluster", clusterName, "--show-conditions", "all"},
		{"kubectl", "-n", namespace, "get", "chalkoscontrolplanes,chalkosconfigs,machines,kubevirtmachines,virtualmachines", "-o", "go-template=" + conds},
		{"kubectl", "-n", namespace, "get", "chalkosconfigs", "-o", `go-template={{range .items}}{{.metadata.name}}: installed={{.status.installed}} rootHash={{.status.imageRootHash}} fingerprints={{len .status.maintenanceFingerprints}}{{"\n"}}{{end}}`},
		{"kubectl", "-n", "bootstrap-chalkos-system", "logs", "deployment/bootstrap-chalkos-controller-manager", "--tail", "200"},
		{"kubectl", "-n", "control-plane-chalkos-system", "logs", "deployment/control-plane-chalkos-controller-manager", "--tail", "200"},
	}
	for _, c := range cmds {
		out, err := l.run(ctx, c[0], c[1:]...)
		if err != nil {
			out += "\n" + err.Error()
		}
		l.t.Logf("$ %s\n%s", strings.Join(c, " "), out)
	}
}

// unstructuredList is an empty list of the kind.
func unstructuredList(gvk schema.GroupVersionKind) *unstructured.UnstructuredList {
	l := &unstructured.UnstructuredList{}
	l.SetGroupVersionKind(gvk)
	return l
}

// conditionsSummary is the status and reason of each condition of the types, or that it is not
// set.
func conditionsSummary(conds []metav1.Condition, types ...string) string {
	var parts []string
	for _, t := range types {
		c := meta.FindStatusCondition(conds, t)
		if c == nil {
			parts = append(parts, t+" unset")
			continue
		}
		s := fmt.Sprintf("%s=%s %s", t, c.Status, c.Reason)
		if c.Message != "" {
			s += ": " + c.Message
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, "; ")
}

// since is the time since start, in whole seconds.
func since(start time.Time) time.Duration {
	return time.Since(start).Round(time.Second)
}

// workloadClient is a client of the workload cluster, reached with the kubeconfig that the lab's
// kubeconfig recipe writes. It reports a cluster that does not answer yet in state, not as an
// error.
func (l *lab) workloadClient(ctx context.Context) (c client.Client, state string, err error) {
	if _, err := l.run(ctx, "just", "-f", filepath.Join(l.root, "lab", "justfile"), "kubeconfig"); err != nil {
		return nil, fmt.Sprintf("no kubeconfig yet: %v", err), nil
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", filepath.Join(l.root, ".lab", "lab.kubeconfig"))
	if err != nil {
		return nil, "", err
	}
	cfg.Timeout = 30 * time.Second
	c, err = client.New(cfg, client.Options{})
	return c, "", err
}

// machinesOf lists the cluster's Machines of the control plane, or those of the workers, oldest
// first; Machines being deleted among them.
func (l *lab) machinesOf(ctx context.Context, controlPlane bool) ([]clusterv1.Machine, error) {
	var list clusterv1.MachineList
	if err := l.c.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: clusterName}); err != nil {
		return nil, err
	}
	return slices.DeleteFunc(list.Items, func(m clusterv1.Machine) bool { return util.IsControlPlaneMachine(&m) != controlPlane }), nil
}

// scaleWorkers sets the replicas of the MachineDeployment lab-workers.
func (l *lab) scaleWorkers(ctx context.Context, n int32) {
	l.t.Helper()
	md := &clusterv1.MachineDeployment{}
	if err := l.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: "lab-workers"}, md); err != nil {
		l.t.Fatal(err)
	}
	original := md.DeepCopy()
	md.Spec.Replicas = ptr.To(n)
	if err := l.c.Patch(ctx, md, client.MergeFrom(original)); err != nil {
		l.t.Fatal(err)
	}
}

// checkHooks checks that every Machine of the cluster that is not being deleted carries the
// pre-terminate hook of the provider that removes its node.
func (l *lab) checkHooks(ctx context.Context) {
	l.t.Helper()
	for _, controlPlane := range []bool{true, false} {
		ms, err := l.machinesOf(ctx, controlPlane)
		if err != nil {
			l.t.Fatal(err)
		}
		want := machineutil.BootstrapHookOwner
		if controlPlane {
			want = machineutil.ControlPlaneHookOwner
		}
		for _, m := range ms {
			if !m.DeletionTimestamp.IsZero() {
				continue
			}
			if got, ok := m.Annotations[machineutil.HookAnnotation]; !ok || got != want {
				l.t.Errorf("the Machine %s carries the hook %s=%q (set: %v), want %q", m.Name, machineutil.HookAnnotation, got, ok, want)
			}
		}
	}
}

// chalkdAddr is an address on the kind node at which the host reaches chalkd on the VM of the
// Machine: the NodePort of a Service for chalkd's port that selects the Machine's virt-launcher
// pod. CAPK labels each VM's pod with kubevirt.io/vm, the name of its KubevirtMachine, which is
// the Machine's. The Service is deleted when the test ends.
func (l *lab) chalkdAddr(ctx context.Context, machine string) (string, error) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "chalkd-" + machine},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"kubevirt.io/vm": machine},
			Ports:    []corev1.ServicePort{{Port: chalkdPort, TargetPort: intstr.FromInt32(chalkdPort)}},
		},
	}
	switch err := l.c.Create(ctx, svc); {
	case err == nil:
		l.t.Cleanup(func() { _ = l.c.Delete(context.Background(), svc) })
	case !apierrors.IsAlreadyExists(err):
		return "", fmt.Errorf("create the Service %s: %w", svc.Name, err)
	}
	if err := l.c.Get(ctx, client.ObjectKeyFromObject(svc), svc); err != nil {
		return "", err
	}
	out, err := l.run(ctx, "docker", "inspect", "-f", "{{.NetworkSettings.Networks.kind.IPAddress}}", kindNode)
	if err != nil {
		return "", fmt.Errorf("the kind node's address: %w: %s", err, out)
	}
	return net.JoinHostPort(strings.TrimSpace(out), strconv.Itoa(int(svc.Spec.Ports[0].NodePort))), nil
}

// revokedBy tells whether the control plane of the Machine cp lists the node as revoked in its
// Kubernetes status, which it reads with an admin certificate from the cluster's secrets.
func (l *lab) revokedBy(ctx context.Context, cp, nodeName string) (bool, error) {
	sec, ok, err := secrets.Get(ctx, l.c, client.ObjectKey{Namespace: namespace, Name: clusterName})
	switch {
	case err != nil:
		return false, fmt.Errorf("the cluster's secrets: %w", err)
	case !ok:
		return false, fmt.Errorf("the cluster's secrets %s do not exist", secrets.Name(clusterName))
	}
	addr, err := l.chalkdAddr(ctx, cp)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	st, err := node.New().KubernetesStatus(ctx, addr, sec, cp)
	switch {
	case err != nil:
		return false, err
	case st.GetRevokedNodesError() != "":
		return false, fmt.Errorf("the revoked nodes: %s", st.GetRevokedNodesError())
	}
	return slices.ContainsFunc(st.GetRevokedNodes(), func(n *kubernetesv1.RevokedNode) bool { return n.GetName() == nodeName }), nil
}

// checkRemoved waits until the Node of the removed Machine name is gone from the workload cluster
// and the control plane of the Machine cp lists it as revoked.
func (l *lab) checkRemoved(ctx context.Context, name, cp string) {
	l.t.Helper()
	l.waitFor(ctx, "the Node "+name+" to be gone and revoked", 5*time.Minute, func(ctx context.Context) (bool, string, error) {
		c, state, err := l.workloadClient(ctx)
		if c == nil {
			return false, state, err
		}
		switch err := c.Get(ctx, client.ObjectKey{Name: name}, &corev1.Node{}); {
		case err == nil:
			return false, "the Node exists", nil
		case !apierrors.IsNotFound(err):
			return false, fmt.Sprintf("getting the Node: %v", err), nil
		}
		revoked, err := l.revokedBy(ctx, cp, name)
		if err != nil {
			return false, fmt.Sprintf("the Kubernetes status of %s: %v", cp, err), nil
		}
		return revoked, fmt.Sprintf("the Node is gone; %s lists it as revoked: %v", cp, revoked), nil
	})
}
