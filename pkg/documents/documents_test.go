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

package documents_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/trevex/chalkos/pkg/chalkd/layers/kubernetes/kubernetestest"
	"github.com/trevex/chalkos/pkg/config"
	k8s "github.com/trevex/chalkos/pkg/kubernetes"
	"github.com/trevex/chalkos/pkg/schema/schematest"
	"github.com/trevex/chalkos/pkg/sdk"
	"github.com/trevex/chalkos/pkg/sdk/layers"

	"github.com/trevex/cluster-api-provider-chalkos/internal/chalkostest"
	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
)

// file is a RawExtension of the JSON text, as a CRD's preserved field holds it.
func file(text string) runtime.RawExtension { return runtime.RawExtension{Raw: []byte(text)} }

const (
	kubernetesAPI = "kubernetes.chalkos.dev/v1alpha1"
	// images are the images a Cluster document must name besides those its version derives.
	images      = `{"etcd": "registry.k8s.io/etcd:3.7.0-0", "coredns": "registry.k8s.io/coredns/coredns:v1.14.6", "pause": "registry.k8s.io/pause:3.10.2", "flannel": "ghcr.io/flannel-io/flannel:v0.28.9"}`
	clusterFile = `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": {"domain": "lab.local", "images": ` + images + `}}`
	systemDisk  = `{"apiVersion": "storage.chalkos.dev/v1alpha1", "kind": "SystemDisk", "metadata": {"name": "system"}, "spec": {"disk": {"serial": "chalk-target"}}}`
)

func worker() documents.Machine {
	return documents.Machine{
		Name:         "w-1",
		Type:         documents.Worker,
		ProviderID:   "kubevirt://w-1",
		Endpoint:     "https://10.96.100.1:6443",
		PodCIDRs:     []string{"10.244.0.0/16"},
		ServiceCIDRs: []string{"10.96.0.0/12"},
		Version:      "v1.37.1",
	}
}

// single returns the spec of the one document of the kind among docs.
func single(t *testing.T, docs []config.Document, kind string) map[string]any {
	t.Helper()
	d, ok := config.Documents(docs).Single(kubernetesAPI, kind)
	if !ok {
		t.Fatalf("not one %s among %v", kind, docs)
	}
	spec, _ := d.Object["spec"].(map[string]any)
	return spec
}

// jsonOf is v as JSON, for comparisons that ignore Go's types.
func jsonOf(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestGeneratedNodeOfAWorker(t *testing.T) {
	docs, err := documents.For(worker(), []runtime.RawExtension{file(clusterFile)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	node := single(t, docs, "Node")
	if node["type"] != "worker" || node["providerID"] != "kubevirt://w-1" {
		t.Errorf("Node spec %v, want a worker with its provider ID", node)
	}
}

func TestClusterFieldsFromClusterAPI(t *testing.T) {
	m := worker()
	m.PodCIDRs = []string{"fd00:10:244::/56", "10.244.0.0/16"}
	m.ServiceCIDRs = []string{"fd00:10:96::/112", "10.96.0.0/12"}
	docs, err := documents.For(m, []runtime.RawExtension{file(clusterFile)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cluster := single(t, docs, "Cluster")
	want := map[string]string{
		"endpoint":     `"https://10.96.100.1:6443"`,
		"ipFamilies":   `["ipv6","ipv4"]`,
		"podCIDRs":     `{"ipv4":"10.244.0.0/16","ipv6":"fd00:10:244::/56"}`,
		"serviceCIDRs": `{"ipv4":"10.96.0.0/12","ipv6":"fd00:10:96::/112"}`,
		"domain":       `"lab.local"`,
	}
	for field, v := range want {
		if got := jsonOf(t, cluster[field]); got != v {
			t.Errorf("spec.%s = %s, want %s", field, got, v)
		}
	}
	imgs, _ := cluster["images"].(map[string]any)
	if imgs["apiServer"] != "registry.k8s.io/kube-apiserver:v1.37.1" || imgs["proxy"] != "registry.k8s.io/kube-proxy:v1.37.1" || imgs["etcd"] != "registry.k8s.io/etcd:3.7.0-0" {
		t.Errorf("images %v, want the version's and the user's", imgs)
	}
}

func TestUsersComponentImagesKept(t *testing.T) {
	text := strings.Replace(clusterFile, `"etcd"`, `"apiServer": "mirror.lab/kube-apiserver:v1.37.1", "etcd"`, 1)
	docs, err := documents.For(worker(), []runtime.RawExtension{file(text)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	imgs, _ := single(t, docs, "Cluster")["images"].(map[string]any)
	if imgs["apiServer"] != "mirror.lab/kube-apiserver:v1.37.1" || imgs["scheduler"] != "registry.k8s.io/kube-scheduler:v1.37.1" {
		t.Errorf("images %v, want the user's API server and the version's scheduler", imgs)
	}
}

// The cluster files must hold the Cluster document, which names the images no version derives.
func TestClusterDocumentRequired(t *testing.T) {
	_, err := documents.For(worker(), []runtime.RawExtension{file(systemDisk)}, nil)
	if err == nil || !strings.Contains(err.Error(), "no Cluster document; a node needs one, naming at least the etcd, coredns, pause and flannel images") {
		t.Errorf("For = %v, want the missing Cluster document named", err)
	}
}

func TestEndpoint(t *testing.T) {
	for host, want := range map[string]string{
		"10.96.100.1":   "https://10.96.100.1:6443",
		"fd00:10:96::1": "https://[fd00:10:96::1]:6443",
		"api.lab":       "https://api.lab:6443",
	} {
		if got := documents.Endpoint(host, 6443); got != want {
			t.Errorf("Endpoint(%s) = %s, want %s", host, got, want)
		}
	}
}

func TestPoolLabelsKept(t *testing.T) {
	pool := `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Node", "metadata": {"name": "default"}, "spec": {"labels": {"pool": "gpu"}, "taints": [{"key": "gpu", "effect": "NoSchedule"}]}}`
	docs, err := documents.For(worker(), []runtime.RawExtension{file(clusterFile)}, []runtime.RawExtension{file(pool)})
	if err != nil {
		t.Fatal(err)
	}
	node := single(t, docs, "Node")
	if jsonOf(t, node["labels"]) != `{"pool":"gpu"}` || jsonOf(t, node["taints"]) != `[{"effect":"NoSchedule","key":"gpu"}]` || node["type"] != "worker" {
		t.Errorf("Node spec %v, want the pool's labels and taints with the owned fields", node)
	}
}

// A Node among the cluster files holds the control planes' Node fields, which workers do not get.
func TestClusterNodeIsTheControlPlanes(t *testing.T) {
	cpNode := `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Node", "metadata": {"name": "default"}, "spec": {"vip": {"interface": "dummy0"}}}`
	files := []runtime.RawExtension{file(clusterFile), file(cpNode)}
	docs, err := documents.For(worker(), files, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := single(t, docs, "Node")["vip"]; ok {
		t.Error("a worker got the control planes' Node fields")
	}
	m := worker()
	m.Type = documents.ControlPlane
	if docs, err = documents.For(m, files, nil); err != nil {
		t.Fatal(err)
	}
	if node := single(t, docs, "Node"); jsonOf(t, node["vip"]) != `{"interface":"dummy0"}` || node["type"] != "controlplane" {
		t.Errorf("control plane's Node spec %v", node)
	}
}

func TestOwnedFieldsRefused(t *testing.T) {
	node := func(spec string) string {
		return `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Node", "metadata": {"name": "default"}, "spec": ` + spec + `}`
	}
	cluster := func(spec string) string {
		return `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": ` + spec + `}`
	}
	for _, tc := range []struct {
		name, file, field string
		pool              bool
	}{
		{"Node type", node(`{"type": "controlplane"}`), "the ChalkosConfig's spec.files[0]: the Node \"default\" sets spec.type", true},
		{"Node provider ID", node(`{"providerID": "kubevirt://other"}`), "spec.providerID", true},
		{"Cluster endpoint", cluster(`{"endpoint": "https://other:6443"}`), "the ChalkosControlPlane's spec.files[0]: the Cluster \"default\" sets spec.endpoint", false},
		{"Cluster pod ranges", cluster(`{"podCIDRs": {"ipv4": "10.0.0.0/16"}}`), "spec.podCIDRs", false},
		{"Cluster service ranges", cluster(`{"serviceCIDRs": {"ipv6": "fd00:1::/112"}}`), "spec.serviceCIDRs", false},
		{"Cluster families", cluster(`{"ipFamilies": ["ipv6"]}`), "spec.ipFamilies", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			if tc.pool {
				_, err = documents.For(worker(), []runtime.RawExtension{file(clusterFile)}, []runtime.RawExtension{file(tc.file)})
			} else {
				_, err = documents.For(worker(), []runtime.RawExtension{file(tc.file)}, nil)
			}
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Errorf("For = %v, want %s refused", err, tc.field)
			}
		})
	}
}

// Ranges Cluster API leaves unset are the user's.
func TestRangesWithoutClusterAPIsAreTheUsers(t *testing.T) {
	m := worker()
	m.PodCIDRs, m.ServiceCIDRs = nil, nil
	text := strings.Replace(clusterFile, `"domain"`, `"podCIDRs": {"ipv4": "10.50.0.0/16"}, "domain"`, 1)
	docs, err := documents.For(m, []runtime.RawExtension{file(text)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cluster := single(t, docs, "Cluster")
	if jsonOf(t, cluster["podCIDRs"]) != `{"ipv4":"10.50.0.0/16"}` {
		t.Errorf("podCIDRs %v, want the user's", cluster["podCIDRs"])
	}
	if _, ok := cluster["ipFamilies"]; ok {
		t.Error("ipFamilies set without Cluster API's ranges")
	}
}

func TestInvalidInput(t *testing.T) {
	clusters := []runtime.RawExtension{file(clusterFile)}
	for _, tc := range []struct {
		name          string
		edit          func(*documents.Machine)
		cluster, pool []runtime.RawExtension
		want          string
	}{
		{"two pod ranges of a family", func(m *documents.Machine) { m.PodCIDRs = []string{"10.0.0.0/16", "10.1.0.0/16"} }, clusters, nil, "two ipv4"},
		{"a range that is none", func(m *documents.Machine) { m.ServiceCIDRs = []string{"bogus"} }, clusters, nil, "bogus"},
		{"a range with host bits", func(m *documents.Machine) { m.PodCIDRs = []string{"10.244.1.0/16"} }, clusters, nil, "write it as 10.244.0.0/16"},
		{"ranges of other families", func(m *documents.Machine) {
			m.ServiceCIDRs = []string{"fd00:10:96::/112", "10.96.0.0/12"}
			m.PodCIDRs = []string{"10.244.0.0/16", "fd00:10:244::/56"}
		}, clusters, nil, "same families in the same order"},
		{"an unknown type", func(m *documents.Machine) { m.Type = "etcd" }, clusters, nil, "etcd"},
		{"no endpoint", func(m *documents.Machine) { m.Endpoint = "" }, clusters, nil, "no API server endpoint"},
		{"two Cluster documents", func(*documents.Machine) {}, []runtime.RawExtension{file(clusterFile), file(strings.Replace(clusterFile, `"default"`, `"other"`, 1))}, nil, "spec.files[0] and the ChalkosControlPlane's spec.files[1] both hold a Cluster document; a node takes one Cluster document"},
		{"a Cluster among the pool's files", func(*documents.Machine) {}, clusters, clusters, "the ChalkosConfig's spec.files[0] holds a Cluster document, which belongs in the ChalkosControlPlane's spec.files"},
		{"a file that is no object", func(*documents.Machine) {}, []runtime.RawExtension{file(`[1, 2]`)}, nil, "the ChalkosControlPlane's spec.files[0]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := worker()
			tc.edit(&m)
			if _, err := documents.For(m, tc.cluster, tc.pool); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("For = %v, want an error naming %q", err, tc.want)
			}
		})
	}
}

// The documents For generates pass the check of chalkos's built-in kinds on an image with
// Kubernetes, as the machine's image checks them before it installs.
func TestGeneratedDocumentsPassChalkosCheck(t *testing.T) {
	chalkostest.Chdir(t)
	facts, err := sdk.ContractFacts(layers.All(), func(name string) ([]byte, error) {
		if name == k8s.ContractFactsFile {
			return []byte(kubernetestest.Image), nil
		}
		return nil, fs.ErrNotExist
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := sdk.SchemaContract(layers.All(), schematest.Files(t, schematest.Builtin, schematest.Networkd), facts)
	if err != nil {
		t.Fatal(err)
	}
	pool := `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Node", "metadata": {"name": "default"}, "spec": {"labels": {"pool": "gpu"}}}`
	for _, typ := range []string{documents.Worker, documents.ControlPlane} {
		m := worker()
		m.Type = typ
		docs, err := documents.For(m, []runtime.RawExtension{file(clusterFile), file(systemDisk)}, []runtime.RawExtension{file(pool)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sdk.Check(context.Background(), c, m.Name, docs); err != nil {
			t.Fatalf("%s: chalkos refuses the documents: %v", typ, err)
		}
	}
}
