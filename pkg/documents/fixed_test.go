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
	"bytes"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	"github.com/trevex/chalkos/pkg/config"

	"github.com/trevex/cluster-api-provider-chalkos/pkg/documents"
)

// capiCluster is a Cluster API Cluster with an endpoint and one pod and one service range.
func capiCluster() *clusterv1.Cluster {
	return &clusterv1.Cluster{Spec: clusterv1.ClusterSpec{
		ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "10.96.100.1", Port: 6443},
		ClusterNetwork: clusterv1.ClusterNetwork{
			Pods:     clusterv1.NetworkRanges{CIDRBlocks: []string{"10.244.0.0/16"}},
			Services: clusterv1.NetworkRanges{CIDRBlocks: []string{"10.96.0.0/12"}},
		},
	}}
}

// clusterOf is a Cluster document of the spec.
func clusterOf(spec string) runtime.RawExtension {
	return file(`{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Cluster", "metadata": {"name": "default"}, "spec": ` + spec + `}`)
}

// The cluster-wide documents are a control plane's without its Node and Secrets, with the
// Cluster's fields filled from the Cluster API Cluster.
func TestClusterWide(t *testing.T) {
	node := `{"apiVersion": "kubernetes.chalkos.dev/v1alpha1", "kind": "Node", "metadata": {"name": "default"}, "spec": {"labels": {"role": "cp"}}}`
	secret := `{"apiVersion": "chalkos.dev/v1alpha1", "kind": "Secret", "metadata": {"name": "bgp"}, "spec": {"data": {"password": "s3cret"}}}`
	docs, err := documents.ClusterWide(capiCluster(), "v1.37.1", []runtime.RawExtension{file(clusterFile), file(node), file(secret), file(systemDisk)})
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 || docs[1].Kind != "SystemDisk" {
		t.Fatalf("documents %v, want the Cluster and the SystemDisk", docs)
	}
	cluster := single(t, docs, "Cluster")
	want := map[string]string{
		"endpoint":     `"https://10.96.100.1:6443"`,
		"ipFamilies":   `["ipv4"]`,
		"podCIDRs":     `{"ipv4":"10.244.0.0/16"}`,
		"serviceCIDRs": `{"ipv4":"10.96.0.0/12"}`,
	}
	for field, v := range want {
		if got := jsonOf(t, cluster[field]); got != v {
			t.Errorf("spec.%s = %s, want %s", field, got, v)
		}
	}
	if imgs, _ := cluster["images"].(map[string]any); imgs["apiServer"] != "registry.k8s.io/kube-apiserver:v1.37.1" {
		t.Errorf("images %v, want the version's API server", imgs)
	}
	if _, err := documents.ClusterWide(capiCluster(), "v1.37.1", []runtime.RawExtension{file(systemDisk)}); err == nil || !strings.Contains(err.Error(), "no Cluster document") {
		t.Errorf("without a Cluster document: %v", err)
	}
	if _, err := documents.ClusterWide(&clusterv1.Cluster{}, "v1.37.1", []runtime.RawExtension{file(clusterFile)}); err == nil || !strings.Contains(err.Error(), "no control-plane endpoint") {
		t.Errorf("without an endpoint: %v", err)
	}
}

// A change of the Cluster's domain is a fixed change, named with both values, and so is one of
// the pod range through the Cluster API Cluster; a change of an image is none, nor is the default
// written out; without observed documents nothing is compared.
func TestFixedChanges(t *testing.T) {
	c := builtinContract(t)
	const before = `{"domain": "lab.local", "images": {"coredns": "a"}}`
	for name, tc := range map[string]struct {
		old, cur string
		edit     func(*clusterv1.Cluster)
		version  string
		want     string
	}{
		"nothing":  {before, before, nil, "", ""},
		"an image": {before, `{"domain": "lab.local", "images": {"coredns": "b"}}`, nil, "", ""},
		"the default written": {`{"images": {"coredns": "a"}}`, `{"domain": "cluster.local", "images": {"coredns": "a"}}`, nil, "", ""},
		"the version": {before, before, nil, "v1.38.0", ""},
		"the domain": {before, `{"domain": "example.org", "images": {"coredns": "a"}}`, nil, "",
			`The cluster-wide documents change Cluster/default spec.domain from "lab.local" to "example.org": every node of the cluster holds these fields alike, and a running cluster keeps them; revert it in spec.files or in the Cluster's clusterNetwork or controlPlaneEndpoint`},
		"the pod range": {before, before, func(cl *clusterv1.Cluster) { cl.Spec.ClusterNetwork.Pods.CIDRBlocks = []string{"10.245.0.0/16"} }, "",
			`The cluster-wide documents change Cluster/default spec.podCIDRs.ipv4 from "10.244.0.0/16" to "10.245.0.0/16": every node of the cluster holds these fields alike, and a running cluster keeps them; revert it in spec.files or in the Cluster's clusterNetwork or controlPlaneEndpoint`},
		"the domain and a service range": {before, `{"domain": "example.org", "images": {"coredns": "a"}}`, func(cl *clusterv1.Cluster) { cl.Spec.ClusterNetwork.Services.CIDRBlocks = []string{"10.97.0.0/16"} }, "",
			`The cluster-wide documents change Cluster/default spec.domain from "lab.local" to "example.org", Cluster/default spec.serviceCIDRs.ipv4 from "10.96.0.0/12" to "10.97.0.0/16": every node of the cluster holds these fields alike, and a running cluster keeps them; revert it in spec.files or in the Cluster's clusterNetwork or controlPlaneEndpoint`},
	} {
		old, err := documents.ClusterWide(capiCluster(), "v1.37.1", []runtime.RawExtension{clusterOf(tc.old), file(systemDisk)})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		observed, err := documents.Observed(c, old)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		cl := capiCluster()
		if tc.edit != nil {
			tc.edit(cl)
		}
		version := "v1.37.1"
		if tc.version != "" {
			version = tc.version
		}
		cur, err := documents.ClusterWide(cl, version, []runtime.RawExtension{clusterOf(tc.cur), file(systemDisk)})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		changes, err := documents.FixedChanges(c, observed, cur)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got := ""
		if len(changes) > 0 {
			got = documents.DescribeFixed(changes)
		}
		if got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	cur, err := documents.ClusterWide(capiCluster(), "v1.37.1", []runtime.RawExtension{clusterOf(before)})
	if err != nil {
		t.Fatal(err)
	}
	if changes, err := documents.FixedChanges(c, "", cur); err != nil || len(changes) != 0 {
		t.Errorf("without observed documents: %v, %v", changes, err)
	}
	if _, err := documents.FixedChanges(c, "{", cur); err == nil || !strings.Contains(err.Error(), "status.observedFiles") {
		t.Errorf("unparsable observed documents: %v", err)
	}
}

// Observed writes out the defaults of the image the Machines were made from, so an image that
// defaults a fixed field differently changes it though the user leaves the field out; the
// documents recorded without the defaults would hide that change.
func TestObservedDefaults(t *testing.T) {
	recording := builtinContract(t)
	comparing := contractOf(t, func(schemas map[string][]byte) {
		const name = "kubernetes.chalkos.dev_v1alpha1_Cluster.json"
		edited := bytes.Replace(schemas[name], []byte(`"default": "cluster.local"`), []byte(`"default": "example.org"`), 1)
		if bytes.Equal(edited, schemas[name]) {
			t.Fatal("the Cluster schema has no default of cluster.local to change")
		}
		schemas[name] = edited
	})
	docs, err := documents.ClusterWide(capiCluster(), "v1.37.1", []runtime.RawExtension{clusterOf(`{"images": {"coredns": "a"}}`)})
	if err != nil {
		t.Fatal(err)
	}
	observed, err := documents.Observed(recording, docs)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(observed, `"domain":"cluster.local"`) {
		t.Errorf("observed %s, want the default domain written out", observed)
	}
	changes, err := documents.FixedChanges(comparing, observed, docs)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Path != "spec.domain" || changes[0].Old != "cluster.local" || changes[0].New != "example.org" {
		t.Errorf("changes %+v, want spec.domain from cluster.local to example.org", changes)
	}
	raw, err := config.Encode(docs)
	if err != nil {
		t.Fatal(err)
	}
	if changes, err := documents.FixedChanges(comparing, raw.Text, docs); err != nil || len(changes) != 0 {
		t.Errorf("without the defaults recorded: %+v, %v, want no change found", changes, err)
	}
}
