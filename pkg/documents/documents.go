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

// Package documents builds the chalkos config documents a machine installs with: the user's
// files, with the fields Cluster API owns of the Kubernetes Node and Cluster documents set from
// the Machine and the Cluster.
package documents

import (
	"bytes"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/trevex/chalkos/pkg/config"
	"github.com/trevex/chalkos/pkg/constants"
)

// The node types a Machine has, as the Node document's spec.type names them.
const (
	ControlPlane = "controlplane"
	Worker       = "worker"
)

// defaultName is the name of a Node document the controllers generate, as chalkos's own
// examples name it.
const defaultName = "default"

// Machine is what the controllers generate documents for: one Machine of a Cluster.
type Machine struct {
	// Name is the Machine's name, which is the node's name.
	Name string
	// Type is the node's type, ControlPlane or Worker.
	Type string
	// ProviderID is the provider ID the node registers with; empty registers it without one.
	ProviderID string
	// Endpoint is the URL of the cluster's API server, as Endpoint makes it from the Cluster's
	// spec.controlPlaneEndpoint.
	Endpoint string
	// PodCIDRs are the pod ranges of the Cluster's spec.clusterNetwork.pods.cidrBlocks, at most
	// one per family, the primary family first.
	PodCIDRs []string
	// ServiceCIDRs are the service ranges of spec.clusterNetwork.services.cidrBlocks, of the
	// same families in the same order when both are given.
	ServiceCIDRs []string
	// Version is the Kubernetes version, such as v1.37.1, that the images of the API server, the
	// controller-manager, the scheduler and kube-proxy derive from when the user names none.
	Version string
}

// Endpoint is the URL of the API server at host and port, https://host:port, with an IPv6
// address in brackets.
func Endpoint(host string, port int32) string {
	return "https://" + net.JoinHostPort(host, strconv.Itoa(int(port)))
}

// versionedImages are the Cluster document's images that registry.k8s.io publishes under the
// Kubernetes version, by their field.
var versionedImages = map[string]string{
	"apiServer":         "registry.k8s.io/kube-apiserver",
	"controllerManager": "registry.k8s.io/kube-controller-manager",
	"scheduler":         "registry.k8s.io/kube-scheduler",
	"proxy":             "registry.k8s.io/kube-proxy",
}

// The sources of the files, as errors name them.
const (
	clusterSource = "the ChalkosControlPlane's spec.files"
	poolSource    = "the ChalkosConfig's spec.files"
)

// file is a user's document and where it came from, for errors.
type file struct {
	doc    config.Document
	source string
}

// For returns the documents machine m installs with: the cluster's files, the ChalkosControlPlane's,
// then its pool's, the ChalkosConfig's, with the fields Cluster API owns merged into the Node and
// Cluster documents.
//
// The cluster files must hold the Cluster document, which names the etcd, coredns, pause and
// flannel images; the pool files hold none. A Node document among the cluster files holds the
// control planes' Node fields, which workers do not get; the pool files may hold the pool's. When
// the files hold no Node document, For generates one with the owned fields alone.
//
// For refuses a file that sets an owned field, naming the field: the Node's spec.type and
// spec.providerID, the Cluster's spec.endpoint and, when the Cluster gives ranges, its
// spec.ipFamilies and the spec.podCIDRs and spec.serviceCIDRs it gives, each as a whole, both
// families. The images of the API server, the controller-manager, the scheduler and kube-proxy
// that the user names none of derive from m.Version as registry.k8s.io's, by tag alone, without a
// digest.
func For(m Machine, clusterFiles, poolFiles []runtime.RawExtension) ([]config.Document, error) {
	switch {
	case m.Type != ControlPlane && m.Type != Worker:
		return nil, fmt.Errorf("the machine %s has the type %q, neither %s nor %s", m.Name, m.Type, ControlPlane, Worker)
	case m.Endpoint == "":
		return nil, fmt.Errorf("the machine %s has no API server endpoint", m.Name)
	}
	files, err := parse(clusterSource, clusterFiles)
	if err != nil {
		return nil, err
	}
	cluster, err := single(files, "Cluster")
	if err != nil {
		return nil, err
	}
	if cluster == nil {
		return nil, fmt.Errorf("%s hold no Cluster document; a node needs one, naming at least the etcd, coredns, pause and flannel images", clusterSource)
	}
	if err := setCluster(cluster, m); err != nil {
		return nil, err
	}
	if m.Type == Worker {
		files = slices.DeleteFunc(files, func(f file) bool { return f.is("Node") })
	}
	pool, err := parse(poolSource, poolFiles)
	if err != nil {
		return nil, err
	}
	for _, f := range pool {
		if f.is("Cluster") {
			return nil, fmt.Errorf("%s holds a Cluster document, which belongs in %s", f.source, clusterSource)
		}
	}
	files = append(files, pool...)
	node, err := single(files, "Node")
	if err != nil {
		return nil, err
	}
	if node == nil {
		files = append(files, file{
			doc: config.Document{Object: map[string]any{
				"apiVersion": kubernetesAPI,
				"kind":       "Node",
				"metadata":   map[string]any{"name": defaultName},
				"spec":       map[string]any{},
			}},
			source: "the generated Node",
		})
		node = &files[len(files)-1]
	}
	if err := setNode(node, m); err != nil {
		return nil, err
	}
	docs := make([]config.Document, len(files))
	for i, f := range files {
		docs[i] = config.FromObject(f.doc.Object)
	}
	return docs, nil
}

// parse reads the files, one document each; source names them in errors.
func parse(source string, raws []runtime.RawExtension) ([]file, error) {
	files := make([]file, 0, len(raws))
	for i, raw := range raws {
		name := fmt.Sprintf("%s[%d]", source, i)
		docs, err := config.Parse(bytes.NewReader(raw.Raw))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if len(docs) != 1 {
			return nil, fmt.Errorf("%s holds %d documents, not one", name, len(docs))
		}
		files = append(files, file{doc: docs[0], source: name})
	}
	return files, nil
}

// kubernetesAPI is the apiVersion of chalkos's Kubernetes kinds.
var kubernetesAPI = constants.APIVersion("kubernetes")

// is tells whether f is a document of chalkos's Kubernetes kind.
func (f file) is(kind string) bool {
	return f.doc.APIVersion == kubernetesAPI && f.doc.Kind == kind
}

// single returns the one file of chalkos's Kubernetes kind among files, nil when there is none.
// The pointer stays valid until files grows.
func single(files []file, kind string) (*file, error) {
	var found *file
	for i := range files {
		f := &files[i]
		if !f.is(kind) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("%s and %s both hold a %s document; a node takes one %s document", found.source, f.source, kind, kind)
		}
		found = f
	}
	return found, nil
}

// spec returns the document's spec, adding an empty one when it has none.
func (f *file) spec() (map[string]any, error) {
	switch s := f.doc.Object["spec"].(type) {
	case map[string]any:
		return s, nil
	case nil:
		empty := map[string]any{}
		f.doc.Object["spec"] = empty
		return empty, nil
	default:
		return nil, fmt.Errorf("%s: the %s's spec is not an object", f.source, f.doc.Kind)
	}
}

// own sets the spec's field to v, refusing a document that sets it already.
func (f *file) own(spec map[string]any, field string, v any) error {
	if _, ok := spec[field]; ok {
		return fmt.Errorf("%s: the %s %q sets spec.%s, which Cluster API owns", f.source, f.doc.Kind, f.doc.Name, field)
	}
	spec[field] = v
	return nil
}

// setNode sets the fields of the Node document that the Machine owns.
func setNode(f *file, m Machine) error {
	spec, err := f.spec()
	if err != nil {
		return err
	}
	if err := f.own(spec, "type", m.Type); err != nil {
		return err
	}
	if _, ok := spec["providerID"]; ok || m.ProviderID != "" {
		return f.own(spec, "providerID", m.ProviderID)
	}
	return nil
}

// setCluster sets the fields of the Cluster document that the Cluster owns, and the images that
// derive from the Kubernetes version where the user names none.
func setCluster(f *file, m Machine) error {
	spec, err := f.spec()
	if err != nil {
		return err
	}
	if err := f.own(spec, "endpoint", m.Endpoint); err != nil {
		return err
	}
	pods, podFamilies, err := byFamily("pod", m.PodCIDRs)
	if err != nil {
		return err
	}
	services, serviceFamilies, err := byFamily("service", m.ServiceCIDRs)
	if err != nil {
		return err
	}
	families := podFamilies
	switch {
	case len(families) == 0:
		families = serviceFamilies
	case len(serviceFamilies) > 0 && !slices.Equal(podFamilies, serviceFamilies):
		return fmt.Errorf("the Cluster's pod ranges are of the families %s, its service ranges of %s; both name the same families in the same order", strings.Join(podFamilies, ", "), strings.Join(serviceFamilies, ", "))
	}
	if len(families) > 0 {
		if err := f.own(spec, "ipFamilies", toAny(families)); err != nil {
			return err
		}
	}
	if len(pods) > 0 {
		if err := f.own(spec, "podCIDRs", pods); err != nil {
			return err
		}
	}
	if len(services) > 0 {
		if err := f.own(spec, "serviceCIDRs", services); err != nil {
			return err
		}
	}
	if m.Version == "" {
		return nil
	}
	images, ok := spec["images"].(map[string]any)
	if !ok {
		if _, set := spec["images"]; set {
			return fmt.Errorf("%s: the Cluster's spec.images is not an object", f.source)
		}
		images = map[string]any{}
		spec["images"] = images
	}
	tag := "v" + strings.TrimPrefix(m.Version, "v")
	for field, repository := range versionedImages {
		if _, set := images[field]; !set {
			images[field] = repository + ":" + tag
		}
	}
	return nil
}

// byFamily sorts the ranges, the Cluster's of what, by family, as a Cluster document's ranges
// are written, and returns the families in their order. It refuses two ranges of a family and a
// range with host bits set, so chalkos and Cluster API write each range alike.
func byFamily(what string, cidrs []string) (map[string]any, []string, error) {
	ranges := map[string]any{}
	var families []string
	for _, c := range cidrs {
		p, err := netip.ParsePrefix(c)
		if err != nil {
			return nil, nil, fmt.Errorf("the Cluster's %s range %q: %w", what, c, err)
		}
		family := "ipv4"
		if p.Addr().Is6() {
			family = "ipv6"
		}
		if _, ok := ranges[family]; ok {
			return nil, nil, fmt.Errorf("the Cluster has two %s %s ranges; chalkos takes one per family", family, what)
		}
		if p != p.Masked() {
			return nil, nil, fmt.Errorf("the Cluster's %s range %s has host bits set; write it as %s", what, c, p.Masked())
		}
		ranges[family] = p.String()
		families = append(families, family)
	}
	return ranges, families, nil
}

// toAny is the strings as a document's list holds them.
func toAny(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
