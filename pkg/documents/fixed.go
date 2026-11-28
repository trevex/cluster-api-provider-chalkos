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

package documents

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	"github.com/trevex/chalkos/pkg/config"
	"github.com/trevex/chalkos/pkg/sdk"
)

// observedSource names the documents FixedChanges compares with in errors.
const observedSource = "the ChalkosControlPlane's status.observedFiles"

// ClusterMachine is the Machine of the name, node type and Kubernetes version in cluster, with the
// endpoint and the pod and service ranges of the Cluster API Cluster; it has no provider ID.
func ClusterMachine(cluster *clusterv1.Cluster, name, typ, version string) Machine {
	network := cluster.Spec.ClusterNetwork
	return Machine{
		Name:         name,
		Type:         typ,
		Endpoint:     Endpoint(cluster.Spec.ControlPlaneEndpoint.Host, cluster.Spec.ControlPlaneEndpoint.Port),
		PodCIDRs:     network.Pods.CIDRBlocks,
		ServiceCIDRs: network.Services.CIDRBlocks,
		Version:      version,
	}
}

// ClusterWide returns the documents every control-plane Machine of cluster installs alike at the
// Kubernetes version: files, a ChalkosControlPlane's spec.files, with the fields For fills from
// the Cluster and the version, but without the Node document, which is each Machine's own, and
// without Secret documents: they hold no fixed fields, and leaving them out keeps a second copy
// of their values out of status.observedFiles. A Cluster without a control-plane endpoint has
// no such documents yet.
func ClusterWide(cluster *clusterv1.Cluster, version string, files []runtime.RawExtension) ([]config.Document, error) {
	if !cluster.Spec.ControlPlaneEndpoint.IsValid() {
		return nil, errors.New("the Cluster has no control-plane endpoint yet")
	}
	docs, err := For(ClusterMachine(cluster, cluster.Name, ControlPlane, version), files, nil)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(docs, func(d config.Document) bool {
		return d.IsSecret() || d.APIVersion == kubernetesAPI && d.Kind == "Node"
	}), nil
}

// Defaulted returns docs with the defaults of the image's contract c filled in, for each document
// of a kind c declares; a document of another kind stays as it is.
func Defaulted(c sdk.Contract, docs []config.Document) ([]config.Document, error) {
	if c.Image == nil || c.Image.Schemas == nil {
		return nil, errors.New("the contract holds no schemas to take defaults from")
	}
	s := c.Image.Schemas
	out := make([]config.Document, len(docs))
	for i, d := range docs {
		out[i] = d
		if _, ok := s.Kind(d.APIVersion, d.Kind); !ok {
			continue
		}
		filled, err := s.Default(d.Object)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", d, err)
		}
		out[i].Object = filled
	}
	return out, nil
}

// Observed is the text status.observedFiles records for docs, the documents ClusterWide returns,
// with the defaults of the image's contract c written out. A field the user leaves out is then
// recorded with the value the Machines got, so FixedChanges finds a change when a later image
// defaults a fixed field differently.
func Observed(c sdk.Contract, docs []config.Document) (string, error) {
	filled, err := Defaulted(c, docs)
	if err != nil {
		return "", err
	}
	enc, err := config.Encode(filled)
	if err != nil {
		return "", err
	}
	return enc.Text, nil
}

// FixedChanges are the changes from observed, the text of a ChalkosControlPlane's
// status.observedFiles, to cur, the documents ClusterWide returns now, of fields that every node
// of a cluster holds alike, as the image's contract c marks them, such as a Cluster's
// spec.domain. Without observed documents there is nothing to compare with, and no change.
func FixedChanges(c sdk.Contract, observed string, cur []config.Document) ([]sdk.FixedChange, error) {
	if observed == "" {
		return nil, nil
	}
	old, err := config.Parse(strings.NewReader(observed))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", observedSource, err)
	}
	return sdk.FixedChanges(c, old, cur)
}

// DescribeFixed says, as a condition's message, what the changes FixedChanges found change and
// that the change is to be reverted.
func DescribeFixed(changes []sdk.FixedChange) string {
	parts := make([]string, len(changes))
	for i, ch := range changes {
		parts[i] = fmt.Sprintf("%s/%s %s from %s to %s", ch.Kind, ch.Name, ch.Path, jsonText(ch.Old), jsonText(ch.New))
	}
	return "The cluster-wide documents change " + strings.Join(parts, ", ") + ": every node of the cluster holds these fields alike, and a running cluster keeps them; revert it in spec.files or in the Cluster's clusterNetwork or controlPlaneEndpoint"
}

// jsonText is a document's value as JSON; null for one the document leaves out.
func jsonText(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}
