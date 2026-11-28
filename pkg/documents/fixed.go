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

// ClusterWide returns the documents every control-plane Machine of cluster installs alike at the
// Kubernetes version: files, a ChalkosControlPlane's spec.files, with the fields For fills from
// the Cluster and the version, but without the Node document, which is each Machine's own.
func ClusterWide(cluster *clusterv1.Cluster, version string, files []runtime.RawExtension) ([]config.Document, error) {
	network := cluster.Spec.ClusterNetwork
	docs, err := For(Machine{
		Name:         cluster.Name,
		Type:         ControlPlane,
		Endpoint:     Endpoint(cluster.Spec.ControlPlaneEndpoint.Host, cluster.Spec.ControlPlaneEndpoint.Port),
		PodCIDRs:     network.Pods.CIDRBlocks,
		ServiceCIDRs: network.Services.CIDRBlocks,
		Version:      version,
	}, files, nil)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(docs, func(d config.Document) bool {
		return d.APIVersion == kubernetesAPI && d.Kind == "Node"
	}), nil
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
	return "the cluster-wide documents change " + strings.Join(parts, ", ") + ": every node of the cluster holds these fields alike, and a running cluster keeps them; revert the change"
}

// jsonText is a document's value as JSON; null for one the document leaves out.
func jsonText(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}
