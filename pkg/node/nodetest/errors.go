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

package nodetest

import (
	"fmt"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"google.golang.org/protobuf/proto"

	kubernetesv1 "github.com/trevex/chalkos/pkg/api/kubernetes/v1"
	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
)

// Reasoned is a node's refusal with the code, the reason and the message.
func Reasoned(code connect.Code, reason nodev1.ErrorReason, message string) error {
	return withDetail(connect.NewError(code, message), &nodev1.ReasonDetail{Reason: reason})
}

// Unreachable is the error of a call to a node at addr that does not answer: the client's own,
// not the node's.
func Unreachable(addr string) error {
	return connect.NewError(connect.CodeUnavailable, fmt.Sprintf("dial %s: connection refused", addr))
}

// NotEtcdMember is a control plane's refusal of a request that needs an etcd member, or of the
// leave of a node that is none.
func NotEtcdMember() error {
	return withDetail(connect.NewError(connect.CodeFailedPrecondition, "the node is not an etcd member"), &kubernetesv1.NotEtcdMember{})
}

// RevokedListFull is a control plane's refusal to revoke node because the revoked nodes' list has
// no room.
func RevokedListFull(node string) error {
	return Reasoned(connect.CodeFailedPrecondition, nodev1.ErrorReason_ERROR_REASON_REVOKED_LIST_FULL,
		fmt.Sprintf("revoking %s would make the admission policy's map of the revoked nodes too long; make room: drop the expired revocations with chalkctl node prune-revoked, or rotate the OS CA and the Kubernetes CA", node))
}

// EtcdUnavailable is a control plane's answer that its etcd did not answer in time: the node
// itself was reached.
func EtcdUnavailable() error {
	return refusal(connect.CodeUnavailable, "etcdserver: request timed out")
}

// QuorumRefused is a control plane's refusal to remove member while the voters left would have no
// healthy quorum.
func QuorumRefused(member string) error {
	return refusal(connect.CodeFailedPrecondition, "removing %s would leave etcd without a healthy quorum", member)
}

// refusal is a node's refusal with the code and the message, without a reason.
func refusal(code connect.Code, format string, args ...any) error {
	return connect.NewError(code, fmt.Sprintf(format, args...)).WithRemote()
}

// withDetail is ce with the detail, as the node answered it.
func withDetail(ce *connect.Error, detail proto.Message) error {
	d, err := connectproto.NewErrorDetail(detail)
	if err != nil {
		panic(err)
	}
	return ce.WithDetail(d).WithRemote()
}
