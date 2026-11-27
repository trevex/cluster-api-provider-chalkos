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
	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"google.golang.org/protobuf/proto"

	nodev1 "github.com/trevex/chalkos/pkg/api/node/v1"
)

// Reasoned is a node's refusal with the code, the reason and the message.
func Reasoned(code connect.Code, reason nodev1.ErrorReason, message string) error {
	return withDetail(connect.NewError(code, message), &nodev1.ReasonDetail{Reason: reason})
}

// withDetail is ce with the detail.
func withDetail(ce *connect.Error, detail proto.Message) error {
	d, err := connectproto.NewErrorDetail(detail)
	if err != nil {
		panic(err)
	}
	return ce.WithDetail(d)
}
