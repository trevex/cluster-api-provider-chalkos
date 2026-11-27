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

// Package conditionlog logs the changes of an object's conditions, so that a controller logs
// each step of its progress once rather than at every reconcile.
package conditionlog

import (
	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Changes logs, at info level, each condition of the types whose status, reason or message in
// after differs from before.
func Changes(log logr.Logger, before, after []metav1.Condition, types ...string) {
	for _, t := range types {
		b, a := find(before, t), find(after, t)
		if a == nil || (b != nil && b.Status == a.Status && b.Reason == a.Reason && b.Message == a.Message) {
			continue
		}
		kv := []any{"condition", t, "status", a.Status, "reason", a.Reason}
		if a.Message != "" {
			kv = append(kv, "message", a.Message)
		}
		log.Info("Condition changed", kv...)
	}
}

// find returns the condition of the type; nil when there is none.
func find(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
