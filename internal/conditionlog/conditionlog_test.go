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

package conditionlog

import (
	"strings"
	"testing"

	"github.com/go-logr/logr/funcr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestChanges(t *testing.T) {
	waiting := metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Waiting", Message: "Waiting for X"}
	installed := metav1.Condition{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Installed"}
	other := metav1.Condition{Type: "Other", Status: metav1.ConditionTrue, Reason: "Fine"}
	for _, tc := range []struct {
		name          string
		before, after []metav1.Condition
		want          []string
	}{
		{"new", nil, []metav1.Condition{waiting}, []string{`"condition"="Ready" "status"="False" "reason"="Waiting" "message"="Waiting for X"`}},
		{"unchanged", []metav1.Condition{waiting}, []metav1.Condition{waiting}, nil},
		{"changed", []metav1.Condition{waiting}, []metav1.Condition{installed}, []string{`"condition"="Ready" "status"="True" "reason"="Installed"`}},
		{"message changed", []metav1.Condition{waiting}, []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Waiting", Message: "Waiting for Y"}}, []string{`"message"="Waiting for Y"`}},
		{"other type", nil, []metav1.Condition{other}, nil},
		{"removed", []metav1.Condition{waiting}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lines []string
			log := funcr.New(func(prefix, args string) { lines = append(lines, args) }, funcr.Options{})
			Changes(log, tc.before, tc.after, "Ready")
			if len(lines) != len(tc.want) {
				t.Fatalf("logged %q, want %d lines", lines, len(tc.want))
			}
			for i, w := range tc.want {
				if !strings.Contains(lines[i], w) {
					t.Errorf("line %q does not contain %q", lines[i], w)
				}
			}
		})
	}
}
