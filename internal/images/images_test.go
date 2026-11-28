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

package images

import (
	"strconv"
	"testing"
)

// A full cache drops one entry for a new key, and none for a key it holds.
func TestHoldDropsOne(t *testing.T) {
	var m map[string]int
	for i := range maxArtifacts {
		hold(&m, strconv.Itoa(i), i)
	}
	hold(&m, "0", 0)
	if len(m) != maxArtifacts {
		t.Fatalf("%d entries after holding a held key, want %d", len(m), maxArtifacts)
	}
	hold(&m, "new", -1)
	if _, ok := m["new"]; !ok || len(m) != maxArtifacts {
		t.Errorf("%d entries after holding a new key, want %d with it", len(m), maxArtifacts)
	}
}
