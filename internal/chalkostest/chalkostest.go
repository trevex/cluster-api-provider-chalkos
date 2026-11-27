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

// Package chalkostest lets tests use chalkos's own test helpers, whose fixtures live in the
// chalkos module's source.
package chalkostest

import (
	"os/exec"
	"strings"
	"sync"
	"testing"
)

// Module is the chalkos module's path.
const Module = "github.com/trevex/chalkos"

// moduleDir is the chalkos module's directory, which the go command finds once per test binary.
var moduleDir = sync.OnceValues(func() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", Module).Output()
	return strings.TrimSpace(string(out)), err
})

// Chdir makes the chalkos module's directory the test's working directory. chalkos's test
// helpers, such as sdktest.Serve and schematest.Files, find their fixtures from the working
// directory up to the first go.mod, which is then chalkos's. It changes the process's working
// directory, so it is not for parallel tests.
func Chdir(t *testing.T) {
	t.Helper()
	dir, err := moduleDir()
	if err != nil {
		t.Fatalf("find the %s module's directory: %v", Module, err)
	}
	t.Chdir(dir)
}
