// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package run

import "github.com/dolthub/fuzzer/errors"

// GCManager handles running GC commands throughout the cycle.
type GCManager struct {
	statementsSinceLastGC uint64
	dataSizeSinceLastGC   uint64
}

var _ HookRegistrant = (*GCManager)(nil)

// Register implements the HookRegistrant interface.
func (m *GCManager) Register(hooks *Hooks) {
	hooks.CycleInitialized(m.Initialize)
	hooks.SQLStatementPostExecution(m.Counter)
	hooks.RepositoryFinished(m.Finish)
}

// Initialize resets the state of GCManager.
func (m *GCManager) Initialize(c *Cycle) error {
	m.statementsSinceLastGC = 0
	m.dataSizeSinceLastGC = 0
	return nil
}

// Counter gets a rough measure of how much data has been written, and runs GC once a threshold has been crossed.
func (m *GCManager) Counter(c *Cycle, statement string) error {
	m.statementsSinceLastGC += 1
	m.dataSizeSinceLastGC += uint64(len(statement))
	if m.statementsSinceLastGC > 256*1024 || m.dataSizeSinceLastGC > 256*1024*1024 { // 256MB
		m.statementsSinceLastGC = 0
		m.dataSizeSinceLastGC = 0
		if _, err := c.CliQuery("gc"); err != nil {
			return errors.Wrap(err)
		}
	}
	return nil
}

// Finish runs GC once all of the data has been written to the repository.
func (m *GCManager) Finish(c *Cycle) error {
	if m.statementsSinceLastGC == 0 && m.dataSizeSinceLastGC == 0 {
		return nil
	}
	m.statementsSinceLastGC = 0
	m.dataSizeSinceLastGC = 0
	if _, err := c.CliQuery("gc"); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
