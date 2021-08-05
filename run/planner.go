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

import (
	"runtime"
	"time"

	"github.com/dolthub/fuzzer/parameters"
)

// Planner is the entry point that commands may use to hook into the various points of a cycle. It also creates each
// cycle.
type Planner struct {
	Hooks            *Hooks
	Base             *parameters.Base
	lastRunStartTime time.Time
}

// NewPlanner returns a new *Planner from the given parameters.Base.
func NewPlanner(base *parameters.Base) (*Planner, error) {
	hooks := &Hooks{}
	(&BlueprintManager{}).Register(hooks)
	(&RepositoryManager{}).Register(hooks)
	if base.Options.ManualGC {
		(&GCManager{}).Register(hooks)
	}
	return &Planner{
		Hooks:            hooks,
		Base:             base,
		lastRunStartTime: time.Unix(0, 0),
	}, nil
}

// NewCycle returns a new *Cycle created from this Planner.
func (p *Planner) NewCycle() (*Cycle, error) {
	// We force GC before each cycle as we put a lot of pressure on the GC each run.
	runtime.GC()
	return newCycle(p)
}
