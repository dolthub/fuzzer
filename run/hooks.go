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
	"fmt"

	"github.com/dolthub/fuzzer/errors"
)

// HookRegistrant represents a service that makes use of hooks.
type HookRegistrant interface {
	// Register assigns any necessary functions to the given hooks.
	Register(hooks *Hooks)
}

// Hook is a hook that is called.
type Hook struct {
	Type   HookType
	Cycle  *Cycle
	Param1 interface{}
	Param2 interface{}
}

// HookType is the type of hook to loop over.
type HookType string

const (
	HookType_CycleInitialized          HookType = "CycleInitialized"
	HookType_CycleStarted              HookType = "CycleStarted"
	HookType_CycleEnded                HookType = "CycleEnded"
	HookType_RepositoryFinished        HookType = "RepositoryFinished"
	HookType_BranchCreated             HookType = "BranchCreated"
	HookType_BranchSwitched            HookType = "BranchSwitched"
	HookType_CommitCreated             HookType = "CommitCreated"
	HookType_TableCreated              HookType = "TableCreated"
	HookType_IndexCreated              HookType = "IndexCreated"
	HookType_ForeignKeyCreated         HookType = "ForeignKeyCreated"
	HookType_SqlStatementPreExecution  HookType = "SqlStatementPreExecution"
	HookType_SqlStatementPostExecution HookType = "SqlStatementPostExecution"
)

// Hooks contains all of the callback functions for each step of a cycle.
type Hooks struct {
	cycleInitialized          []func(c *Cycle) error
	cycleStarted              []func(c *Cycle) error
	cycleEnded                []func(c *Cycle) error
	repositoryFinished        []func(c *Cycle) error
	branchCreated             []func(c *Cycle, branch *Branch) error
	branchSwitched            []func(c *Cycle, prevBranch *Branch, branch *Branch) error
	commitCreated             []func(c *Cycle, commit *Commit) error
	tableCreated              []func(c *Cycle, table *Table) error
	indexCreated              []func(c *Cycle, table *Table, index *Index) error
	foreignKeyCreated         []func(c *Cycle, commit *Commit, foreignKey *ForeignKey) error
	sqlStatementPreExecution  []func(c *Cycle, statement string) error
	sqlStatementPostExecution []func(c *Cycle, statement string) error
}

// RunHook loops over all of the hooks of the given type and gives the appropriate data.
func (h *Hooks) RunHook(hook Hook) error {
	switch hook.Type {
	case HookType_CycleInitialized:
		for _, hookFunc := range h.cycleInitialized {
			if err := hookFunc(hook.Cycle); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_CycleStarted:
		for _, hookFunc := range h.cycleStarted {
			if err := hookFunc(hook.Cycle); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_CycleEnded:
		// This one needs to run through all hooks, even if one of them errors.
		var err error
		for _, hookFunc := range h.cycleEnded {
			if eErr := hookFunc(hook.Cycle); eErr != nil && err == nil {
				err = errors.Wrap(eErr)
			}
		}
		return err
	case HookType_RepositoryFinished:
		for _, hookFunc := range h.repositoryFinished {
			if err := hookFunc(hook.Cycle); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_BranchCreated:
		branch := hook.Param1.(*Branch)
		for _, hookFunc := range h.branchCreated {
			if err := hookFunc(hook.Cycle, branch); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_BranchSwitched:
		prevBranch := hook.Param1.(*Branch)
		branch := hook.Param2.(*Branch)
		for _, hookFunc := range h.branchSwitched {
			if err := hookFunc(hook.Cycle, prevBranch, branch); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_CommitCreated:
		commit := hook.Param1.(*Commit)
		for _, hookFunc := range h.commitCreated {
			if err := hookFunc(hook.Cycle, commit); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_TableCreated:
		table := hook.Param1.(*Table)
		for _, hookFunc := range h.tableCreated {
			if err := hookFunc(hook.Cycle, table); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_IndexCreated:
		table := hook.Param1.(*Table)
		index := hook.Param2.(*Index)
		for _, hookFunc := range h.indexCreated {
			if err := hookFunc(hook.Cycle, table, index); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_ForeignKeyCreated:
		commit := hook.Param1.(*Commit)
		foreignKey := hook.Param2.(*ForeignKey)
		for _, hookFunc := range h.foreignKeyCreated {
			if err := hookFunc(hook.Cycle, commit, foreignKey); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_SqlStatementPreExecution:
		statement := hook.Param1.(string)
		for _, hookFunc := range h.sqlStatementPreExecution {
			if err := hookFunc(hook.Cycle, statement); err != nil {
				return errors.Wrap(err)
			}
		}
	case HookType_SqlStatementPostExecution:
		statement := hook.Param1.(string)
		for _, hookFunc := range h.sqlStatementPostExecution {
			if err := hookFunc(hook.Cycle, statement); err != nil {
				return errors.Wrap(err)
			}
		}
	default:
		return errors.New(fmt.Sprintf("unknown HookType: %v", hook.Type))
	}
	return nil
}

// CycleInitialized is called right after the dolt repository has been initialized. This should be used to set up values
// for the rest of the cycle.
func (h *Hooks) CycleInitialized(f func(c *Cycle) error) {
	h.cycleInitialized = append(h.cycleInitialized, f)
}

// CycleStarted is called immediately after CycleInitialized.
func (h *Hooks) CycleStarted(f func(c *Cycle) error) {
	h.cycleStarted = append(h.cycleStarted, f)
}

// CycleEnded is called when the cycle has ended. Any close functions should be called here. This is always called when
// a cycle is ended for any reason, including on errors. It is not guaranteed that CycleStarted has been called before
// this hook.
func (h *Hooks) CycleEnded(f func(c *Cycle) error) {
	h.cycleEnded = append(h.cycleEnded, f)
}

// RepositoryFinished is called when the repository has finished writing data to its tables.
func (h *Hooks) RepositoryFinished(f func(c *Cycle) error) {
	h.repositoryFinished = append(h.repositoryFinished, f)
}

// BranchCreated is called when a branch has been created.
func (h *Hooks) BranchCreated(f func(c *Cycle, branch *Branch) error) {
	h.branchCreated = append(h.branchCreated, f)
}

// BranchSwitched is called when the active branch has switched.
func (h *Hooks) BranchSwitched(f func(c *Cycle, prevBranch *Branch, branch *Branch) error) {
	h.branchSwitched = append(h.branchSwitched, f)
}

// CommitCreated is called when a commit has been created. This is not the working set.
func (h *Hooks) CommitCreated(f func(c *Cycle, commit *Commit) error) {
	h.commitCreated = append(h.commitCreated, f)
}

// TableCreated is called when a table has been created.
func (h *Hooks) TableCreated(f func(c *Cycle, table *Table) error) {
	h.tableCreated = append(h.tableCreated, f)
}

// IndexCreated is called when an index has been created.
func (h *Hooks) IndexCreated(f func(c *Cycle, table *Table, index *Index) error) {
	h.indexCreated = append(h.indexCreated, f)
}

// ForeignKeyCreated is called when a foreign key has been created.
func (h *Hooks) ForeignKeyCreated(f func(c *Cycle, commit *Commit, foreignKey *ForeignKey) error) {
	h.foreignKeyCreated = append(h.foreignKeyCreated, f)
}

// SQLStatementPreExecution is called whenever a SQL statement is about to be executed.
func (h *Hooks) SQLStatementPreExecution(f func(c *Cycle, statement string) error) {
	h.sqlStatementPreExecution = append(h.sqlStatementPreExecution, f)
}

// SQLStatementPostExecution is called after a SQL statement has been executed.
func (h *Hooks) SQLStatementPostExecution(f func(c *Cycle, statement string) error) {
	h.sqlStatementPostExecution = append(h.sqlStatementPostExecution, f)
}
