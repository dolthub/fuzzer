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

// BlueprintManager handles the blueprint creation and alteration during a cycle's run.
type BlueprintManager struct{}

var _ HookRegistrant = (*BlueprintManager)(nil)

// Register implements the HookRegistrant interface.
func (m *BlueprintManager) Register(hooks *Hooks) {
	hooks.CycleInitialized(m.InitializeBlueprint)
	hooks.SQLStatementPostExecution(m.UpdateStatementsExecuted)
	hooks.BranchCreated(m.NewBranch)
	hooks.TableCreated(m.NewTable)
}

// InitializeBlueprint is run when the cycle is initialized. Handles the initialization of the blueprint for this cycle.
func (m *BlueprintManager) InitializeBlueprint(c *Cycle) error {
	branchCount, err := c.Planner.Base.Amounts.Branches.RandomValue()
	if err != nil {
		return errors.Wrap(err)
	}
	c.Blueprint.BranchCount = uint64(branchCount)
	tableCount, err := c.Planner.Base.Amounts.Tables.RandomValue()
	if err != nil {
		return errors.Wrap(err)
	}
	c.Blueprint.TableCount = uint64(tableCount)
	c.Blueprint.TargetRowCount = map[string]map[string]uint64{"main": make(map[string]uint64)}
	return nil
}

// UpdateStatementsExecuted sets the SQLStatementsExecuted when a statement has executed.
func (m *BlueprintManager) UpdateStatementsExecuted(c *Cycle, statement string) error {
	c.Blueprint.SQLStatementsExecuted += 1
	return nil
}

// NewBranch is run when a new branch has been created.
func (m *BlueprintManager) NewBranch(c *Cycle, branch *Branch) error {
	tablesOnThisBranch := make(map[string]uint64)
	currentBranchName := c.GetCurrentBranch().Name
	var rowCount int64
	var err error
	for tableName := range c.Blueprint.TargetRowCount[currentBranchName] {
		if c.Planner.Base.Options.LowerRowsMainOnly && currentBranchName != "main" {
			rowCount, err = c.Planner.Base.Amounts.Rows.RandomValueExpandLower(0)
			if err != nil {
				return errors.Wrap(err)
			}
		} else {
			rowCount, err = c.Planner.Base.Amounts.Rows.RandomValue()
			if err != nil {
				return errors.Wrap(err)
			}
		}
		tablesOnThisBranch[tableName] = uint64(rowCount)
	}
	c.Blueprint.TargetRowCount[branch.Name] = tablesOnThisBranch
	return nil
}

// NewTable is run when a new table has been created.
func (m *BlueprintManager) NewTable(c *Cycle, table *Table) error {
	currentBranchName := c.GetCurrentBranch().Name
	var rowCount int64
	var err error
	if c.Planner.Base.Options.LowerRowsMainOnly && currentBranchName != "main" {
		rowCount, err = c.Planner.Base.Amounts.Rows.RandomValueExpandLower(0)
		if err != nil {
			return errors.Wrap(err)
		}
	} else {
		rowCount, err = c.Planner.Base.Amounts.Rows.RandomValue()
		if err != nil {
			return errors.Wrap(err)
		}
	}
	c.Blueprint.TargetRowCount[currentBranchName][table.Name] = uint64(rowCount)
	return nil
}
