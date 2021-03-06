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
	"math"
	"os"
	"time"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/utils"
)

// RepositoryManager handles the general repository generation commands throughout the cycle.
type RepositoryManager struct {
	clearedBranches   map[string]struct{}
	tableProbability  uint64
	branchProbability uint64
}

var _ HookRegistrant = (*RepositoryManager)(nil)

// Register implements the HookRegistrant interface.
func (m *RepositoryManager) Register(hooks *Hooks) {
	hooks.CycleInitialized(m.Initialize)
	hooks.CycleStarted(m.CycleStarted)
}

// Initialize resets the state of the repository manager.
func (m *RepositoryManager) Initialize(c *Cycle) error {
	m.clearedBranches = make(map[string]struct{})
	m.tableProbability = 0
	m.branchProbability = 0
	return nil
}

// CycleStarted creates the initial table and starts the MainLoop of the repository manager.
func (m *RepositoryManager) CycleStarted(c *Cycle) error {
	// This is only set when a command is replaying a log file, so we don't want to generate any data in that case.
	// If the RepositoryManager ever handles more than just generating and validating data, this may need to be updated.
	if c.Planner.Base.Arguments.DontGenRandomData {
		return nil
	}
	_, err := c.GetCurrentBranch().NewTable(c)
	if err != nil {
		return errors.Wrap(err)
	}
	// These probabilities are used as such: if we generate a random uint64 across the whole range, then we return a hit
	// if that value is less than or equal to the probability value.
	m.tableProbability = math.MaxUint64 / (uint64(c.Planner.Base.Amounts.Rows.Median()) * 2)
	m.branchProbability = math.MaxUint64 / uint64(float64(c.Planner.Base.Amounts.Rows.Median())*1.5*float64(c.Blueprint.TableCount))
	c.QueueAction(m.MainLoop)
	return nil
}

// MainLoop is the main execution loop of the repository manager.
func (m *RepositoryManager) MainLoop(c *Cycle) error {
	currentBranch := c.GetCurrentBranch()
	tables := c.GetCurrentBranch().GetWorkingSet().Tables
	branches := c.GetBranchNames()

	// Check if we create a new table or branch
	probabilityVal, err := rand.Uint64()
	if err != nil {
		return errors.Wrap(err)
	}
	if currentBranch.Name == "main" && uint64(len(tables)) < c.Blueprint.TableCount &&
		probabilityVal < m.tableProbability {
		_, err := currentBranch.NewTable(c)
		if err != nil {
			return errors.Wrap(err)
		}
		c.QueueAction(m.MainLoop)
		return nil
	}
	if uint64(len(branches)) < c.Blueprint.BranchCount && probabilityVal < m.branchProbability {
		_, err = currentBranch.Commit(c, false)
		if err != nil {
			return errors.Wrap(err)
		}
		_, err = currentBranch.NewBranch(c)
		if err != nil {
			return errors.Wrap(err)
		}
		c.QueueAction(m.MainLoop)
		return nil
	}

	// Get an unfinished table, if there is one
	tableRandArray, err := utils.NewRandomArray(int64(len(tables)))
	if err != nil {
		return errors.Wrap(err)
	}
	var table *Table
	for i, ok := tableRandArray.NextIndex(); ok; i, ok = tableRandArray.NextIndex() {
		rowCount, err := tables[i].Data.GetRowCount()
		if err != nil {
			return errors.Wrap(err)
		}
		if uint64(rowCount) < c.Blueprint.TargetRowCount[currentBranch.Name][tables[i].Name] {
			table = tables[i]
			break
		}
	}

	// If all the tables have their target amount of rows, then table will be nil
	if table == nil {
		// If we still have tables to create on main then we create them now
		if currentBranch.Name == "main" && uint64(len(tables)) < c.Blueprint.TableCount {
			_, err = currentBranch.NewTable(c)
			if err != nil {
				return errors.Wrap(err)
			}
			c.QueueAction(m.MainLoop)
			return nil
		}
		// If we still have branches to create then we create them now
		if uint64(len(branches)) < c.Blueprint.BranchCount {
			// Have to commit before creating branch
			_, err = currentBranch.Commit(c, false)
			if err != nil {
				return errors.Wrap(err)
			}
			_, err = currentBranch.NewBranch(c)
			if err != nil {
				return errors.Wrap(err)
			}
			c.QueueAction(m.MainLoop)
			return nil
		}
		// Commit this branch's working set before we switch
		_, err = currentBranch.Commit(c, false)
		if err != nil {
			return errors.Wrap(err)
		}
		m.clearedBranches[currentBranch.Name] = struct{}{}
		branchRandArray, err := utils.NewRandomArray(int64(len(branches)))
		if err != nil {
			return errors.Wrap(err)
		}
		for i, ok := branchRandArray.NextIndex(); ok; i, ok = branchRandArray.NextIndex() {
			if _, ok := m.clearedBranches[branches[i]]; !ok {
				err = c.SwitchCurrentBranch(branches[i])
				if err != nil {
					return errors.Wrap(err)
				}
				c.QueueAction(m.MainLoop)
				return nil
			}
		}
		c.QueueAction(m.ValidateRows)
		return nil
	}

	// Execute the next statement
	statement, err := c.statementDist.Get(1)
	if err != nil {
		return errors.Wrap(err)
	}
	statementStr, err := statement.(Statement).GenerateStatement(table)
	if err != nil {
		return errors.Wrap(err)
	}
	err = c.SqlServer(statementStr)
	if err != nil {
		return errors.Wrap(err)
	}

	c.QueueAction(m.MainLoop)
	return nil
}

// ValidateRows validates all rows of each table on each branch according to the stored data.
func (m *RepositoryManager) ValidateRows(c *Cycle) error {
	err := c.Logger.WriteLine(LogType_INFO,
		fmt.Sprintf("Validating Data: %s", time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}

	for _, branchName := range c.GetBranchNames() {
		// Using a function here to make use of the defer
		err = func() (err error) {
			err = c.SwitchCurrentBranch(branchName)
			if err != nil {
				return errors.Wrap(err)
			}
			currentCommitTables := c.GetCurrentBranch().GetWorkingSet().Tables
			// If we encounter an error then we should export all of our internal data to compare against
			defer func() {
				if err != nil {
					fErr := m.exportTableData(c, currentCommitTables...)
					if fErr != nil {
						err = errors.New(fmt.Sprintf("Error 1: %s\n\nError 2: %s", err.Error(), fErr.Error()))
					}
				}
			}()

			for _, table := range currentCommitTables {
				// Using a function here to make use of the defer
				err = (func() error {
					internalCursor, err := table.Data.GetRowCursor()
					if err != nil {
						return errors.Wrap(err)
					}
					defer internalCursor.Close()
					doltCursor, err := table.GetDoltCursor(c)
					if err != nil {
						return errors.Wrap(err)
					}
					defer func() {
						_ = doltCursor.Close()
					}()

					var iRow Row
					var ok bool
					for iRow, ok, err = internalCursor.NextRow(); ok && err == nil; iRow, ok, err = internalCursor.NextRow() {
						dRow, ok, err := doltCursor.NextRow()
						if !ok {
							return errors.New(fmt.Sprintf("On table `%s`, internal data contains more rows than Dolt", table.Name))
						}
						if err != nil {
							return errors.Wrap(err)
						}
						if !iRow.Equals(dRow) {
							return errors.New(fmt.Sprintf("On table `%s`, internal data contains [%s]\nDolt contains [%s]",
								table.Name, iRow.MySQLString(), dRow.MySQLString()))
						}
					}
					if err != nil {
						return errors.Wrap(err)
					}

					_, ok, err = doltCursor.NextRow()
					if ok {
						return errors.New(fmt.Sprintf("On table `%s`, Dolt contains more rows than internal data", table.Name))
					}
					if err != nil {
						return errors.Wrap(err)
					}
					return nil
				})()
				if err != nil {
					return errors.Wrap(err)
				}
			}
			return nil
		}()
		if err != nil {
			return errors.Wrap(err)
		}
	}

	if err = c.Planner.Hooks.RunHook(Hook{
		Type:  HookType_RepositoryFinished,
		Cycle: c,
	}); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// exportTableData exports the data for each given table.
func (m *RepositoryManager) exportTableData(c *Cycle, tables ...*Table) error {
	internalDataPath := c.Planner.Base.Arguments.RepoWorkingPath + c.Name + "/internal_data"
	err := os.Mkdir(internalDataPath, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	for _, table := range tables {
		err = table.Data.ExportToCSV(fmt.Sprintf("%s/%s.csv", internalDataPath, table.Name))
		if err != nil {
			return errors.Wrap(err)
		}
	}
	if c.Planner.Base.Options.ZipInternalData {
		return utils.ZipDirectory(internalDataPath+"/", internalDataPath+".zip", c.Planner.Base.Options.DeleteAfterZip)
	}
	return nil
}
