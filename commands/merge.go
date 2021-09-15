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

package commands

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/types"
	"github.com/dolthub/fuzzer/utils"
)

// Merge handles merge testing.
type Merge struct {
	mergeCombinations map[mergeCombination]bool
}

// mergeCombination is the combination of branches representing a specific merge.
type mergeCombination struct {
	ours   string
	theirs string
}

// mergeCommits are the commits to be referenced when processing a merge.
type mergeCommits struct {
	ours   *run.Commit
	theirs *run.Commit
	base   *run.Commit
}

// mergeTables contains all of the data referenced by each table, along with the final table.
type mergeTables struct {
	tableName string
	ours      *run.Table
	theirs    *run.Table
	base      *run.Table
	final     *run.Table
}

// mergeConflict represents a merge conflict.
type mergeConflict struct {
	base   run.Row
	ours   run.Row
	theirs run.Row
}

// mergeTableWithConflicts is a merged table with its associated conflicts.
type mergeTableWithConflicts struct {
	base      *run.Table
	ours      *run.Table
	theirs    *run.Table
	final     *run.Table
	conflicts []run.Row
}

var _ Command = (*Merge)(nil)

// init adds the command to the map.
func init() {
	addCommand(&Merge{})
}

// Name implements the interface Command.
func (m *Merge) Name() string {
	return "merge"
}

// Description implements the interface Command.
func (m *Merge) Description() string {
	return "Tests dolt's merge functionality."
}

// ParseArgs implements the interface Command.
func (m *Merge) ParseArgs(commandStr string, ap *argparser.ArgParser, args []string) error {
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{
		ShortDesc: "Tests dolt's merge functionality",
		LongDesc: `This command verifies that "dolt merge" functions as expected under randomly constructed scenarios.
This also performs a validation step before testing merge, which is the same as the "basic" command.`,
		Synopsis: nil,
	}, ap))
	_ = cli.ParseArgsOrDie(ap, args, help)
	return nil
}

// Register implements the HookRegistrant interface.
func (m *Merge) Register(hooks *run.Hooks) {
	hooks.CycleInitialized(m.Reset)
	hooks.CycleStarted(m.VerifyCycle)
	hooks.RepositoryFinished(m.BeginMerge)
}

// Reset resets the state of Merge.
func (m *Merge) Reset(c *run.Cycle) error {
	m.mergeCombinations = make(map[mergeCombination]bool)
	return nil
}

// VerifyCycle verifies that the run.Cycle will support merge testing, and will modify the run.Cycle to guarantee that
// merge can be tested.
func (m *Merge) VerifyCycle(c *run.Cycle) error {
	if c.Blueprint.BranchCount < 2 {
		c.Blueprint.BranchCount = 2
	}
	return nil
}

// BeginMerge starts the merge process.
func (m *Merge) BeginMerge(c *run.Cycle) error {
	err := c.Logger.WriteLine(run.LogType_INFO,
		fmt.Sprintf("Beginning Merge Testing: %s", time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}
	branches := c.GetBranchNames()
	for i := 0; i < len(branches); i++ {
		for j := i + 1; j < len(branches); j++ {
			m.mergeCombinations[mergeCombination{
				ours:   branches[i],
				theirs: branches[j],
			}] = false
			m.mergeCombinations[mergeCombination{
				ours:   branches[j],
				theirs: branches[i],
			}] = false
		}
	}
	c.QueueAction(m.Run)
	return nil
}

// Run is the primary loop that selects a merge combination and processes it.
func (m *Merge) Run(c *run.Cycle) error {
	var combination mergeCombination
	for mc, visited := range m.mergeCombinations {
		if visited == false {
			combination = mc
			m.mergeCombinations[mc] = true
			break
		}
	}
	if combination.ours == "" || combination.theirs == "" { // We've tested all merge combinations
		return nil
	}

	err := c.Logger.WriteLine(run.LogType_INFO,
		fmt.Sprintf(`Merging "%s" into "%s": %s`, combination.theirs, combination.ours, time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}
	err = c.SwitchCurrentBranch(combination.ours)
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = c.GetCurrentBranch().NewCustomBranch(c, combination.UniqueBranchName())
	if err != nil {
		return errors.Wrap(err)
	}
	err = c.SwitchCurrentBranch(combination.UniqueBranchName())
	if err != nil {
		return errors.Wrap(err)
	}

	commits, err := combination.GetCommits(c)
	if err != nil {
		return errors.Wrap(err)
	}
	allMergeTables, err := commits.GetTables()
	if err != nil {
		return errors.Wrap(err)
	}

	var finalTables []mergeTableWithConflicts
	defer func() {
		for _, finalTable := range finalTables {
			finalTable.final.Data.Close()
		}
	}()
	for _, mt := range allMergeTables {
		mtc, err := mt.ProcessMerge()
		if err != nil {
			return errors.Wrap(err)
		}
		finalTables = append(finalTables, mtc)
	}

	_, err = c.CliQuery("merge", combination.theirs)
	if err != nil {
		return errors.Wrap(err)
	}
	for _, finalTable := range finalTables {
		err = finalTable.Verify(c)
		if err != nil {
			fErr := finalTable.Export(c)
			if fErr != nil {
				return errors.New(fmt.Sprintf("Error 1: %s\n\nError 2: %s", err.Error(), fErr.Error()))
			} else {
				return errors.Wrap(err)
			}
		}
	}
	_, err = c.CliQuery("merge", "--abort")
	if err != nil && !strings.Contains(err.Error(), "no merge to abort") {
		return errors.Wrap(err)
	}
	_, err = c.CliQuery("reset", "--hard")
	if err != nil {
		return errors.Wrap(err)
	}
	c.QueueAction(m.Run)
	return nil
}

// UniqueBranchName returns the unique branch name that will be used for this merge combination.
func (mc mergeCombination) UniqueBranchName() string {
	return fmt.Sprintf("__merge_%s_%s", mc.ours, mc.theirs)
}

// GetCommits returns the commits used in this merge.
func (mc mergeCombination) GetCommits(c *run.Cycle) (mergeCommits, error) {
	ourBranch := c.GetBranch(mc.ours)
	if ourBranch == nil {
		return mergeCommits{}, errors.New(fmt.Sprintf("unable to get branch: %s", mc.ours))
	}
	theirBranch := c.GetBranch(mc.theirs)
	if theirBranch == nil {
		return mergeCommits{}, errors.New(fmt.Sprintf("unable to get branch: %s", mc.theirs))
	}
	// We're getting the working set, so we always backtrack to the last commit as the working set is guaranteed to be empty.
	ourCommit := ourBranch.GetWorkingSet().Parents[0]
	theirCommit := theirBranch.GetWorkingSet().Parents[0]

	var baseCommit *run.Commit
	parent1 := ourCommit
	parent2 := theirCommit
	for {
		if parent1.Hash == parent2.Hash {
			baseCommit = parent1
			break
		}
		// TODO: Whenever we can generate commits with multiple parents, we'll need to update this logic
		if len(parent2.Parents) == 0 {
			if len(parent1.Parents) == 0 {
				return mergeCommits{},
					errors.New(fmt.Sprintf("the following branches do not have a common ancestor: %s, %s", mc.ours, mc.theirs))
			} else {
				parent1 = parent1.Parents[0]
				parent2 = theirCommit
			}
		} else {
			parent2 = parent2.Parents[0]
		}
	}

	return mergeCommits{
		ours:   ourCommit,
		theirs: theirCommit,
		base:   baseCommit,
	}, nil
}

// GetTables returns all of the tables for the calling merge commits.
func (mc mergeCommits) GetTables() ([]mergeTables, error) {
	var allMergeTables []mergeTables
	tables := make(map[string]*mergeTables)
	for _, baseTable := range mc.base.Tables {
		tables[baseTable.Name] = &mergeTables{
			tableName: baseTable.Name,
			base:      baseTable,
		}
	}
	for _, ourTable := range mc.ours.Tables {
		if mt, ok := tables[ourTable.Name]; ok {
			mt.ours = ourTable
		} else {
			tables[ourTable.Name] = &mergeTables{
				tableName: ourTable.Name,
				ours:      ourTable,
			}
		}
	}
	for _, theirTable := range mc.theirs.Tables {
		if mt, ok := tables[theirTable.Name]; ok {
			mt.theirs = theirTable
		} else {
			tables[theirTable.Name] = &mergeTables{
				tableName: theirTable.Name,
				theirs:    theirTable,
			}
		}
	}
	for tableName, mt := range tables {
		cases := 0
		if mt.base != nil {
			cases += 1
		}
		if mt.ours != nil {
			cases += 2
		}
		if mt.theirs != nil {
			cases += 4
		}
		switch cases {
		case 1: // TODO: This represents a deletion on both branches, but we don't yet support table deletion in the fuzzer
			return nil, errors.New("merge implementation inconsistency, table deletion should not yet be possible")
		case 2: // This represents an addition on ours, so it goes straight to final
			mt.final = mt.ours
			mt.ours = nil
			allMergeTables = append(allMergeTables, *mt)
		case 1 + 2: // TODO: This represents a deletion on theirs, but we don't yet support table deletion in the fuzzer
			return nil, errors.New("merge implementation inconsistency, table deletion should not yet be possible")
		case 4: // This represents an addition on theirs, so it goes straight to final
			mt.final = mt.theirs
			mt.theirs = nil
			allMergeTables = append(allMergeTables, *mt)
		case 1 + 4: // TODO: This represents a deletion on ours, but we don't yet support table deletion in the fuzzer
			return nil, errors.New("merge implementation inconsistency, table deletion should not yet be possible")
		case 2 + 4: // TODO: This represents the same named table added on both branches, which is not yet supported
			return nil, errors.New("unable to reconcile table addition on two branches with the same name")
		case 1 + 2 + 4: // This represents a table having potential changes in both branches
			allMergeTables = append(allMergeTables, *mt)
		default:
			return nil, errors.New(fmt.Sprintf("unknown case for merging table: %s: %d", tableName, cases))
		}
	}
	sort.Slice(allMergeTables, func(i, j int) bool {
		return allMergeTables[i].tableName < allMergeTables[j].tableName
	})
	return allMergeTables, nil
}

// ProcessMerge processes the called tables by merging them using our internal data.
func (mt mergeTables) ProcessMerge() (mergeTableWithConflicts, error) {
	if mt.final != nil {
		return mergeTableWithConflicts{
			ours:      mt.ours,
			theirs:    mt.theirs,
			base:      mt.base,
			final:     mt.final,
			conflicts: nil,
		}, nil
	}
	final, err := mt.ours.Copy()
	var conflicts []run.Row
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}

	baseCursor, err := mt.base.Data.GetRowCursor()
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}
	defer baseCursor.Close()
	ourCursor, err := mt.ours.Data.GetRowCursor()
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}
	defer ourCursor.Close()
	theirCursor, err := mt.theirs.Data.GetRowCursor()
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}
	defer theirCursor.Close()

	baseRow, baseRowExists, err := baseCursor.NextRow()
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}
	ourRow, ourRowExists, err := ourCursor.NextRow()
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}
	theirRow, theirRowExists, err := theirCursor.NextRow()
	if err != nil {
		return mergeTableWithConflicts{}, errors.Wrap(err)
	}

	for {
		if !baseRowExists && !ourRowExists && !theirRowExists {
			break
		}
		switch ourRow.PKCompare(baseRow) {
		case -1:
			switch theirRow.PKCompare(baseRow) {
			case -1: // both are new, check if same
				switch ourRow.PKCompare(theirRow) {
				case -1: // ours is new
					ourRow, ourRowExists, err = ourCursor.NextRow()
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
				case 0: // same row, check for equivalence
					if !ourRow.Equals(theirRow) { // both modified, conflict
						conflicts = append(conflicts, mergeConflict{
							base:   run.Row{},
							ours:   ourRow,
							theirs: theirRow,
						}.ToRow(final))
					}
					ourRow, ourRowExists, err = ourCursor.NextRow()
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
					theirRow, theirRowExists, err = theirCursor.NextRow()
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
				case 1: // theirs is new
					err = final.Data.Exec(fmt.Sprintf("REPLACE INTO `%s` VALUES (%s);", final.Name, theirRow.SQLiteString()))
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
					theirRow, theirRowExists, err = theirCursor.NextRow()
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
				}
			case 0: // ours is new
				ourRow, ourRowExists, err = ourCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			case 1: // ours is new
				ourRow, ourRowExists, err = ourCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			}
		case 0:
			switch theirRow.PKCompare(baseRow) {
			case -1: // theirs is new
				err = final.Data.Exec(fmt.Sprintf("REPLACE INTO `%s` VALUES (%s);", final.Name, theirRow.SQLiteString()))
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
				theirRow, theirRowExists, err = theirCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			case 0: // check for updates
				if !ourRow.Equals(theirRow) {
					if ourRow.Equals(baseRow) { // theirs modified
						err = final.Data.Exec(fmt.Sprintf("REPLACE INTO `%s` VALUES (%s);", final.Name, theirRow.SQLiteString()))
						if err != nil {
							return mergeTableWithConflicts{}, errors.Wrap(err)
						}
					} else if !theirRow.Equals(baseRow) { // both modified
						mergedRow := ourRow.Copy()
						var conflict *mergeConflict
						for i := 0; i < len(mergedRow.Values); i++ {
							if ourRow.Values[i].Compare(theirRow.Values[i]) == 0 {
								continue
							} else if ourRow.Values[i].Compare(baseRow.Values[i]) == 0 {
								mergedRow.Values[i] = theirRow.Values[i]
							} else if theirRow.Values[i].Compare(baseRow.Values[i]) == 0 {
								continue
							} else {
								conflict = &mergeConflict{
									base:   baseRow,
									ours:   ourRow,
									theirs: theirRow,
								}
								break
							}
						}
						if conflict == nil {
							err = final.Data.Exec(fmt.Sprintf("REPLACE INTO `%s` VALUES (%s);", final.Name, mergedRow.SQLiteString()))
							if err != nil {
								return mergeTableWithConflicts{}, errors.Wrap(err)
							}
						} else {
							conflicts = append(conflicts, conflict.ToRow(final))
						}
					}
				}
				baseRow, baseRowExists, err = baseCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
				ourRow, ourRowExists, err = ourCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
				theirRow, theirRowExists, err = theirCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			case 1: // check for updates, deleted in theirs
				if !ourRow.Equals(baseRow) { // modified ours, conflict
					conflicts = append(conflicts, mergeConflict{
						base:   baseRow,
						ours:   ourRow,
						theirs: run.Row{},
					}.ToRow(final))
				} else { // ours unmodified, valid deletion
					wheresSQLite, err := run.GenerateColumnEqualsSQLite(final.PKCols, ourRow.Key())
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
					err = final.Data.Exec(fmt.Sprintf("DELETE FROM `%s` WHERE %s;", final.Name, strings.Join(wheresSQLite, " AND ")))
					if err != nil {
						return mergeTableWithConflicts{}, errors.Wrap(err)
					}
				}
				baseRow, baseRowExists, err = baseCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
				ourRow, ourRowExists, err = ourCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			}
		case 1:
			switch theirRow.PKCompare(baseRow) {
			case -1: // theirs is new
				err = final.Data.Exec(fmt.Sprintf("REPLACE INTO `%s` VALUES (%s);", final.Name, theirRow.SQLiteString()))
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
				theirRow, theirRowExists, err = theirCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			case 0: // check for updates, deleted in ours
				if !theirRow.Equals(baseRow) { // modified theirs, conflict
					conflicts = append(conflicts, mergeConflict{
						base:   baseRow,
						ours:   run.Row{},
						theirs: theirRow,
					}.ToRow(final))
				}
				baseRow, baseRowExists, err = baseCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
				theirRow, theirRowExists, err = theirCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			case 1: // base row deleted in both
				baseRow, baseRowExists, err = baseCursor.NextRow()
				if err != nil {
					return mergeTableWithConflicts{}, errors.Wrap(err)
				}
			}
		}
	}

	sort.Slice(conflicts, func(i, j int) bool {
		return conflicts[i].Compare(conflicts[j]) == -1
	})
	return mergeTableWithConflicts{
		ours:      mt.ours,
		theirs:    mt.theirs,
		base:      mt.base,
		final:     final,
		conflicts: conflicts,
	}, nil
}

// ToRow returns this merge conflict as a row, which is directly comparable to a conflict returned from Dolt's
// conflict cursor.
func (mc mergeConflict) ToRow(table *run.Table) run.Row {
	allColsLen := len(table.PKCols) + len(table.NonPKCols)
	values := make([]types.Value, 3*allColsLen)
	valIdx := 0
	if mc.base.IsEmpty() {
		for i := 0; i < allColsLen; i, valIdx = i+1, valIdx+1 {
			values[valIdx] = types.NilValue{}
		}
	} else {
		for i := 0; i < allColsLen; i, valIdx = i+1, valIdx+1 {
			values[valIdx] = mc.base.Values[i]
		}
	}
	if mc.ours.IsEmpty() {
		for i := 0; i < allColsLen; i, valIdx = i+1, valIdx+1 {
			values[valIdx] = types.NilValue{}
		}
	} else {
		for i := 0; i < allColsLen; i, valIdx = i+1, valIdx+1 {
			values[valIdx] = mc.ours.Values[i]
		}
	}
	if mc.theirs.IsEmpty() {
		for i := 0; i < allColsLen; i, valIdx = i+1, valIdx+1 {
			values[valIdx] = types.NilValue{}
		}
	} else {
		for i := 0; i < allColsLen; i, valIdx = i+1, valIdx+1 {
			values[valIdx] = mc.theirs.Values[i]
		}
	}
	return run.Row{
		Values:    values,
		PkColsLen: 0,
	}
}

// Export writes all four internal tables (base, ours, theirs, merged) involved in the merge, the conflicts, and a shell
// script to set up and import all the data into a Dolt instance.
func (mtc mergeTableWithConflicts) Export(c *run.Cycle) error {
	internalDataPath := c.Planner.Base.Arguments.RepoWorkingPath + c.Name + "/internal_data"
	err := os.Mkdir(internalDataPath, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	err = mtc.final.Data.ExportToCSV(fmt.Sprintf("%s/merged_%s.csv", internalDataPath, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	err = mtc.ours.Data.ExportToCSV(fmt.Sprintf("%s/our_%s.csv", internalDataPath, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	err = mtc.theirs.Data.ExportToCSV(fmt.Sprintf("%s/their_%s.csv", internalDataPath, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	err = mtc.base.Data.ExportToCSV(fmt.Sprintf("%s/base_%s.csv", internalDataPath, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	err = mtc.exportConflictsToCSV(c)
	if err != nil {
		return errors.Wrap(err)
	}
	err = mtc.exportShellSetup(c)
	if err != nil {
		return errors.Wrap(err)
	}

	if c.Planner.Base.Options.ZipInternalData {
		return utils.ZipDirectory(internalDataPath+"/", internalDataPath+".zip", c.Planner.Base.Options.DeleteAfterZip)
	}
	return nil
}

// exportConflictsToCSV writes the conflict data to a CSV in the working directory.
func (mtc mergeTableWithConflicts) exportConflictsToCSV(c *run.Cycle) error {
	file, err := os.OpenFile(fmt.Sprintf("%s%s/internal_data/conflicts.csv", c.Planner.Base.Arguments.RepoWorkingPath, c.Name),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		fErr := file.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	// Write the column header row
	firstItem := true
	for _, pkCol := range mtc.base.PKCols {
		if firstItem {
			firstItem = false
		} else {
			_, err = file.WriteString(",")
			if err != nil {
				return errors.Wrap(err)
			}
		}
		_, err = file.WriteString("base_" + pkCol.Name)
		if err != nil {
			return errors.Wrap(err)
		}
	}
	for _, nonPkCol := range mtc.base.NonPKCols {
		if firstItem {
			firstItem = false
		} else {
			_, err = file.WriteString(",")
			if err != nil {
				return errors.Wrap(err)
			}
		}
		_, err = file.WriteString("base_" + nonPkCol.Name)
		if err != nil {
			return errors.Wrap(err)
		}
	}
	for _, pkCol := range mtc.ours.PKCols {
		_, err = file.WriteString(",our_" + pkCol.Name)
		if err != nil {
			return errors.Wrap(err)
		}
	}
	for _, nonPkCol := range mtc.ours.NonPKCols {
		_, err = file.WriteString(",our_" + nonPkCol.Name)
		if err != nil {
			return errors.Wrap(err)
		}
	}
	for _, pkCol := range mtc.theirs.PKCols {
		_, err = file.WriteString(",their_" + pkCol.Name)
		if err != nil {
			return errors.Wrap(err)
		}
	}
	for _, nonPkCol := range mtc.theirs.NonPKCols {
		_, err = file.WriteString(",their_" + nonPkCol.Name)
		if err != nil {
			return errors.Wrap(err)
		}
	}
	_, err = file.WriteString("\n")
	if err != nil {
		return errors.Wrap(err)
	}

	// Write the rows
	for _, conflictsRow := range mtc.conflicts {
		_, err = file.WriteString(conflictsRow.CSVString())
		if err != nil {
			return errors.Wrap(err)
		}
		_, err = file.WriteString("\n")
		if err != nil {
			return errors.Wrap(err)
		}
	}
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// exportShellSetup writes a shell setup file that will import the four tables and conflict data into a Dolt instance.
func (mtc mergeTableWithConflicts) exportShellSetup(c *run.Cycle) error {
	file, err := os.OpenFile(fmt.Sprintf("%s%s/internal_data/setup.sh", c.Planner.Base.Arguments.RepoWorkingPath, c.Name),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		fErr := file.Close()
		if fErr != nil && err == nil {
			err = errors.Wrap(fErr)
		}
	}()

	_, err = file.WriteString("#!/bin/sh\nset -e\n\ndolt init\ndolt sql <<\"SQL\"\n")
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("%s\n",
		strings.Replace(
			mtc.base.CreateString(true, false),
			fmt.Sprintf("`%s`", mtc.base.Name),
			fmt.Sprintf("`base_%s`", mtc.final.Name),
			1,
		)))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("%s\n",
		strings.Replace(
			mtc.ours.CreateString(true, false),
			fmt.Sprintf("`%s`", mtc.ours.Name),
			fmt.Sprintf("`our_%s`", mtc.final.Name),
			1,
		)))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("%s\n",
		strings.Replace(
			mtc.theirs.CreateString(true, false),
			fmt.Sprintf("`%s`", mtc.theirs.Name),
			fmt.Sprintf("`their_%s`", mtc.final.Name),
			1,
		)))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("%s\n",
		strings.Replace(
			mtc.final.CreateString(true, false),
			fmt.Sprintf("`%s`", mtc.final.Name),
			fmt.Sprintf("`merged_%s`", mtc.final.Name),
			1,
		)))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(mtc.createConflictsTable())
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString("SQL\n")
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("dolt table import -u base_%s base_%s.csv\n", mtc.final.Name, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("dolt table import -u our_%s our_%s.csv\n", mtc.final.Name, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("dolt table import -u their_%s their_%s.csv\n", mtc.final.Name, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString(fmt.Sprintf("dolt table import -u merged_%s merged_%s.csv\n", mtc.final.Name, mtc.final.Name))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = file.WriteString("dolt table import -u conflicts conflicts.csv\n")
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// createConflictsTable returns a CREATE TABLE string that may be used to import the conflict data.
func (mtc mergeTableWithConflicts) createConflictsTable() string {
	sb1 := strings.Builder{}
	sb1.Grow(512)
	for _, pkCol := range mtc.base.PKCols {
		sb1.WriteString(fmt.Sprintf("base_%s %s, ", pkCol.Name, pkCol.Type.Name(false)))
	}
	for _, nonPkCol := range mtc.base.NonPKCols {
		sb1.WriteString(fmt.Sprintf("base_%s %s, ", nonPkCol.Name, nonPkCol.Type.Name(false)))
	}
	for _, pkCol := range mtc.ours.PKCols {
		sb1.WriteString(fmt.Sprintf("our_%s %s, ", pkCol.Name, pkCol.Type.Name(false)))
	}
	for _, nonPkCol := range mtc.ours.NonPKCols {
		sb1.WriteString(fmt.Sprintf("our_%s %s, ", nonPkCol.Name, nonPkCol.Type.Name(false)))
	}
	for _, pkCol := range mtc.theirs.PKCols {
		sb1.WriteString(fmt.Sprintf("their_%s %s, ", pkCol.Name, pkCol.Type.Name(false)))
	}
	for _, nonPkCol := range mtc.theirs.NonPKCols {
		sb1.WriteString(fmt.Sprintf("their_%s %s, ", nonPkCol.Name, nonPkCol.Type.Name(false)))
	}

	// Remove the last comma and space from the end of the column names
	str := sb1.String()
	return fmt.Sprintf("CREATE TABLE conflicts (%s);\n", str[:len(str)-2])
}

// Verify verifies that the table merged as expected, including checking for conflicts.
func (mtc mergeTableWithConflicts) Verify(c *run.Cycle) error {
	internalCursor, err := mtc.final.Data.GetRowCursor()
	if err != nil {
		return errors.Wrap(err)
	}
	defer internalCursor.Close()
	doltCursor, err := mtc.final.GetDoltCursor(c)
	if err != nil {
		return errors.Wrap(err)
	}
	defer func() {
		_ = doltCursor.Close()
	}()

	var iRow run.Row
	var ok bool
	for iRow, ok, err = internalCursor.NextRow(); ok && err == nil; iRow, ok, err = internalCursor.NextRow() {
		dRow, ok, err := doltCursor.NextRow()
		if !ok {
			return errors.New(fmt.Sprintf("On table `%s`, internal data contains more rows than Dolt", mtc.final.Name))
		}
		if err != nil {
			return errors.Wrap(err)
		}
		if !iRow.Equals(dRow) {
			return errors.New(fmt.Sprintf("On table `%s`, internal data contains [%s]\nDolt contains [%s]",
				mtc.final.Name, iRow.MySQLString(), dRow.MySQLString()))
		}
	}
	if err != nil {
		return errors.Wrap(err)
	}

	_, ok, err = doltCursor.NextRow()
	if ok {
		return errors.New(fmt.Sprintf("On table `%s`, Dolt contains more rows than internal data", mtc.final.Name))
	}
	if err != nil {
		return errors.Wrap(err)
	}
	_ = doltCursor.Close()

	if ok, err = mtc.final.DoltTableHasConflicts(c); err != nil {
		return errors.Wrap(err)
	} else if ok {
		if len(mtc.conflicts) == 0 {
			return errors.New(fmt.Sprintf("On table `%s`, Dolt contains conflicts while internal data does not", mtc.final.Name))
		}
		doltConflictsCursor, err := mtc.final.GetDoltConflictsCursor(c)
		if err != nil {
			return errors.Wrap(err)
		}
		defer func() {
			_ = doltConflictsCursor.Close()
		}()
		conflictIdx := 0
		var dConflictRow run.Row
		for dConflictRow, ok, err = doltConflictsCursor.NextRow(); ok && err == nil; dConflictRow, ok, err = doltConflictsCursor.NextRow() {
			if conflictIdx >= len(mtc.conflicts) {
				return errors.New(fmt.Sprintf("On table `%s`, internal conflicts contain more conflicts than Dolt", mtc.final.Name))
			}
			iConflictRow := mtc.conflicts[conflictIdx]
			conflictIdx++
			if !iConflictRow.Equals(dConflictRow) {
				return errors.New(fmt.Sprintf("On table `%s`, internal conflict contains [%s]\nDolt contains [%s]",
					mtc.final.Name, iConflictRow.MySQLString(), dConflictRow.MySQLString()))
			}
		}
	} else if len(mtc.conflicts) > 0 {
		return errors.New(fmt.Sprintf("On table `%s`, Dolt does not contain conflicts while internal data does", mtc.final.Name))
	}
	return nil
}
