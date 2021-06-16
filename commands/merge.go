package commands

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/types"
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
	table     *run.Table
	conflicts []run.Row
}

var _ run.HookRegistrant = (*Merge)(nil)

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
	//TODO: cycle through all combinations of branches and add them to the map (including the reverse pair)
	branches := c.GetBranchNames()
	m.mergeCombinations[mergeCombination{
		ours:   branches[0],
		theirs: branches[1],
	}] = false
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
	_, err = c.CliQuery("checkout", "-b", combination.UniqueBranchName())
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
			return errors.Wrap(err)
		}
	}
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
			table:     mt.final,
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
				switch ourRow.Compare(theirRow) {
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
		table:     final,
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

// Verify verifies that the table merged as expected, including checking for conflicts.
func (mtc mergeTableWithConflicts) Verify(c *run.Cycle) error {
	internalCursor, err := mtc.table.Data.GetRowCursor()
	if err != nil {
		return errors.Wrap(err)
	}
	defer internalCursor.Close()
	doltCursor, err := mtc.table.GetDoltCursor(c)
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
			return errors.New(fmt.Sprintf("On table `%s`, internal data contains more rows than Dolt", mtc.table.Name))
		}
		if err != nil {
			return errors.Wrap(err)
		}
		if !iRow.Equals(dRow) {
			return errors.New(fmt.Sprintf("On table `%s`, internal data contains [%s]\nDolt contains [%s]",
				mtc.table.Name, iRow.MySQLString(), dRow.MySQLString()))
		}
	}
	if err != nil {
		return errors.Wrap(err)
	}

	_, ok, err = doltCursor.NextRow()
	if ok {
		return errors.New(fmt.Sprintf("On table `%s`, Dolt contains more rows than internal data", mtc.table.Name))
	}
	if err != nil {
		return errors.Wrap(err)
	}

	if ok, err = mtc.table.DoltTableHasConflicts(c); err != nil {
		return errors.Wrap(err)
	} else if ok {
		if len(mtc.conflicts) == 0 {
			return errors.New(fmt.Sprintf("On table `%s`, Dolt contains conflicts while internal data does not", mtc.table.Name))
		}
		doltConflictsCursor, err := mtc.table.GetDoltConflictsCursor(c)
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
				return errors.New(fmt.Sprintf("On table `%s`, internal conflicts contain more conflicts than Dolt", mtc.table.Name))
			}
			iConflictRow := mtc.conflicts[conflictIdx]
			conflictIdx++
			if !iConflictRow.Equals(dConflictRow) {
				return errors.New(fmt.Sprintf("On table `%s`, internal conflict contains [%s]\nDolt contains [%s]",
					mtc.table.Name, iConflictRow.MySQLString(), dConflictRow.MySQLString()))
			}
		}
	} else if len(mtc.conflicts) > 0 {
		return errors.New(fmt.Sprintf("On table `%s`, Dolt does not contain conflicts while internal data does", mtc.table.Name))
	}
	return nil
}
