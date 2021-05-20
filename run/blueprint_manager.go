package run

import "github.com/dolthub/fuzzer/errors"

// BlueprintManager handles the blueprint creation and alteration during a cycle's run.
type BlueprintManager struct{}

var _ HookRegistrant = (*BlueprintManager)(nil)

// Register implements the HookRegistrant interface.
func (m *BlueprintManager) Register(hooks *Hooks) {
	hooks.CycleInitialized(m.InitializeBlueprint)
	hooks.SQLStatementBatchStarted(m.SetBatchSize)
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
	c.Blueprint.TargetRowCount = map[string]map[string]uint64{"master": make(map[string]uint64)}
	return nil
}

// SetBatchSize sets the SQLStatementBatchSize when a batch is started.
func (m *BlueprintManager) SetBatchSize(c *Cycle, table *Table) error {
	consecutiveStatements, err := c.Planner.Base.InterfaceDistribution.ConsecutiveRange.RandomValue()
	if err != nil {
		return errors.Wrap(err)
	}
	c.Blueprint.SQLStatementBatchSize = uint64(consecutiveStatements)
	return nil
}

// NewBranch is run when a new branch has been created.
func (m *BlueprintManager) NewBranch(c *Cycle, branch *Branch) error {
	tablesOnThisBranch := make(map[string]uint64)
	currentBranchName := c.GetCurrentBranch().Name
	var rowCount int64
	var err error
	for tableName := range c.Blueprint.TargetRowCount[currentBranchName] {
		if c.Planner.Base.Options.LowerRowsMasterOnly && currentBranchName != "master" {
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
	if c.Planner.Base.Options.LowerRowsMasterOnly && currentBranchName != "master" {
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
