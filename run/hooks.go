package run

// HookRegistrant represents a service that makes use of hooks.
type HookRegistrant interface {
	// Register assigns any necessary functions to the given hooks.
	Register(hooks *Hooks)
}

// Hooks contains all of the callback functions for each step of a cycle.
type Hooks struct {
	cycleStarted              []func(c *Cycle, stats *CycleStats) error
	cycleEnded                []func(c *Cycle) error
	repositoryFinished        []func(c *Cycle, stats *CycleStats) error
	branchCreated             []func(c *Cycle, stats *CycleStats, branch *Branch) error
	branchSwitched            []func(c *Cycle, stats *CycleStats, prevBranch Branch, branch *Branch) error
	commitCreated             []func(c *Cycle, stats *CycleStats, commit *Commit) error
	tableCreated              []func(c *Cycle, stats *CycleStats, table *Table) error
	indexCreated              []func(c *Cycle, stats *CycleStats, table *Table, index *Index) error
	foreignKeyCreated         []func(c *Cycle, stats *CycleStats, commit *Commit, foreignKey *ForeignKey) error
	sqlStatementBatchStarted  []func(c *Cycle, stats *CycleStats, table *Table) error
	sqlStatementBatchFinished []func(c *Cycle, stats *CycleStats, table *Table) error
	sqlStatementPreExecution  []func(c *Cycle, stats *CycleStats, statement string) error
	sqlStatementPostExecution []func(c *Cycle, stats *CycleStats, statement string) error
}

// CycleStarted is called right after the dolt repository has been initialized.
func (h *Hooks) CycleStarted(f func(c *Cycle, stats *CycleStats) error) {
	h.cycleStarted = append(h.cycleStarted, f)
}

// CycleEnded is called when the cycle has ended. Any close functions should be called here. This is always called when
// a cycle is ended for any reason, including on errors. It is not guaranteed that CycleStarted has been called before
// this hook.
func (h *Hooks) CycleEnded(f func(c *Cycle) error) {
	h.cycleEnded = append(h.cycleEnded, f)
}

// RepositoryFinished is called when the repository has finished writing data to its tables.
func (h *Hooks) RepositoryFinished(f func(c *Cycle, stats *CycleStats) error) {
	h.repositoryFinished = append(h.repositoryFinished, f)
}

// BranchCreated is called when a branch has been created.
func (h *Hooks) BranchCreated(f func(c *Cycle, stats *CycleStats, branch *Branch) error) {
	h.branchCreated = append(h.branchCreated, f)
}

// BranchSwitched is called when the active branch has switched.
func (h *Hooks) BranchSwitched(f func(c *Cycle, stats *CycleStats, prevBranch Branch, branch *Branch) error) {
	h.branchSwitched = append(h.branchSwitched, f)
}

// CommitCreated is called when a commit has been created.
func (h *Hooks) CommitCreated(f func(c *Cycle, stats *CycleStats, commit *Commit) error) {
	h.commitCreated = append(h.commitCreated, f)
}

// TableCreated is called when a table has been created.
func (h *Hooks) TableCreated(f func(c *Cycle, stats *CycleStats, table *Table) error) {
	h.tableCreated = append(h.tableCreated, f)
}

// IndexCreated is called when an index has been created.
func (h *Hooks) IndexCreated(f func(c *Cycle, stats *CycleStats, table *Table, index *Index) error) {
	h.indexCreated = append(h.indexCreated, f)
}

// ForeignKeyCreated is called when a foreign key has been created.
func (h *Hooks) ForeignKeyCreated(f func(c *Cycle, stats *CycleStats, commit *Commit, foreignKey *ForeignKey) error) {
	h.foreignKeyCreated = append(h.foreignKeyCreated, f)
}

// SQLStatementBatchStarted is called when a batch of SQL statements are about to be ran. Modifying the
// SQLStatementBatchSize will alter how many statements are run.
func (h *Hooks) SQLStatementBatchStarted(f func(c *Cycle, stats *CycleStats, table *Table) error) {
	h.sqlStatementBatchStarted = append(h.sqlStatementBatchStarted, f)
}

// SQLStatementBatchFinished is called when a batch of SQL statements have finished executing (without errors).
func (h *Hooks) SQLStatementBatchFinished(f func(c *Cycle, stats *CycleStats, table *Table) error) {
	h.sqlStatementBatchFinished = append(h.sqlStatementBatchFinished, f)
}

// SQLStatementPreExecution is called whenever a SQL statement is about to be executed.
func (h *Hooks) SQLStatementPreExecution(f func(c *Cycle, stats *CycleStats, statement string) error) {
	h.sqlStatementPreExecution = append(h.sqlStatementPreExecution, f)
}

// SQLStatementPostExecution is called after a SQL statement has been executed.
func (h *Hooks) SQLStatementPostExecution(f func(c *Cycle, stats *CycleStats, statement string) error) {
	h.sqlStatementPostExecution = append(h.sqlStatementPostExecution, f)
}
