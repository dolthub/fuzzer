package blueprint

import "time"

// Blueprint is used by hooked functions to describe the exact plans of this run. A Hook may modify any of the values here.
type Blueprint struct {
	// CycleStart is the time that the cycle started at.
	CycleStart time.Time
	// BranchCount is the number of branches that will be created for this repository.
	BranchCount uint64
	// TableCount is the number of tables that will be created on the master branch for this repository.
	TableCount uint64
	// TargetRowCount is the number of rows that each table of each branch is targeting. The first map is the branch name,
	// while the second map is the table name.
	TargetRowCount map[string]map[string]uint64
	// SQLStatementsExecuted is the total number of SQL statements executed.
	SQLStatementsExecuted uint64
	// SQLStatementBatchSize is the batch size for the next run of SQL statements.
	SQLStatementBatchSize uint64
}
