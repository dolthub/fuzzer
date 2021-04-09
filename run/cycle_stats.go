package run

import "time"

// CycleStats are given to hooked functions for additional information. The hooks may modify the values here.
type CycleStats struct {
	// CycleStart is the time that the cycle started at.
	CycleStart time.Time
	// SQLStatementsExecuted is the total number of SQL statements executed.
	SQLStatementsExecuted uint64
	// SQLStatementBatchSize is the batch size for the next run of SQL statements.
	SQLStatementBatchSize uint64
}
