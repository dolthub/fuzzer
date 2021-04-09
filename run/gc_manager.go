package run

import "github.com/dolthub/fuzzer/errors"

// GCManager handles running GC commands throughout the cycle.
type GCManager struct {
	statementsSinceLastGC uint64
}

var _ HookRegistrant = (*GCManager)(nil)

// Register implements the HookRegistrant interface.
func (m *GCManager) Register(hooks *Hooks) {
	hooks.SQLStatementBatchFinished(m.Counter)
	hooks.RepositoryFinished(m.Finish)
}

// Counter counts the number of statements ran, and runs GC once a threshold has been crossed.
func (m *GCManager) Counter(c *Cycle, stats *CycleStats, table *Table) error {
	m.statementsSinceLastGC += stats.SQLStatementBatchSize
	if m.statementsSinceLastGC > 150 {
		m.statementsSinceLastGC = 0
		if err := c.CliQuery("gc"); err != nil {
			return errors.Wrap(err)
		}
	}
	return nil
}

// Finish runs GC once all of the data has been written to the repository.
func (m *GCManager) Finish(c *Cycle, stats *CycleStats) error {
	if err := c.CliQuery("gc"); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
