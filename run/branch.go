package run

import (
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/types"
)

// Branch represents a dolt branch.
type Branch struct {
	Name    string
	Commits []*Commit
}

// NewMasterBranch returns a new master branch. Does not execute any commands, as the master branch is automatically
// created on repo initialization.
func NewMasterBranch() *Branch {
	return &Branch{
		Name:    "master",
		Commits: []*Commit{{}},
	}
}

// NewBranch returns a new branch. Just as in dolt, the new branch is created based on the contents of the branch it is
// branching from.
func (b *Branch) NewBranch(c *Cycle) (*Branch, error) {
	var branchName string
	var err error
	for i := 0; i <= 10000000; i++ {
		branchName, err = rand.StringExtendedAlphanumeric(10)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		if _, ok := c.usedNames[branchName]; !ok && !c.nameRegexes.Branches.MatchString(branchName) {
			break
		}
		if i == 10000000 {
			return nil, errors.New("10 million consecutive failed regexes on branch name, aborting cycle")
		}
	}
	c.usedNames[branchName] = struct{}{}

	err = c.CliQuery("branch", branchName)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	commits := make([]*Commit, len(b.Commits))
	for i := 0; i < len(b.Commits); i++ {
		commits[i], err = b.Commits[i].Copy()
		if err != nil {
			return nil, errors.Wrap(err)
		}
	}
	return &Branch{
		Name:    branchName,
		Commits: commits,
	}, nil
}

// NewTable creates a new random table on the branch.
func (b *Branch) NewTable(c *Cycle) (*Table, error) {
	var tableName string
	var err error
	for i := 0; i <= 10000000; i++ {
		tableName, err = rand.StringExtendedAlphanumeric(10)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		if _, ok := c.usedNames[tableName]; !ok && !c.nameRegexes.Tables.MatchString(tableName) {
			break
		}
		if i == 10000000 {
			return nil, errors.New("10 million consecutive failed regexes on table name, aborting cycle")
		}
	}
	c.usedNames[tableName] = struct{}{}

	parent := b.Commits[len(b.Commits)-1]
	totalCols, err := c.planner.base.Amounts.Columns.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	pkCount, err := c.planner.base.Amounts.PrimaryKeys.RandomValueRestrictUpper(totalCols)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	pkCols := make([]*Column, pkCount)
	nonPkCols := make([]*Column, totalCols-pkCount)

	for pkIter := 0; pkIter <= 100; pkIter++ {
		valueCombinations := float64(1)
		for i := 0; i < len(pkCols); i++ {
			fullType, err := c.typeDist.Get(1)
			if err != nil {
				return nil, errors.Wrap(err)
			}
			typeInstance, err := fullType.(types.Type).Instance()
			if err != nil {
				return nil, errors.Wrap(err)
			}
			var colName string
			for j := 0; j <= 10000000; j++ {
				colName, err = rand.StringExtendedAlphanumeric(6)
				if err != nil {
					return nil, errors.Wrap(err)
				}
				if _, ok := c.usedNames[colName]; !ok && !c.nameRegexes.Columns.MatchString(colName) {
					break
				}
				if j == 10000000 {
					return nil, errors.New("10 million consecutive failed regexes on column name, aborting cycle")
				}
			}
			valueCombinations *= typeInstance.MaxValueCount()
			pkCols[i] = &Column{
				Name: colName,
				Type: typeInstance,
			}
		}
		// The divisor controls the relative saturation of the primary key's range. The higher the number, the lower
		// the max saturation, meaning it is quicker to generate a random key that does not already exist.
		if (valueCombinations / 3) > float64(c.planner.base.Amounts.Rows.Upperbound) {
			for i := 0; i < len(pkCols); i++ {
				c.usedNames[pkCols[i].Name] = struct{}{}
			}
			break
		}
		if pkIter == 100 {
			return nil, errors.New("100 consecutive failed attempts at primary keys conforming to the desired row count, aborting cycle")
		}
	}

	for i := 0; i < len(nonPkCols); i++ {
		fullType, err := c.typeDist.Get(1)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		typeInstance, err := fullType.(types.Type).Instance()
		if err != nil {
			return nil, errors.Wrap(err)
		}
		var colName string
		for j := 0; j <= 10000000; j++ {
			colName, err = rand.StringExtendedAlphanumeric(6)
			if err != nil {
				return nil, errors.Wrap(err)
			}
			if _, ok := c.usedNames[colName]; !ok && !c.nameRegexes.Columns.MatchString(colName) {
				break
			}
			if j == 10000000 {
				return nil, errors.New("10 million consecutive failed regexes on column name, aborting cycle")
			}
		}
		c.usedNames[colName] = struct{}{}
		nonPkCols[i] = &Column{
			Name: colName,
			Type: typeInstance,
		}
	}
	table, err := NewTable(parent, tableName, pkCols, nonPkCols, nil)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return table, c.UseInterface(1, func(f func(string) error) error {
		return f(table.CreateString(false))
	})
}
