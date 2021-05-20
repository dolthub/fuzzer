package run

import (
	"fmt"
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/types"
)

// Branch represents a dolt branch.
type Branch struct {
	Name    string
	Commits []*Commit
}

// NewMasterBranch returns a new master branch. Fetches the hash of the initial commit, as the master branch is
// automatically created on repo initialization, along with an "Initialize data repository" commit.
func NewMasterBranch(c *Cycle) (*Branch, error) {
	result, err := c.CliQuery("log", "-n", "1")
	if err != nil {
		return nil, errors.Wrap(err)
	}
	hashIdx := strings.Index(result, "commit ")
	hash := result[hashIdx+7 : hashIdx+39]

	initialCommit := &Commit{
		Hash:        hash,
		Parents:     nil,
		Tables:      nil,
		ForeignKeys: nil,
	}
	workingSet := &Commit{
		Hash:        "",
		Parents:     []*Commit{initialCommit},
		Tables:      nil,
		ForeignKeys: nil,
	}
	return &Branch{
		Name:    "master",
		Commits: []*Commit{initialCommit, workingSet},
	}, nil
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

	_, err = c.CliQuery("branch", branchName)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	commits := make([]*Commit, len(b.Commits))
	for i := 0; i < len(b.Commits); i++ {
		commits[i] = b.Commits[i]
	}
	// The last commit is the working set, so the new branch needs its own working set
	commits[len(commits)-1], err = b.Commits[len(b.Commits)-1].Copy()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	branch := &Branch{
		Name:    branchName,
		Commits: commits,
	}
	c.branches = append(c.branches, branch)
	c.hookQueue <- Hook{
		Type:   HookType_BranchCreated,
		Cycle:  c,
		Param1: branch,
	}
	return branch, nil
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

	parent := b.GetWorkingSet()
	totalCols, err := c.Planner.Base.Amounts.Columns.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	pkCount, err := c.Planner.Base.Amounts.PrimaryKeys.RandomValueRestrictUpper(totalCols)
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
		if (valueCombinations / 3) > float64(c.Planner.Base.Amounts.Rows.Upperbound) {
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
	parent.Tables = append(parent.Tables, table)
	c.hookQueue <- Hook{
		Type:   HookType_TableCreated,
		Cycle:  c,
		Param1: table,
	}
	return table, c.UseInterface(1, func(f func(string) error) error {
		return f(table.CreateString(false))
	})
}

// Commit adds all of the changes from this branch to the staged set, and then commits those.
func (b *Branch) Commit(c *Cycle, verifyCurrentBranch bool) (*Commit, error) {
	if verifyCurrentBranch {
		currentBranchName, err := c.CliQuery("branch", "--show-current")
		if err != nil {
			return nil, errors.Wrap(err)
		}
		if b.Name != currentBranchName {
			return nil, errors.New(fmt.Sprintf("cannot commit branch '%s' when on branch '%s'", b.Name, currentBranchName))
		}
	}
	workingSet := b.GetWorkingSet()
	repoStatus, err := c.CliQuery("status")
	if err != nil {
		return nil, errors.Wrap(err)
	}
	if strings.Contains(repoStatus, "nothing to commit") {
		return workingSet, nil
	}
	_, err = c.CliQuery("add", "-A")
	if err != nil {
		return nil, errors.Wrap(err)
	}
	result, err := c.CliQuery("commit", "-m", "COMMITTED")
	if err != nil {
		return nil, errors.Wrap(err)
	}
	hashIdx := strings.Index(result, "commit ")
	hash := result[hashIdx+7 : hashIdx+39]

	newWorkingSet, err := workingSet.Copy()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	newWorkingSet.Hash = ""
	workingSet.Hash = hash
	newWorkingSet.Parents = []*Commit{workingSet}
	b.Commits = append(b.Commits, newWorkingSet)
	c.hookQueue <- Hook{
		Type:   HookType_CommitCreated,
		Cycle:  c,
		Param1: workingSet,
	}
	return newWorkingSet, nil
}

// GetWorkingSet returns the working set of this branch.
func (b *Branch) GetWorkingSet() *Commit {
	return b.Commits[len(b.Commits)-1]
}
