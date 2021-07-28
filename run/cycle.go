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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dolthub/fuzzer/blueprint"
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/ranges"
)

// Cycle is the orchestrator of a run cycle, which includes the creation of a repository, as well as the execution of
// any commands obtained from the planner.
type Cycle struct {
	Name          string
	Planner       *Planner
	Blueprint     *blueprint.Blueprint
	Logger        Logger
	SqlServer     *SqlServer
	statementDist *ranges.DistributionCenter
	interfaceDist *ranges.DistributionCenter
	typeDist      *ranges.DistributionCenter
	nameRegexes   *nameRegexes
	usedNames     map[string]struct{}
	branches      []*Branch
	currentBranch int
	curBranch     *Branch
	actionQueue   chan func(*Cycle) error
	hookQueue     chan Hook
}

// newCycle returns a *Cycle.
func newCycle(planner *Planner) (*Cycle, error) {
	nameRegexes, err := newNameRegexes(planner.Base)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	statementDist, err := ranges.NewDistributionCenter(
		&InsertStatement{planner.Base.StatementDistribution.Insert},
		&ReplaceStatement{planner.Base.StatementDistribution.Replace},
		&UpdateStatement{planner.Base.StatementDistribution.Update},
		&DeleteStatement{planner.Base.StatementDistribution.Delete},
	)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	typeDist, err := ranges.NewDistributionCenter(
		&planner.Base.Types.Bigint,
		&planner.Base.Types.BigintUnsigned,
		&planner.Base.Types.Binary,
		&planner.Base.Types.Bit,
		&planner.Base.Types.Blob,
		&planner.Base.Types.Char,
		&planner.Base.Types.Date,
		&planner.Base.Types.Datetime,
		&planner.Base.Types.Decimal,
		&planner.Base.Types.Double,
		&planner.Base.Types.Enum,
		&planner.Base.Types.Float,
		&planner.Base.Types.Int,
		&planner.Base.Types.IntUnsigned,
		&planner.Base.Types.Longblob,
		&planner.Base.Types.Longtext,
		&planner.Base.Types.Mediumblob,
		&planner.Base.Types.Mediumint,
		&planner.Base.Types.MediumintUnsigned,
		&planner.Base.Types.Mediumtext,
		&planner.Base.Types.Set,
		&planner.Base.Types.Smallint,
		&planner.Base.Types.SmallintUnsigned,
		&planner.Base.Types.Text,
		&planner.Base.Types.Time,
		&planner.Base.Types.Timestamp,
		&planner.Base.Types.Tinyblob,
		&planner.Base.Types.Tinyint,
		&planner.Base.Types.TinyintUnsigned,
		&planner.Base.Types.Tinytext,
		&planner.Base.Types.Varbinary,
		&planner.Base.Types.Varchar,
		&planner.Base.Types.Year,
	)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &Cycle{
		Planner:       planner,
		Blueprint:     &blueprint.Blueprint{},
		Logger:        &fakeLogger{},
		usedNames:     map[string]struct{}{"master": {}},
		statementDist: statementDist,
		typeDist:      typeDist,
		nameRegexes:   nameRegexes,
		currentBranch: 0,
		actionQueue:   make(chan func(*Cycle) error, 300),
		hookQueue:     make(chan Hook, 100),
	}, nil
}

// Run runs the cycle.
func (c *Cycle) Run() (err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				err = errors.Wrap(rErr)
			} else {
				err = errors.New(fmt.Sprintf("%+v", r))
			}
		}
		if err != nil {
			_ = c.Logger.WriteLine(LogType_INFO,
				fmt.Sprintf("Cycle finished unsuccessfully: %s", time.Now().Format("2006-01-02 15:04:05")))
			_ = c.Logger.WriteLine(LogType_ERR, fmt.Sprintf("%+v", err))
			_ = c.Logger.Close()
		} else {
			cErr := c.Logger.WriteLine(LogType_INFO,
				fmt.Sprintf("Cycle finished successfully: %s", time.Now().Format("2006-01-02 15:04:05")))
			if cErr != nil {
				err = errors.Wrap(cErr)
			}
			cErr = c.Logger.Close()
			if err == nil && cErr != nil {
				err = errors.Wrap(cErr)
			}
			if c.Planner.Base.Options.DeleteSuccesses {
				cErr := os.Chdir(c.Planner.workingDirectory)
				if err == nil && cErr != nil {
					err = errors.Wrap(cErr)
				}
				cErr = os.RemoveAll(fmt.Sprintf("%s/%s", c.Planner.workingDirectory, c.Name))
				if err == nil && cErr != nil {
					err = errors.Wrap(cErr)
				}
			}
		}
		close(c.hookQueue)
		close(c.actionQueue)
		for _, branch := range c.branches {
			for _, commit := range branch.Commits {
				for _, table := range commit.Tables {
					table.Data.Close()
				}
			}
		}
	}()

	err = c.init()
	if err != nil {
		return errors.Wrap(err)
	}

	defer func() {
		if hookErr := c.Planner.Hooks.RunHook(Hook{
			Type:  HookType_CycleEnded,
			Cycle: c,
		}); hookErr != nil && err == nil {
			err = errors.Wrap(hookErr)
		}
	}()
	c.hookQueue <- Hook{
		Type:  HookType_CycleInitialized,
		Cycle: c,
	}
	c.hookQueue <- Hook{
		Type:  HookType_CycleStarted,
		Cycle: c,
	}

	for breakOuter := false; !breakOuter; {
		select {
		case action := <-c.actionQueue:
			err = action(c)
			if err != nil {
				return errors.Wrap(err)
			}
		default:
			breakOuter = true
		}
		for breakInner := false; !breakInner; {
			select {
			case hook := <-c.hookQueue:
				breakOuter = false
				err = c.Planner.Hooks.RunHook(hook)
				if err != nil {
					return errors.Wrap(err)
				}
			default:
				breakInner = true
			}
		}
	}
	return nil
}

// GetBranchNames returns all of the branch names.
func (c *Cycle) GetBranchNames() []string {
	branchNames := make([]string, len(c.branches))
	for i := 0; i < len(branchNames); i++ {
		branchNames[i] = c.branches[i].Name
	}
	return branchNames
}

// GetCurrentBranch returns the current branch that Dolt is on.
func (c *Cycle) GetCurrentBranch() *Branch {
	return c.branches[c.currentBranch]
}

// SwitchCurrentBranch switches the current branch to the given branch.
func (c *Cycle) SwitchCurrentBranch(targetBranch string) error {
	prevBranch := c.GetCurrentBranch()
	if targetBranch == prevBranch.Name {
		return nil
	}
	_, err := prevBranch.Commit(c, false)
	if err != nil {
		return errors.Wrap(err)
	}
	for i := range c.branches {
		if c.branches[i].Name == targetBranch {
			if err != nil {
				return errors.Wrap(err)
			}
			currentBranch := c.branches[i]
			c.currentBranch = i
			_, err = c.CliQuery("checkout", currentBranch.Name)
			if err != nil {
				return errors.Wrap(err)
			}
			c.hookQueue <- Hook{
				Type:   HookType_BranchSwitched,
				Cycle:  c,
				Param1: prevBranch,
				Param2: currentBranch,
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("could not find a branch with the name '%s' to switch to", targetBranch))
}

// GetBranch returns the Branch with the given name. If the branch does not exist, returns nil.
func (c *Cycle) GetBranch(branchName string) *Branch {
	for _, branch := range c.branches {
		if branch.Name == branchName {
			return branch
		}
	}
	return nil
}

// QueueAction queues the given action.
func (c *Cycle) QueueAction(f func(*Cycle) error) {
	c.actionQueue <- f
}

// CliQuery is used to run dolt commands on the CLI.
func (c *Cycle) CliQuery(args ...string) (string, error) {
	err := c.Logger.WriteLine(LogType_CLI, strings.Join(append([]string{"dolt"}, args...), " "))
	if err != nil {
		return "", errors.Wrap(err)
	}
	stdOutBuffer := &bytes.Buffer{}
	stdErrBuffer := &bytes.Buffer{}
	doltQuery := exec.Command("dolt", args...)
	doltQuery.Stdout = stdOutBuffer
	doltQuery.Stderr = stdErrBuffer
	err = doltQuery.Run()
	if stdErrBuffer.Len() > 0 {
		return "", errors.New(stdErrBuffer.String())
	}
	if err != nil {
		return "", errors.Wrap(err)
	}
	return strings.TrimSpace(stdOutBuffer.String()), nil
}

// UseInterface is used to run SQL queries. The cycle automatically manages which interface will be used (whether that
// be the CLI query argument, batch mode, or the server). If the number of calls is known beforehand, then it allows
// for the cycle to properly balance the distribution of used interfaces based on the number of commands executed.
func (c *Cycle) UseInterface(expectedCalls int64, caller func(func(string) error) error) error {
	chosenInterface, err := c.interfaceDist.Get(float64(expectedCalls))
	if err != nil {
		return errors.Wrap(err)
	}
	err = chosenInterface.(Interface).ProvideInterface(caller)
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// init creates the initial repository.
func (c *Cycle) init() error {
	var err error
	c.Blueprint.CycleStart = time.Now()
	if c.Blueprint.CycleStart.Add(-time.Second).Before(c.Planner.lastRunStartTime) {
		time.Sleep(time.Second)
		c.Blueprint.CycleStart = time.Now()
	}
	c.Planner.lastRunStartTime = c.Blueprint.CycleStart
	dbName := c.Blueprint.CycleStart.Format("20060102150405")
	c.Name = dbName

	cycleDir := fmt.Sprintf("%s/%s", c.Planner.workingDirectory, dbName)
	err = os.Mkdir(cycleDir, os.ModeDir)
	if err != nil {
		return errors.Wrap(err)
	}
	err = os.Chdir(cycleDir)
	if err != nil {
		return errors.Wrap(err)
	}

	if c.Planner.Base.Options.Logging {
		logFile, err := os.OpenFile("./log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return errors.Wrap(err)
		}
		c.Logger = &fileLogger{logFile}
	}

	sqlServer := &SqlServer{
		r:      c.Planner.Base.InterfaceDistribution.SQLServer,
		port:   c.Planner.Base.Options.Port,
		dbName: dbName,
		logger: c.Logger,
	}
	c.interfaceDist, err = ranges.NewDistributionCenter(
		&CliQuery{c.Planner.Base.InterfaceDistribution.CLIQuery, c.Logger},
		&CliBatch{c.Planner.Base.InterfaceDistribution.CLIBatch, c.Logger},
		sqlServer,
	)
	if err != nil {
		return errors.Wrap(err)
	}
	c.SqlServer = sqlServer

	err = c.Logger.WriteLine(LogType_INFO, fmt.Sprintf("Cycle started: %s", c.Blueprint.CycleStart.Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = c.CliQuery("init")
	if err != nil {
		return errors.Wrap(err)
	}
	masterBranch, err := NewMasterBranch(c)
	if err != nil {
		return errors.Wrap(err)
	}
	c.branches = []*Branch{masterBranch}
	return nil
}
