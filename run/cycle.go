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
	"github.com/dolthub/fuzzer/run/connection"
	"github.com/dolthub/fuzzer/utils/file"
	fuzzer_os "github.com/dolthub/fuzzer/utils/os"
)

// Cycle is the orchestrator of a run cycle, which includes the creation of a repository, as well as the execution of
// any commands obtained from the planner.
type Cycle struct {
	Name          string
	Planner       *Planner
	Blueprint     *blueprint.Blueprint
	Logger        Logger
	statementDist *ranges.DistributionCenter
	pkTypeDist    *ranges.DistributionCenter
	nonPkTypeDist *ranges.DistributionCenter
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
	pkTypeDist, err := ranges.NewDistributionCenter(
		&planner.Base.Types.Bigint,
		&planner.Base.Types.BigintUnsigned,
		&planner.Base.Types.Binary,
		&planner.Base.Types.Bit,
		&planner.Base.Types.Char,
		&planner.Base.Types.Date,
		&planner.Base.Types.Datetime,
		&planner.Base.Types.Decimal,
		&planner.Base.Types.Double,
		&planner.Base.Types.Enum,
		&planner.Base.Types.Float,
		&planner.Base.Types.Int,
		&planner.Base.Types.IntUnsigned,
		&planner.Base.Types.Mediumint,
		&planner.Base.Types.MediumintUnsigned,
		&planner.Base.Types.Set,
		&planner.Base.Types.Smallint,
		&planner.Base.Types.SmallintUnsigned,
		&planner.Base.Types.Time,
		&planner.Base.Types.Timestamp,
		&planner.Base.Types.Tinyint,
		&planner.Base.Types.TinyintUnsigned,
		&planner.Base.Types.Varbinary,
		&planner.Base.Types.Varchar,
		&planner.Base.Types.Year,
	)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	nonPkTypeDist, err := ranges.NewDistributionCenter(
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
		usedNames:     map[string]struct{}{"main": {}},
		statementDist: statementDist,
		pkTypeDist:    pkTypeDist,
		nonPkTypeDist: nonPkTypeDist,
		nameRegexes:   nameRegexes,
		currentBranch: 0,
		actionQueue:   make(chan func(*Cycle) error, 300),
		hookQueue:     make(chan Hook, 100),
	}, nil
}

// Run runs the cycle.
func (c *Cycle) Run() (err error) {
	defer func() {
		moveRepo := true
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok {
				err = errors.Wrap(rErr)
			} else {
				err = errors.New(fmt.Sprintf("%+v", r))
			}
		}
		if err != nil {
			now := time.Now()
			since := now.Sub(c.Blueprint.CycleStart)
			_ = c.Logger.WriteLine(LogType_INFO,
				fmt.Sprintf("Cycle finished unsuccessfully: %s (%s)", now.Format("2006-01-02 15:04:05"), since.String()))
			_ = c.Logger.WriteLine(LogType_ERR, fmt.Sprintf("%+v", err))
			_ = c.Logger.Close()
			func() {
				errFile, fileErr := os.OpenFile(c.Planner.Base.Arguments.RepoWorkingPath+c.Name+"/err.txt",
					os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
				if fileErr != nil {
					return
				}
				defer func() {
					_ = errFile.Close()
				}()
				_, _ = errFile.WriteString(fmt.Sprintf("%+v", err))
			}()
			if errors.ShouldIgnore(err) {
				moveRepo = false
				_ = os.Chdir(c.Planner.Base.Arguments.RepoWorkingPath)
				_ = file.RemoveAll(c.Planner.Base.Arguments.RepoWorkingPath + c.Name)
			}
		} else {
			now := time.Now()
			since := now.Sub(c.Blueprint.CycleStart)
			cErr := c.Logger.WriteLine(LogType_INFO,
				fmt.Sprintf("Cycle finished successfully: %s (%s)", now.Format("2006-01-02 15:04:05"), since.String()))
			if cErr != nil {
				err = errors.Wrap(cErr)
			}
			cErr = c.Logger.Close()
			if err == nil && cErr != nil {
				err = errors.Wrap(cErr)
			}
			if c.Planner.Base.Options.DeleteSuccesses {
				moveRepo = false
				cErr := os.Chdir(c.Planner.Base.Arguments.RepoWorkingPath)
				if err == nil && cErr != nil {
					err = errors.Wrap(cErr)
				}
				cErr = file.RemoveAll(c.Planner.Base.Arguments.RepoWorkingPath + c.Name)
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
		if moveRepo && c.Planner.Base.Arguments.RepoWorkingPath != c.Planner.Base.Arguments.RepoFinishedPath {
			_ = os.Chdir(c.Planner.Base.Arguments.RepoWorkingPath)
			rErr := file.Rename(c.Planner.Base.Arguments.RepoWorkingPath+c.Name, c.Planner.Base.Arguments.RepoFinishedPath+c.Name)
			if rErr != nil {
				// If we can't move the finish directory then we should probably panic about it as it's pretty bad.
				if err != nil {
					panic(err.Error() + "\n" + rErr.Error())
				} else {
					panic(rErr)
				}
			}
		}
		cErr := connection.CloseDoltConnections()
		if err == nil && cErr != nil {
			err = errors.Wrap(cErr)
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

// CliQuery is used to run dolt commands on the CLI. Automatically closes any running servers before usage.
func (c *Cycle) CliQuery(args ...string) (string, error) {
	formattedArgs := make([]string, len(args))
	copy(formattedArgs, args)
	for i, arg := range formattedArgs {
		if strings.Contains(arg, " ") {
			formattedArgs[i] = `"` + strings.ReplaceAll(arg, `"`, `\"`) + `"`
		}
	}

	err := c.Logger.WriteLine(LogType_CLI, strings.Join(append([]string{"dolt"}, formattedArgs...), " "))
	if err != nil {
		return "", errors.Wrap(err)
	}
	err = connection.CloseDoltConnections()
	if err != nil {
		return "", errors.Wrap(err)
	}
	stdOutBuffer := &bytes.Buffer{}
	stdErrBuffer := &bytes.Buffer{}
	doltQuery := exec.Command("dolt", args...)
	doltQuery.Env = fuzzer_os.Environ()
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

// SqlServer is used to run SQL statements on the server. If output of a statement is desired, then the connection
// should be manually acquired using GetDoltConnection. This will reuse an existing server connection if one exists.
// Additionally, this will call the pre- and post-SQL execution hooks.
func (c *Cycle) SqlServer(statement string) error {
	if err := c.Planner.Hooks.RunHook(Hook{
		Type:   HookType_SqlStatementPreExecution,
		Cycle:  c,
		Param1: statement,
	}); err != nil {
		return errors.Wrap(err)
	}

	err := c.Logger.WriteLine(LogType_SQLS, statement)
	if err != nil {
		return errors.Wrap(err)
	}
	dc, err := connection.GetDoltConnection(c.Planner.Base.Options.Port, c.Name)
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = dc.Conn.Exec(statement)
	if err != nil {
		return errors.Wrap(err)
	}

	if err = c.Planner.Hooks.RunHook(Hook{
		Type:   HookType_SqlStatementPostExecution,
		Cycle:  c,
		Param1: statement,
	}); err != nil {
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

	cycleDir := c.Planner.Base.Arguments.RepoWorkingPath + dbName
	err = os.Mkdir(cycleDir, os.ModeDir|0777)
	if err != nil {
		return errors.Wrap(err)
	}
	err = os.Chdir(cycleDir)
	if err != nil {
		return errors.Wrap(err)
	}

	if c.Planner.Base.Options.Logging {
		logFile, err := os.OpenFile("./log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			return errors.Wrap(err)
		}
		c.Logger = &fileLogger{logFile}
	}

	err = c.Logger.WriteLine(LogType_INFO, fmt.Sprintf("Cycle started: %s", c.Blueprint.CycleStart.Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}
	_, err = c.CliQuery("init")
	if err != nil {
		return errors.Wrap(err)
	}
	mainBranch, err := NewMainBranch(c)
	if err != nil {
		return errors.Wrap(err)
	}
	c.branches = []*Branch{mainBranch}
	return nil
}
