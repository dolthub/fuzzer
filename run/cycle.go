package run

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/ranges"
)

// Cycle is the orchestrator of a run cycle, which includes the creation of a repository, as well as the execution of
// any commands obtained from the planner.
type Cycle struct {
	Name          string
	planner       *Planner
	statementDist *ranges.DistributionCenter
	interfaceDist *ranges.DistributionCenter
	typeDist      *ranges.DistributionCenter
	nameRegexes   *nameRegexes
	usedNames     map[string]struct{}
	Branches      []*Branch
	curBranch     *Branch
	stats         *CycleStats
	Logger        Logger
}

// newCycle returns a *Cycle.
func newCycle(planner *Planner) (*Cycle, error) {
	nameRegexes, err := newNameRegexes(planner.base)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	statementDist, err := ranges.NewDistributionCenter(
		&InsertStatement{planner.base.StatementDistribution.Insert},
		&ReplaceStatement{planner.base.StatementDistribution.Replace},
		&UpdateStatement{planner.base.StatementDistribution.Update},
		&DeleteStatement{planner.base.StatementDistribution.Delete},
	)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	typeDist, err := ranges.NewDistributionCenter(
		&planner.base.Types.Bigint,
		&planner.base.Types.BigintUnsigned,
		&planner.base.Types.Binary,
		&planner.base.Types.Bit,
		&planner.base.Types.Blob,
		&planner.base.Types.Char,
		&planner.base.Types.Date,
		&planner.base.Types.Datetime,
		&planner.base.Types.Decimal,
		&planner.base.Types.Double,
		&planner.base.Types.Enum,
		&planner.base.Types.Float,
		&planner.base.Types.Int,
		&planner.base.Types.IntUnsigned,
		&planner.base.Types.Longblob,
		&planner.base.Types.Longtext,
		&planner.base.Types.Mediumblob,
		&planner.base.Types.Mediumint,
		&planner.base.Types.MediumintUnsigned,
		&planner.base.Types.Mediumtext,
		&planner.base.Types.Set,
		&planner.base.Types.Smallint,
		&planner.base.Types.SmallintUnsigned,
		&planner.base.Types.Text,
		&planner.base.Types.Time,
		&planner.base.Types.Timestamp,
		&planner.base.Types.Tinyblob,
		&planner.base.Types.Tinyint,
		&planner.base.Types.TinyintUnsigned,
		&planner.base.Types.Tinytext,
		&planner.base.Types.Varbinary,
		&planner.base.Types.Varchar,
		&planner.base.Types.Year,
	)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &Cycle{
		planner:       planner,
		usedNames:     map[string]struct{}{"master": {}},
		statementDist: statementDist,
		typeDist:      typeDist,
		nameRegexes:   nameRegexes,
		Branches:      []*Branch{NewMasterBranch()},
		stats:         &CycleStats{},
		Logger:        &fakeLogger{},
	}, nil
}

// Run runs the cycle.
func (c *Cycle) Run() (err error) {
	defer func() {
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
		}
	}()

	err = c.init()
	if err != nil {
		return errors.Wrap(err)
	}

	defer func() {
		var cErr error
		for i := 0; i < len(c.planner.Hooks.cycleEnded); i++ {
			if eErr := c.planner.Hooks.cycleEnded[i](c); eErr != nil && cErr == nil {
				cErr = errors.Wrap(err)
			}
		}
		if err == nil && cErr != nil {
			err = cErr
		}
	}()
	for i := 0; i < len(c.planner.Hooks.cycleStarted); i++ {
		if err = c.planner.Hooks.cycleStarted[i](c, c.stats); err != nil {
			return errors.Wrap(err)
		}
	}

	if err = c.run(); err != nil {
		return errors.Wrap(err)
	}

	for i := 0; i < len(c.planner.Hooks.repositoryFinished); i++ {
		if err = c.planner.Hooks.repositoryFinished[i](c, c.stats); err != nil {
			return errors.Wrap(err)
		}
	}
	return nil
}

// CliQuery is used to run dolt commands on the CLI.
func (c *Cycle) CliQuery(args ...string) error {
	err := c.Logger.WriteLine(LogType_CLI, strings.Join(append([]string{"dolt"}, args...), " "))
	if err != nil {
		return errors.Wrap(err)
	}
	stdErrBuffer := &bytes.Buffer{}
	doltQuery := exec.Command("dolt", args...)
	doltQuery.Stderr = stdErrBuffer
	err = doltQuery.Run()
	if stdErrBuffer.Len() > 0 {
		return errors.New(stdErrBuffer.String())
	}
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
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

// run is the internal cycle loop.
func (c *Cycle) run() error {
	//TODO: handle multiple table creation, branch creation/switching, and committing
	table, err := c.Branches[0].NewTable(c)
	if err != nil {
		return errors.Wrap(err)
	}
	targetRowCount, err := c.planner.base.Amounts.Rows.RandomValue()
	if err != nil {
		return errors.Wrap(err)
	}
	for {
		consecutiveStatements, err := c.planner.base.InterfaceDistribution.ConsecutiveRange.RandomValue()
		if err != nil {
			return errors.Wrap(err)
		}
		c.stats.SQLStatementBatchSize = uint64(consecutiveStatements)
		for i := 0; i < len(c.planner.Hooks.sqlStatementBatchStarted); i++ {
			if err = c.planner.Hooks.sqlStatementBatchStarted[i](c, c.stats, table); err != nil {
				return errors.Wrap(err)
			}
		}
		consecutiveStatements = int64(c.stats.SQLStatementBatchSize)
		err = c.UseInterface(consecutiveStatements, func(f func(string) error) error {
			for i := int64(0); i < consecutiveStatements; i++ {
				statement, err := c.statementDist.Get(1)
				if err != nil {
					return errors.Wrap(err)
				}
				statementStr, err := statement.(Statement).GenerateStatement(table)
				if err != nil {
					return errors.Wrap(err)
				}

				for j := 0; j < len(c.planner.Hooks.sqlStatementPreExecution); j++ {
					if err = c.planner.Hooks.sqlStatementPreExecution[j](c, c.stats, statementStr); err != nil {
						return errors.Wrap(err)
					}
				}
				if err = f(statementStr); err != nil {
					return errors.Wrap(err)
				}
				for j := 0; j < len(c.planner.Hooks.sqlStatementPostExecution); j++ {
					if err = c.planner.Hooks.sqlStatementPostExecution[j](c, c.stats, statementStr); err != nil {
						return errors.Wrap(err)
					}
				}
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err)
		}
		for i := 0; i < len(c.planner.Hooks.sqlStatementBatchFinished); i++ {
			if err = c.planner.Hooks.sqlStatementBatchFinished[i](c, c.stats, table); err != nil {
				return errors.Wrap(err)
			}
		}
		c.stats.SQLStatementsExecuted += uint64(consecutiveStatements)
		c.stats.SQLStatementBatchSize = 0
		if int64(table.Data.Size()) >= targetRowCount {
			break
		}
	}
	return nil
}

// init creates the initial repository.
func (c *Cycle) init() error {
	var err error
	c.stats.CycleStart = time.Now()
	dbName := c.stats.CycleStart.Format("20060102150405")
	c.Name = dbName

	cycleDir := fmt.Sprintf("%s/%s", c.planner.workingDirectory, dbName)
	err = os.Mkdir(cycleDir, os.ModeDir)
	if err != nil {
		return errors.Wrap(err)
	}
	err = os.Chdir(cycleDir)
	if err != nil {
		return errors.Wrap(err)
	}

	if c.planner.base.Options.Logging {
		logFile, err := os.OpenFile("./log.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return errors.Wrap(err)
		}
		c.Logger = &fileLogger{logFile}
	}

	c.interfaceDist, err = ranges.NewDistributionCenter(
		&CliQuery{c.planner.base.InterfaceDistribution.CLIQuery, c.Logger},
		&CliBatch{c.planner.base.InterfaceDistribution.CLIBatch, c.Logger},
		&SqlServer{
			r:      c.planner.base.InterfaceDistribution.SQLServer,
			port:   c.planner.base.Options.Port,
			dbName: dbName,
			logger: c.Logger,
		},
	)
	if err != nil {
		return errors.Wrap(err)
	}

	err = c.Logger.WriteLine(LogType_INFO, fmt.Sprintf("Cycle started: %s", c.stats.CycleStart.Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}
	err = c.CliQuery("init")
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}
