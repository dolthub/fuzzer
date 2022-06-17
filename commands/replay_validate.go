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

package commands

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/types"
	"github.com/dolthub/fuzzer/utils/argparser"
	"github.com/dolthub/fuzzer/utils/cli"
)

const (
	logfileParam     = "logfile"
	dataParam        = "data"
	replayBufferSize = 64 * 1024 * 1024
)

// ReplayValidate takes a log file and a folder location containing internal data that was output from a previous
// failure, and replays the log file to check if the operations now produce the expected result when compared to the
// internal data.
type ReplayValidate struct {
	logfileLocation      string
	internalDataLocation string
	logfileOsFile        *os.File
	logfileScanner       *bufio.Scanner
}

var _ Command = (*ReplayValidate)(nil)

// init adds the command to the map.
func init() {
	addCommand(&ReplayValidate{})
}

// Register implements the interface Command.
func (rv *ReplayValidate) Register(hooks *run.Hooks) {
	hooks.CycleInitialized(rv.Reset)
	hooks.CycleStarted(rv.CycleStarted)
}

// Name implements the interface Command.
func (rv *ReplayValidate) Name() string {
	return "replay-validate"
}

// Description implements the interface Command.
func (rv *ReplayValidate) Description() string {
	return "Replays a log file to validate data."
}

// ParseArgs implements the interface Command.
func (rv *ReplayValidate) ParseArgs(commandStr string, ap *argparser.ArgParser, args []string) error {
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{
		ShortDesc: "Replays a log file to validate data.",
		LongDesc: `This command takes a log file and a folder location containing internal data that was output from a
previous failure, and replays the log file to check if the operations now produce the expected result when compared to
the internal data.`,
		Synopsis: nil,
	}, ap))
	ap.SupportsString(logfileParam, "", "location", "The log file to read.")
	ap.SupportsString(dataParam, "", "location",
		"The folder containing the internal data that was previously output.")
	apr := cli.ParseArgsOrDie(ap, args, help)
	if readParam, ok := apr.GetValue(logfileParam); ok {
		readParam = strings.ReplaceAll(readParam, `\`, `/`)
		rv.logfileLocation = readParam
	} else {
		return errors.New(fmt.Sprintf("The '%s' parameter is required to use the '%s' command", logfileParam, rv.Name()))
	}
	if readParam, ok := apr.GetValue(dataParam); ok {
		readParam = strings.ReplaceAll(readParam, `\`, `/`)
		rv.internalDataLocation = readParam
	} else {
		return errors.New(fmt.Sprintf("The '%s' parameter is required to use the '%s' command", dataParam, rv.Name()))
	}
	return nil
}

// AdjustConfig implements the interface Command.
func (rv *ReplayValidate) AdjustConfig(config *parameters.Base) error {
	config.Arguments.NumOfCycles = 1
	return nil
}

// Reset resets the state of our command.
func (rv *ReplayValidate) Reset(c *run.Cycle) error {
	c.Planner.Base.Arguments.DontGenRandomData = true
	c.Planner.Base.Arguments.NumOfCycles = 1
	return nil
}

// CycleStarted creates the logfile stream and starts the MainLoop.
func (rv *ReplayValidate) CycleStarted(c *run.Cycle) error {
	logfileOsFile, err := os.OpenFile(rv.logfileLocation, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return errors.Wrap(err)
	}
	rv.logfileOsFile = logfileOsFile
	rv.logfileScanner = bufio.NewScanner(logfileOsFile)
	rv.logfileScanner.Buffer(make([]byte, 0, replayBufferSize), replayBufferSize)
	c.QueueAction(rv.MainLoop)
	return nil
}

// MainLoop reads each line from the stream.
func (rv *ReplayValidate) MainLoop(c *run.Cycle) error {
	if !rv.logfileScanner.Scan() {
		return rv.Validate(c)
	}
	line := rv.logfileScanner.Text()
	linePrefix := line[:6]
	lineContents := line[6:]

	switch linePrefix {
	case "INFO: ", "WARN: ":
		break
	case "CLI:  ":
		if lineContents == "dolt init" {
			break
		}
		args := strings.Split(lineContents[5:], " ")
		_, err := c.CliQuery(args...)
		if err != nil {
			return errors.Wrap(err)
		}
	case "SQLS: ", "SQLQ: ", "SQLB: ":
		if strings.HasPrefix(lineContents, "CREATE TABLE ") {
			sqlNode, err := parse.Parse(sql.NewEmptyContext(), lineContents)
			if err != nil {
				return errors.Wrap(err)
			}
			planCreateTable := sqlNode.(*plan.CreateTable)
			tPKCols, tNonPKCols, err := types.ConvertGMSSchemaToFuzzerSchema(planCreateTable.Schema())
			if err != nil {
				return errors.Wrap(err)
			}

			pkCols := make([]*run.Column, len(tPKCols))
			nonPKCols := make([]*run.Column, len(tNonPKCols))
			for i, tCol := range tPKCols {
				pkCols[i] = &run.Column{
					Name: tCol.Name,
					Type: tCol.Type,
				}
			}
			for i, tCol := range tNonPKCols {
				nonPKCols[i] = &run.Column{
					Name: tCol.Name,
					Type: tCol.Type,
				}
			}
			workingSet := c.GetCurrentBranch().GetWorkingSet()
			tbl, err := run.NewTable(c.GetCurrentBranch().GetWorkingSet(), planCreateTable.Name(), pkCols, nonPKCols, nil)
			if err != nil {
				return errors.Wrap(err)
			}
			workingSet.Tables = append(workingSet.Tables, tbl)
		}
		if err := c.SqlServer(lineContents); err != nil {
			return errors.Wrap(err)
		}
	case "ERR:  ":
		// Consume the rest of the input once an error has been found
		for rv.logfileScanner.Scan() {
		}
	default:
		return errors.New(fmt.Sprintf("Unhandled log prefix: '%s'", strings.TrimSpace(linePrefix)))
	}

	c.QueueAction(rv.MainLoop)
	return nil
}

// Validate will validate all the rows for each table based on the internal data.
func (rv *ReplayValidate) Validate(c *run.Cycle) error {
	err := c.Logger.WriteLine(run.LogType_INFO,
		fmt.Sprintf("Validating Data: %s", time.Now().Format("2006-01-02 15:04:05")))
	if err != nil {
		return errors.Wrap(err)
	}
	internalDataEntries, err := os.ReadDir(rv.internalDataLocation)
	if err != nil {
		return errors.Wrap(err)
	}

	fileBuffer := make([]byte, 0, replayBufferSize)
	for _, entry := range internalDataEntries {
		err := (func() error {
			dataFile, err := os.OpenFile(rv.internalDataLocation+"/"+entry.Name(), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0777)
			if err != nil {
				return errors.Wrap(err)
			}
			defer func() {
				_ = dataFile.Close()
			}()
			dataScanner := bufio.NewScanner(dataFile)
			dataScanner.Buffer(fileBuffer, replayBufferSize)
			_ = dataScanner.Scan() // First line will always contain the column names, so we skip it

			tableName := entry.Name()[:len(entry.Name())-4]
			table := c.GetCurrentBranch().GetWorkingSet().GetTable(tableName)
			if table == nil {
				return errors.New(fmt.Sprintf("Table could not be found internally: '%s'", tableName))
			}
			doltCursor, err := table.GetDoltCursor(c)
			if err != nil {
				return errors.Wrap(err)
			}
			defer func() {
				_ = doltCursor.Close()
			}()

			for dataScanner.Scan() {
				csvRow := dataScanner.Text()
				dRow, ok, err := doltCursor.NextRow()
				if err != nil {
					return errors.Wrap(err)
				}
				if !ok {
					return errors.New(fmt.Sprintf("On table `%s`, internal data contains more rows than Dolt", table.Name))
				}
				dRowCSVString := dRow.CSVString()
				if csvRow != dRowCSVString {
					return errors.New(fmt.Sprintf("On table `%s`, internal data contains [%s]\nDolt contains [%s]",
						table.Name, csvRow, dRowCSVString))
				}
			}
			return nil
		})()
		if err != nil {
			return errors.Wrap(err)
		}
	}

	if err = c.Planner.Hooks.RunHook(run.Hook{
		Type:  run.HookType_RepositoryFinished,
		Cycle: c,
	}); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// CycleEnded ensures that all open streams are closed.
func (rv *ReplayValidate) CycleEnded(c *run.Cycle) error {
	if rv.logfileOsFile != nil {
		err := rv.logfileOsFile.Close()
		if err != nil {
			return errors.Wrap(err)
		}
	}
	rv.logfileOsFile = nil
	rv.logfileScanner = nil
	return nil
}
