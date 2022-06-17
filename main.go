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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dolthub/fuzzer/commands"
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/utils/argparser"
	"github.com/dolthub/fuzzer/utils/cli"
)

const (
	configPathParam   = "config"
	cyclesParam       = "cycles"
	firstErrorParam   = "first-error"
	metricsPathParam  = "metrics"
	repoDonePathParam = "repo-finished"
	repoWorkPathParam = "repo-working"
	timeoutParam      = "timeout"
)

func main() {
	ap, apr := getArgParser()
	args := apr.Args()
	if len(args) < 1 {
		usageFunc()()
		os.Exit(0)
	}

	var cmd commands.Command
	var ok bool
	if cmd, ok = commands.Commands[strings.ToLower(args[0])]; !ok {
		cli.PrintErrf("error: unknown command `%v`\n", args[0])
		usageFunc()()
		os.Exit(1)
	}
	err := cmd.ParseArgs("fuzzer "+cmd.Name(), ap, os.Args[1:])
	if err != nil {
		cli.PrintErrln("%v", err)
		os.Exit(1)
	}

	configPath := "./config.toml"
	if readParam, ok := apr.GetValue(configPathParam); ok {
		configPath = strings.ReplaceAll(readParam, `\`, `/`)
	}
	base, err := parameters.LoadFromFile(configPath)
	if err != nil {
		cli.PrintErrln("%v", err)
		os.Exit(1)
	}
	planner, err := run.NewPlanner(base)
	if err != nil {
		cli.PrintErrln("%v", err)
		os.Exit(1)
	}
	cmd.Register(planner.Hooks)

	base.Arguments.ConfigPath = configPath
	base.Arguments.NumOfCycles = -1
	if readParam, ok := apr.GetInt(cyclesParam); ok {
		base.Arguments.NumOfCycles = int64(readParam)
	}
	base.Arguments.Timeout = 0
	if readParam, ok := apr.GetValue(timeoutParam); ok {
		base.Arguments.Timeout, err = time.ParseDuration(readParam)
		if err != nil {
			cli.PrintErrf("%+v\n", err)
			os.Exit(1)
		}
	}
	base.Arguments.FirstError = apr.Contains(firstErrorParam)
	base.Arguments.RepoWorkingPath = "./"
	if readParam, ok := apr.GetValue(repoWorkPathParam); ok {
		readParam = strings.ReplaceAll(readParam, `\`, `/`)
		base.Arguments.RepoWorkingPath = readParam
	}
	base.Arguments.RepoFinishedPath = base.Arguments.RepoWorkingPath
	if readParam, ok := apr.GetValue(repoDonePathParam); ok {
		readParam = strings.ReplaceAll(readParam, `\`, `/`)
		base.Arguments.RepoFinishedPath = readParam
	}
	base.Arguments.MetricsPath = ""
	if readParam, ok := apr.GetValue(metricsPathParam); ok {
		readParam = strings.ReplaceAll(readParam, `\`, `/`)
		base.Arguments.MetricsPath = readParam
	}

	createFolder(base.Arguments.RepoWorkingPath)
	base.Arguments.RepoWorkingPath = expandPath(base.Arguments.RepoWorkingPath)
	createFolder(base.Arguments.RepoFinishedPath)
	base.Arguments.RepoFinishedPath = expandPath(base.Arguments.RepoFinishedPath)
	if base.Arguments.MetricsPath != "" {
		createFolder(base.Arguments.MetricsPath)
		base.Arguments.MetricsPath = expandPath(base.Arguments.MetricsPath)
	}
	err = cmd.AdjustConfig(base)
	if err != nil {
		cli.PrintErrln("%v", err)
		os.Exit(1)
	}

	i := int64(0)
	cycleCount := int64(0)
	failures := int64(0)
	startTime := time.Now()
	for ; (base.Arguments.NumOfCycles < 0 && time.Since(startTime) < base.Arguments.Timeout) || i < base.Arguments.NumOfCycles; i++ {
		cycle, err := planner.NewCycle()
		if err != nil {
			cli.PrintErrf("%+v\n", err)
			if base.Arguments.FirstError {
				break
			}
		}
		cycleCount++
		func() {
			defer func() {
				// If a panic slips through the cycle then we should just exit
				if r := recover(); r != nil {
					base.Arguments.NumOfCycles = 1
					failures++
					cli.PrintErrf(fmt.Sprintf("%+v", r))
				}
			}()
			if err = cycle.Run(); err != nil {
				// If we're ignoring this cycle, then we should undo the cycle count and progress towards num of cycles run.
				if errors.ShouldIgnore(err) {
					cycleCount--
					i--
				} else {
					cli.PrintErrf("%+v\n", err)
					failures++
					if base.Arguments.FirstError {
						base.Arguments.NumOfCycles = 1
					}
				}
			}
		}()
	}
	if base.Arguments.MetricsPath != "" {
		metricsFile, err := os.OpenFile(fmt.Sprintf("%s%s.txt", base.Arguments.MetricsPath, time.Now().Format("20060102150405")),
			os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		if err != nil {
			cli.PrintErrf("%+v\n", err)
			os.Exit(1)
		}
		defer func() {
			_ = metricsFile.Close()
		}()
		_, err = metricsFile.WriteString(fmt.Sprintf(`{"Runs":%d,"Successful":%d,"Failed":%d}`,
			cycleCount, cycleCount-failures, failures))
		if err != nil {
			cli.PrintErrf("%+v\n", err)
			os.Exit(1)
		}
	}
}

func getArgParser() (*argparser.ArgParser, *argparser.ArgParseResults) {
	ap := argparser.NewArgParser()
	ap.SupportsString(configPathParam, "", "location", "Specifies a custom location for the config file.")
	ap.SupportsString(cyclesParam, "", "count", "Controls how many cycles are run. Assuming no timeout, if absent or negative then runs forever.")
	ap.SupportsString(timeoutParam, "", "duration",
		`Stops starting new cycles once the timeout has been reached. The specified cycle count overrides this parameter.
Uses time.ParseDuration, so refer to Go's documentation on allowed strings: https://pkg.go.dev/time#ParseDuration`)
	ap.SupportsFlag(firstErrorParam, "", "If specified, immediately stops the fuzzer when the first error is encountered.")
	ap.SupportsString(repoDonePathParam, "", "location",
		"Specifies a custom location for completed repositories. Defaults to the working path if not specified.")
	ap.SupportsString(repoWorkPathParam, "", "location", "Specifies a custom location for repositories as they're being worked on.")
	ap.SupportsString(metricsPathParam, "", "location",
		"Specifies a custom location for where metric logs are stored. Metrics are not created if a location is not specified.")

	// Argument parser requires all arguments to be defined upfront, which doesn't work when commands will later define
	// more arguments. As a result, we remove any arguments that we don't know about here, and the commands will complain
	// later on.
	args := make([]string, len(os.Args)-1)
	copy(args, os.Args[1:])
	for {
		_, err := ap.Parse(args)
		unknownArgErr, ok := err.(argparser.UnknownArgumentParam)
		if !ok {
			break
		}
		for i, arg := range args {
			argStr := unknownArgErr.Error()[23:]
			argStr = argStr[:len(argStr)-1]
			if arg == "-"+argStr || arg == "--"+argStr {
				args = append(args[:i], args[i+1:]...)
			}
		}
	}

	apr, err := ap.Parse(args)
	if err != nil {
		if err != argparser.ErrHelp {
			cli.PrintErrln("%v", err.Error())
			usageFunc()()
			os.Exit(1)
		} else {
			for i, arg := range args {
				arg := strings.ToLower(arg)
				if arg == "-h" || arg == "--help" {
					args = append(args[:i], args[i+1:]...)
				}
			}
			apr, err = ap.Parse(args)
			if err != nil {
				cli.PrintErrln("%v", err.Error())
				usageFunc()()
				os.Exit(1)
			}

			if apr.NArg() == 0 {
				help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation("fuzzer", cli.CommandDocumentationContent{
					ShortDesc: "Creates and tests randomly generated repositories",
					LongDesc: `This tool has the core ability to randomly generate a collection of Dolt repositories, and to perform some kind of
action(s) on them. This is for the purpose of fuzzing Dolt. Those actions are selectable by usage of different commands.`,
					Synopsis: []string{"<command> [<option>...]"},
				}, ap))
				help()
				cli.Println("\bCOMMANDS")
				cmds := sortedCommands()
				for _, cmd := range cmds {
					cli.Printf("\t%s - %s\n", cmd.Name(), cmd.Description())
				}
				os.Exit(0)
			}
		}
	}
	return ap, apr
}

func usageFunc() func() {
	return func() {
		cmds := sortedCommands()
		cli.Println("Valid commands for fuzzer are")
		for _, cmd := range cmds {
			cli.Printf("    %16s - %s\n", cmd.Name(), cmd.Description())
		}
	}
}

func sortedCommands() []commands.Command {
	var cmds []commands.Command
	for _, cmd := range commands.Commands {
		cmds = append(cmds, cmd)
	}
	sort.Slice(cmds, func(i, j int) bool {
		return strings.ToLower(cmds[i].Name()) < strings.ToLower(cmds[j].Name())
	})
	return cmds
}

func createFolder(path string) {
	if path != "./" {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			err = os.Mkdir(path, os.ModeDir|0777)
			if err != nil {
				cli.PrintErrf("%+v\n", err)
				os.Exit(1)
			}
		} else if err != nil {
			cli.PrintErrf("%+v\n", err)
			os.Exit(1)
		}
	}
}

func expandPath(path string) string {
	absPath, err := filepath.Abs(path)
	if err != nil {
		cli.PrintErrf("%+v\n", err)
		os.Exit(1)
	}
	absPath = strings.ReplaceAll(absPath, `\`, `/`)
	if absPath[len(absPath)-1] != '/' {
		absPath += "/"
	}
	return absPath
}
