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
	"os"
	"sort"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/fatih/color"

	"github.com/dolthub/fuzzer/commands"
	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
)

const (
	configPathParam = "config"
	cyclesParam     = "cycles"
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
		cli.PrintErrf(color.RedString("error: unknown command `%v`\n", args[0]))
		usageFunc()()
		os.Exit(1)
	}
	err := cmd.ParseArgs("fuzzer "+cmd.Name(), ap, os.Args[1:])
	if err != nil {
		cli.PrintErrln(color.RedString("%v", err))
		os.Exit(1)
	}

	configPath := "./config.toml"
	if readParam, ok := apr.GetValue(configPathParam); ok {
		configPath = readParam
	}
	base, err := parameters.LoadFromFile(configPath)
	if err != nil {
		cli.PrintErrln(color.RedString("%v", err))
		os.Exit(1)
	}
	planner, err := run.NewPlanner(base)
	if err != nil {
		cli.PrintErrln(color.RedString("%v", err))
		os.Exit(1)
	}
	cmd.Register(planner.Hooks)
	cycleCount := -1
	if readParam, ok := apr.GetInt(cyclesParam); ok {
		cycleCount = readParam
	}
	for i := 0; cycleCount < 0 || i < cycleCount; i++ {
		cycle, err := planner.NewCycle()
		if err != nil {
			cli.PrintErrf(color.RedString("%+v\n", err))
		}
		err = cycle.Run()
		if err != nil {
			cli.PrintErrf(color.RedString("%+v\n", err))
		}
	}
}

func getArgParser() (*argparser.ArgParser, *argparser.ArgParseResults) {
	ap := argparser.NewArgParser()
	ap.SupportsString("config", "", "location", "Specifies a custom location for the config file.")
	ap.SupportsString("cycles", "", "count", "Controls how many cycles are run. If absent or negative then runs forever.")
	args := os.Args[1:]
	apr, err := ap.Parse(os.Args[1:])
	if err != nil {
		if err != argparser.ErrHelp {
			cli.PrintErrln(color.RedString("%v", err.Error()))
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
				cli.PrintErrln(color.RedString("%v", err.Error()))
				usageFunc()()
				os.Exit(1)
			}
		}
	}
	return ap, apr
}

func usageFunc() func() {
	return func() {
		var cmds []commands.Command
		for _, cmd := range commands.Commands {
			cmds = append(cmds, cmd)
		}
		sort.Slice(cmds, func(i, j int) bool {
			return strings.ToLower(cmds[i].Name()) < strings.ToLower(cmds[j].Name())
		})
		cli.Println("Valid commands for fuzzer are")
		for _, cmd := range cmds {
			cli.Printf("    %16s - %s\n", cmd.Name(), cmd.Description())
		}
	}
}
