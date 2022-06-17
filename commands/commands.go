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
	"strings"

	"github.com/dolthub/fuzzer/parameters"

	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/utils/argparser"
)

// Command is the interface for fuzzer commands.
type Command interface {
	run.HookRegistrant
	// Name returns the name of the command. This is what should be passed as an argument.
	Name() string
	// Description is the help text to display for this argument.
	Description() string
	// ParseArgs handle argument parsing for this command.
	ParseArgs(commandStr string, ap *argparser.ArgParser, args []string) error
	// AdjustConfig will adjust the configuration file, as needed, immediately before any cycles begin (after directory
	// creation, etc.).
	AdjustConfig(config *parameters.Base) error
}

var Commands = make(map[string]Command)

// addCommand adds the given command to the command map, allowing its usage from the command line.
func addCommand(cmd Command) {
	Commands[strings.ToLower(cmd.Name())] = cmd
}
