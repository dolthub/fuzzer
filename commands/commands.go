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

	"github.com/dolthub/dolt/go/libraries/utils/argparser"

	"github.com/dolthub/fuzzer/run"
)

// Command is the interface for fuzzer commands.
type Command interface {
	run.HookRegistrant
	Name() string
	Description() string
	ParseArgs(commandStr string, ap *argparser.ArgParser, args []string) error
}

var Commands = make(map[string]Command)

// addCommand adds the given command to the command map, allowing its usage from the command line.
func addCommand(cmd Command) {
	Commands[strings.ToLower(cmd.Name())] = cmd
}
