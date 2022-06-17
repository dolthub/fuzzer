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
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/utils/argparser"
	"github.com/dolthub/fuzzer/utils/cli"
)

const (
	errorMessageParam = "message"
)

// Error forces each cycle to fail with an error.
type Error struct {
	errMsg string
}

var _ Command = (*Error)(nil)

// init adds the command to the map.
func init() {
	addCommand(&Error{})
}

// Register implements the interface Command.
func (e *Error) Register(hooks *run.Hooks) {
	hooks.CycleStarted(e.Fail)
}

func (e *Error) Fail(c *run.Cycle) error {
	return errors.New(e.errMsg)
}

// Name implements the interface Command.
func (e *Error) Name() string {
	return "error"
}

// Description implements the interface Command.
func (e *Error) Description() string {
	return "Forces each cycle to error."
}

// ParseArgs implements the interface Command.
func (e *Error) ParseArgs(commandStr string, ap *argparser.ArgParser, args []string) error {
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{
		ShortDesc: "Forces each cycle to error",
		LongDesc: `This command causes each cycle to error rather than succeed. This is primarily useful for setting up automated workflows
that need to handle the condition of a cycle finishing unsuccessfully.`,
		Synopsis: nil,
	}, ap))
	ap.SupportsString(errorMessageParam, "", "contents", "The error message to output.")
	apr := cli.ParseArgsOrDie(ap, args, help)
	if errMsg, ok := apr.GetValue(errorMessageParam); ok {
		e.errMsg = errMsg
	} else {
		e.errMsg = "A forced error has occurred."
	}
	return nil
}

// AdjustConfig implements the interface Command.
func (e *Error) AdjustConfig(config *parameters.Base) error {
	return nil
}
