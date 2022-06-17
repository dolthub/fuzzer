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
	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
	"github.com/dolthub/fuzzer/utils/argparser"
	"github.com/dolthub/fuzzer/utils/cli"
)

// Basic handles basic repository validation.
type Basic struct{}

var _ Command = (*Basic)(nil)

// init adds the command to the map.
func init() {
	addCommand(&Basic{})
}

// Register implements the interface Command.
func (b *Basic) Register(_ *run.Hooks) {}

// Name implements the interface Command.
func (b *Basic) Name() string {
	return "basic"
}

// Description implements the interface Command.
func (b *Basic) Description() string {
	return "Performs basic validation of dolt repositories."
}

// ParseArgs implements the interface Command.
func (b *Basic) ParseArgs(commandStr string, ap *argparser.ArgParser, args []string) error {
	help, _ := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{
		ShortDesc: "Validates created dolt repositories",
		LongDesc: `This command performs a basic validation for created dolt repositories by internally comparing them against a generated
valid repository. Many other commands will perform this validation, although they may change variables to ensure that
the repository conforms to their required parameters (such as merge enforcing at least two branches). Running this
command validates repositories that are constrained only by the configuration file as loaded.`,
		Synopsis: nil,
	}, ap))
	_ = cli.ParseArgsOrDie(ap, args, help)
	return nil
}

// AdjustConfig implements the interface Command.
func (b *Basic) AdjustConfig(config *parameters.Base) error {
	return nil
}
