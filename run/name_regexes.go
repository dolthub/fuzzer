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
	"regexp"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/parameters"
)

// nameRegexes holds the regexes used for all of the names that are regex checked.
type nameRegexes struct {
	Branches    *regexp.Regexp
	Tables      *regexp.Regexp
	Columns     *regexp.Regexp
	Indexes     *regexp.Regexp
	Constraints *regexp.Regexp
}

// newNameRegexes returns a *nameRegexes.
func newNameRegexes(base *parameters.Base) (*nameRegexes, error) {
	branches, err := regexp.Compile(base.InvalidNameRegexes.Branches)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	tables, err := regexp.Compile(base.InvalidNameRegexes.Tables)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	columns, err := regexp.Compile(base.InvalidNameRegexes.Columns)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	indexes, err := regexp.Compile(base.InvalidNameRegexes.Indexes)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	constraints, err := regexp.Compile(base.InvalidNameRegexes.Constraints)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &nameRegexes{
		Branches:    branches,
		Tables:      tables,
		Columns:     columns,
		Indexes:     indexes,
		Constraints: constraints,
	}, nil
}
