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
