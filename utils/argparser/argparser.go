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

package argparser

import (
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	optNameValDelimChars = " =:"
	whitespaceChars      = " \r\n\t"

	helpFlag       = "help"
	helpFlagAbbrev = "h"
)

type ArgParser struct {
	Supported         []*Option
	NameOrAbbrevToOpt map[string]*Option
	ArgListHelp       [][2]string
}

func NewArgParser() *ArgParser {
	var supported []*Option
	nameOrAbbrevToOpt := make(map[string]*Option)
	return &ArgParser{supported, nameOrAbbrevToOpt, nil}
}

// Adds support for a new argument with the option given. Options must have a unique name and abbreviated name.
func (ap *ArgParser) SupportOption(opt *Option) {
	name := opt.Name
	abbrev := opt.Abbrev

	_, nameExist := ap.NameOrAbbrevToOpt[name]
	_, abbrevExist := ap.NameOrAbbrevToOpt[abbrev]

	if name == "" {
		panic("Name is required")
	} else if name == "help" || abbrev == "help" || name == "h" || abbrev == "h" {
		panic(`"help" and "h" are both reserved`)
	} else if nameExist || abbrevExist {
		panic("There is a bug.  Two supported arguments have the same name or abbreviation")
	} else if name[0] == '-' || (len(abbrev) > 0 && abbrev[0] == '-') {
		panic("There is a bug. Option names, and abbreviations should not start with -")
	} else if strings.IndexAny(name, optNameValDelimChars) != -1 || strings.IndexAny(name, whitespaceChars) != -1 {
		panic("There is a bug.  Option name contains an invalid character")
	}

	ap.Supported = append(ap.Supported, opt)
	ap.NameOrAbbrevToOpt[name] = opt

	if abbrev != "" {
		ap.NameOrAbbrevToOpt[abbrev] = opt
	}
}

// Adds support for a new flag (argument with no value). See SupportOpt for details on params.
func (ap *ArgParser) SupportsFlag(name, abbrev, desc string) *ArgParser {
	opt := &Option{name, abbrev, "", OptionalFlag, desc, nil}
	ap.SupportOption(opt)

	return ap
}

// Adds support for a new string argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsString(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, nil}
	ap.SupportOption(opt)

	return ap
}

func (ap *ArgParser) SupportsValidatedString(name, abbrev, valDesc, desc string, validator ValidationFunc) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, validator}
	ap.SupportOption(opt)

	return ap
}

// Adds support for a new uint argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsUint(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isUintStr}
	ap.SupportOption(opt)

	return ap
}

// Adds support for a new int argument with the description given. See SupportOpt for details on params.
func (ap *ArgParser) SupportsInt(name, abbrev, valDesc, desc string) *ArgParser {
	opt := &Option{name, abbrev, valDesc, OptionalValue, desc, isIntStr}
	ap.SupportOption(opt)

	return ap
}

// modal options in order of descending string length
func (ap *ArgParser) sortedModalOptions() []string {
	smo := make([]string, 0, len(ap.Supported))
	for s, opt := range ap.NameOrAbbrevToOpt {
		if opt.OptType == OptionalFlag && s != "" {
			smo = append(smo, s)
		}
	}
	sort.Slice(smo, func(i, j int) bool { return len(smo[i]) > len(smo[j]) })
	return smo
}

func (ap *ArgParser) matchModalOptions(arg string) (matches []*Option, rest string) {
	rest = arg

	// try to match longest options first
	candidateFlagNames := ap.sortedModalOptions()

	kontinue := true
	for kontinue {
		kontinue = false

		// stop if we see a value option
		for _, vo := range ap.sortedValueOptions() {
			lv := len(vo)
			isValOpt := len(rest) >= lv && rest[:lv] == vo
			if isValOpt {
				return matches, rest
			}
		}

		for i, on := range candidateFlagNames {
			lo := len(on)
			isMatch := len(rest) >= lo && rest[:lo] == on
			if isMatch {
				rest = rest[lo:]
				m := ap.NameOrAbbrevToOpt[on]
				matches = append(matches, m)

				// only match options once
				head := candidateFlagNames[:i]
				var tail []string
				if i+1 < len(candidateFlagNames) {
					tail = candidateFlagNames[i+1:]
				}
				candidateFlagNames = append(head, tail...)

				kontinue = true
				break
			}
		}
	}
	return matches, rest
}

func (ap *ArgParser) sortedValueOptions() []string {
	vos := make([]string, 0, len(ap.Supported))
	for s, opt := range ap.NameOrAbbrevToOpt {
		if opt.OptType == OptionalValue && s != "" {
			vos = append(vos, s)
		}
	}
	sort.Slice(vos, func(i, j int) bool { return len(vos[i]) > len(vos[j]) })
	return vos
}

func (ap *ArgParser) matchValueOption(arg string) (match *Option, value *string) {
	for _, on := range ap.sortedValueOptions() {
		lo := len(on)
		isMatch := len(arg) >= lo && arg[:lo] == on
		if isMatch {
			v := arg[lo:]
			v = strings.TrimLeft(v, optNameValDelimChars)
			if len(v) > 0 {
				value = &v
			}
			match = ap.NameOrAbbrevToOpt[on]
			return match, value
		}
	}
	return nil, nil
}

// Parses the string args given using the configuration previously specified with calls to the various Supports*
// methods. Any unrecognized arguments or incorrect types will result in an appropriate error being returned. If the
// universal --help or -h flag is found, an ErrHelp error is returned.
func (ap *ArgParser) Parse(args []string) (*ArgParseResults, error) {
	list := make([]string, 0, 16)
	results := make(map[string]string)

	i := 0
	for ; i < len(args); i++ {
		arg := args[i]

		if len(arg) == 0 || arg[0] != '-' || arg == "--" { // empty strings should get passed through like other naked words
			list = append(list, arg)
			continue
		}

		arg = strings.TrimLeft(arg, "-")

		if arg == helpFlag || arg == helpFlagAbbrev {
			return nil, ErrHelp
		}

		modalOpts, rest := ap.matchModalOptions(arg)

		for _, opt := range modalOpts {
			if _, exists := results[opt.Name]; exists {
				return nil, errors.New("error: multiple values provided for `" + opt.Name + "'")
			}

			results[opt.Name] = ""
		}

		opt, value := ap.matchValueOption(rest)

		if opt == nil {
			if rest == "" {
				continue
			}

			if len(modalOpts) > 0 {
				// value was attached to modal flag
				// eg: dolt branch -fdmy_branch
				list = append(list, rest)
				continue
			}

			return nil, UnknownArgumentParam{name: arg}
		}

		if _, exists := results[opt.Name]; exists {
			//already provided
			return nil, errors.New("error: multiple values provided for `" + opt.Name + "'")
		}

		if value == nil {
			i++
			if i >= len(args) {
				return nil, errors.New("error: no value for option `" + opt.Name + "'")
			}

			valueStr := args[i]
			value = &valueStr
		}

		if opt.Validator != nil {
			err := opt.Validator(*value)

			if err != nil {
				return nil, err
			}
		}

		results[opt.Name] = *value
	}

	if i < len(args) {
		copy(list, args[i:])
	}

	return &ArgParseResults{results, list, ap}, nil
}

type ArgParseResults struct {
	options map[string]string
	args    []string
	parser  *ArgParser
}

func (res *ArgParseResults) Equals(other *ArgParseResults) bool {
	if len(res.args) != len(other.args) || len(res.options) != len(res.options) {
		return false
	}

	for i, arg := range res.args {
		if other.args[i] != arg {
			return false
		}
	}

	for k, v := range res.options {
		if otherVal, ok := other.options[k]; !ok || v != otherVal {
			return false
		}
	}

	return true
}

func (res *ArgParseResults) Contains(name string) bool {
	_, ok := res.options[name]
	return ok
}

func (res *ArgParseResults) ContainsArg(name string) bool {
	for _, val := range res.args {
		if val == name {
			return true
		}
	}
	return false
}

func (res *ArgParseResults) ContainsAll(names ...string) bool {
	for _, name := range names {
		if _, ok := res.options[name]; !ok {
			return false
		}
	}

	return true
}

func (res *ArgParseResults) ContainsAny(names ...string) bool {
	for _, name := range names {
		if _, ok := res.options[name]; ok {
			return true
		}
	}

	return false
}

func (res *ArgParseResults) ContainsMany(names ...string) []string {
	var contains []string
	for _, name := range names {
		if _, ok := res.options[name]; ok {
			contains = append(contains, name)
		}
	}
	return contains
}

func (res *ArgParseResults) GetValue(name string) (string, bool) {
	val, ok := res.options[name]
	return val, ok
}

func (res *ArgParseResults) GetValues(names ...string) map[string]string {
	vals := make(map[string]string)

	for _, name := range names {
		if val, ok := res.options[name]; ok {
			vals[name] = val
		}
	}

	return vals
}

func (res *ArgParseResults) MustGetValue(name string) string {
	val, ok := res.options[name]

	if !ok {
		panic("Value not available.")
	}

	return val
}

func (res *ArgParseResults) GetValueOrDefault(name, defVal string) string {
	val, ok := res.options[name]

	if ok {
		return val
	}

	return defVal
}

func (res *ArgParseResults) GetInt(name string) (int, bool) {
	val, ok := res.options[name]

	if !ok {
		return math.MinInt32, false
	}

	intVal, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return math.MinInt32, false
	}

	return int(intVal), true
}

func (res *ArgParseResults) GetUint(name string) (uint64, bool) {
	val, ok := res.options[name]

	if !ok {
		return math.MaxUint64, false
	}

	uintVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return math.MaxUint64, false
	}

	return uintVal, true
}

func (res *ArgParseResults) GetIntOrDefault(name string, defVal int) int {
	n, ok := res.GetInt(name)

	if ok {
		return n
	}

	return defVal
}

func (res *ArgParseResults) Args() []string {
	return res.args
}

func (res *ArgParseResults) NArg() int {
	return len(res.args)
}

func (res *ArgParseResults) Arg(idx int) string {
	return res.args[idx]
}

func (res *ArgParseResults) AnyFlagsEqualTo(val bool) map[string]struct{} {
	results := make([]string, 0, len(res.parser.Supported))
	for _, opt := range res.parser.Supported {
		if opt.OptType == OptionalFlag {
			name := opt.Name
			_, ok := res.options[name]

			if ok == val {
				results = append(results, name)
			}
		}
	}

	resultsMap := make(map[string]struct{})
	for _, result := range results {
		resultsMap[result] = struct{}{}
	}
	return resultsMap
}

func (res *ArgParseResults) FlagsEqualTo(names []string, val bool) map[string]struct{} {
	results := make([]string, 0, len(res.parser.Supported))
	for _, name := range names {
		opt, ok := res.parser.NameOrAbbrevToOpt[name]
		if ok && opt.OptType == OptionalFlag {
			_, ok := res.options[name]

			if ok == val {
				results = append(results, name)
			}
		}
	}

	resultsMap := make(map[string]struct{})
	for _, result := range results {
		resultsMap[result] = struct{}{}
	}
	return resultsMap
}

type UnknownArgumentParam struct {
	name string
}

func (unkn UnknownArgumentParam) Error() string {
	return "error: unknown option `" + unkn.name + "'"
}

var ErrHelp = errors.New("Help")

type OptionType int

const (
	OptionalFlag OptionType = iota
	OptionalValue
)

type ValidationFunc func(string) error

// Convenience validation function that asserts that an arg is an integer
func isIntStr(str string) error {
	_, err := strconv.ParseInt(str, 10, 32)
	if err != nil {
		return errors.New("error: \"" + str + "\" is not a valid int.")
	}

	return nil
}

// Convenience validation function that asserts that an arg is an unsigned integer
func isUintStr(str string) error {
	_, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return errors.New("error: \"" + str + "\" is not a valid uint.")
	}

	return nil
}

// An Option encapsulates all the information necessary to represent and parse a command line argument.
type Option struct {
	// Long name for this Option, specified on the command line with --Name. Required.
	Name string
	// Abbreviated name for this Option, specified on the command line with -Abbrev. Optional.
	Abbrev string
	// Brief description of the Option.
	ValDesc string
	// The type of this option, either a flag or a value.
	OptType OptionType
	// Longer help text for the option.
	Desc string
	// Function to validate an Option after parsing, returning any error.
	Validator ValidationFunc
}
