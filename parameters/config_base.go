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

package parameters

import (
	"fmt"
	"regexp"
	"unicode"

	"github.com/dolthub/fuzzer/errors"
)

var (
	errASCIIOnly      = "%s must be comprised of only ASCII characters"
	errInvalidRange   = "%s must contain either 1 or 2 elements, but has %d"
	errEmptyRangeColl = "%s must contain at least 1 range, but has none"
	errRangeBackwards = "%s has the upper and lower bounds mixed"
	errRangeNegative  = "%s cannot contain any negative numbers"
	errRangeMinimum1  = "%s minimum must be >= 1"
	errDistLowerbound = "%s needs at least one non-zero lower bound"
)

// configBase represents the root table in the config file.
type configBase struct {
	InvalidNameRegexes    configInvalidNameRegexes    `json:"Invalid_Name_Regexes"`
	Amounts               configAmounts               `json:"Amounts"`
	StatementDistribution configStatementDistribution `json:"Statement_Distribution"`
	InterfaceDistribution configInterfaceDistribution `json:"Interface_Distribution"`
	Options               configOptions               `json:"Options"`
	Types                 configTypes                 `json:"Types"`
}

// configInvalidNameRegexes represents the "Invalid_Name_Regexes" table in the config file.
type configInvalidNameRegexes struct {
	Branches    string `json:"Branches"`
	Tables      string `json:"Tables"`
	Columns     string `json:"Columns"`
	Indexes     string `json:"Indexes"`
	Constraints string `json:"Constraints"`
}

// Validate checks if the read values are valid.
func (c *configInvalidNameRegexes) Validate() error {
	if !isASCII(c.Branches) {
		return errors.New(fmt.Sprintf(errASCIIOnly, "Invalid_Name_Regexes.Branches"))
	}
	_, err := regexp.Compile(c.Branches)
	if err != nil {
		return errors.Wrap(err)
	}
	if !isASCII(c.Tables) {
		return errors.New(fmt.Sprintf(errASCIIOnly, "Invalid_Name_Regexes.Tables"))
	}
	_, err = regexp.Compile(c.Tables)
	if err != nil {
		return errors.Wrap(err)
	}
	if !isASCII(c.Columns) {
		return errors.New(fmt.Sprintf(errASCIIOnly, "Invalid_Name_Regexes.Columns"))
	}
	_, err = regexp.Compile(c.Columns)
	if err != nil {
		return errors.Wrap(err)
	}
	if !isASCII(c.Indexes) {
		return errors.New(fmt.Sprintf(errASCIIOnly, "Invalid_Name_Regexes.Indexes"))
	}
	_, err = regexp.Compile(c.Indexes)
	if err != nil {
		return errors.Wrap(err)
	}
	if !isASCII(c.Constraints) {
		return errors.New(fmt.Sprintf(errASCIIOnly, "Invalid_Name_Regexes.Constraints"))
	}
	_, err = regexp.Compile(c.Constraints)
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// configAmounts represents the "Amounts" table in the config file.
type configAmounts struct {
	Branches              []int64 `json:"Branches"`
	Tables                []int64 `json:"Tables"`
	PrimaryKeys           []int64 `json:"Primary_Keys"`
	Columns               []int64 `json:"Columns"`
	Indexes               []int64 `json:"Indexes"`
	ForeignKeyConstraints []int64 `json:"Foreign_Key_Constraints"`
	Rows                  []int64 `json:"Rows"`
	IndexDelay            []int64 `json:"Index_Delay"`
}

// Normalize checks if the read values are valid, while normalizing all values to their expected forms.
func (c *configAmounts) Normalize() error {
	var err error
	c.Branches, err = normalizeIntRange(c.Branches, "Amounts.Branches")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Branches[0] < 1 {
		return errors.New(fmt.Sprintf(errRangeMinimum1, "Amounts.Branches"))
	}
	c.Tables, err = normalizeIntRange(c.Tables, "Amounts.Tables")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Tables[0] < 1 {
		return errors.New(fmt.Sprintf(errRangeMinimum1, "Amounts.Tables"))
	}
	c.PrimaryKeys, err = normalizeIntRange(c.PrimaryKeys, "Amounts.Primary_Keys")
	if err != nil {
		return errors.Wrap(err)
	}
	c.Columns, err = normalizeIntRange(c.Columns, "Amounts.Columns")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Columns[0] < 1 {
		return errors.New(fmt.Sprintf(errRangeMinimum1, "Amounts.Columns"))
	}
	c.Indexes, err = normalizeIntRange(c.Indexes, "Amounts.Indexes")
	if err != nil {
		return errors.Wrap(err)
	}
	c.ForeignKeyConstraints, err = normalizeIntRange(c.ForeignKeyConstraints, "Amounts.Foreign_Key_Constraints")
	if err != nil {
		return errors.Wrap(err)
	}
	c.Rows, err = normalizeIntRange(c.Rows, "Amounts.Rows")
	if err != nil {
		return errors.Wrap(err)
	}
	c.IndexDelay, err = normalizeIntRange(c.IndexDelay, "Amounts.Index_Delay")
	if err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// configStatementDistribution represents the "Statement_Distribution" table in the config file.
type configStatementDistribution struct {
	Insert  []int64 `json:"INSERT"`
	Replace []int64 `json:"REPLACE"`
	Update  []int64 `json:"UPDATE"`
	Delete  []int64 `json:"DELETE"`
}

// Normalize checks if the read values are valid, while normalizing all values to their expected forms.
func (c *configStatementDistribution) Normalize() error {
	var err error
	atLeastOneLowerbound := false
	c.Insert, err = normalizeIntRange(c.Insert, "Statement_Distribution.INSERT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Insert[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Replace, err = normalizeIntRange(c.Replace, "Statement_Distribution.REPLACE")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Replace[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Update, err = normalizeIntRange(c.Update, "Statement_Distribution.UPDATE")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Update[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Delete, err = normalizeIntRange(c.Delete, "Statement_Distribution.DELETE")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Delete[0] > 0 {
		atLeastOneLowerbound = true
	}
	if !atLeastOneLowerbound {
		return errors.New(fmt.Sprintf(errDistLowerbound, "Statement_Distribution"))
	}
	return nil
}

// configInterfaceDistribution represents the "Interface_Distribution" table in the config file.
type configInterfaceDistribution struct {
	CLIQuery         []int64 `json:"CLI_Query"`
	CLIBatch         []int64 `json:"CLI_Batch"`
	SQLServer        []int64 `json:"SQL_Server"`
	ConsecutiveRange []int64 `json:"Consecutive_Range"`
}

// Normalize checks if the read values are valid, while normalizing all values to their expected forms.
func (c *configInterfaceDistribution) Normalize() error {
	var err error
	atLeastOneLowerbound := false
	c.CLIQuery, err = normalizeIntRange(c.CLIQuery, "Interface_Distribution.CLI_Query")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.CLIQuery[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.CLIBatch, err = normalizeIntRange(c.CLIBatch, "Interface_Distribution.CLI_Batch")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.CLIBatch[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.SQLServer, err = normalizeIntRange(c.SQLServer, "Interface_Distribution.SQL_Server")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.SQLServer[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.ConsecutiveRange, err = normalizeIntRange(c.ConsecutiveRange, "Interface_Distribution.Consecutive_Range")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.ConsecutiveRange[0] > 0 {
		atLeastOneLowerbound = true
	}
	if !atLeastOneLowerbound {
		return errors.New(fmt.Sprintf(errDistLowerbound, "Interface_Distribution"))
	}
	return nil
}

// configOptions represents the "Options" table in the config file.
type configOptions struct {
	DoltVersion         string `json:"Dolt_Version"`
	AutoGC              bool   `json:"Auto_GC"`
	ManualGC            bool   `json:"Manual_GC"`
	IncludeReadme       bool   `json:"Include_README_Config"`
	LowerRowsMasterOnly bool   `json:"Enforce_Rows_Lower_Bound_on_Master_Only"`
	Logging             bool   `json:"Logging"`
	DeleteSuccesses     bool   `json:"Delete_Successful_Runs"`
	Port                uint64 `json:"Port"`
}

// Validate checks if the read values are valid.
func (c *configOptions) Validate() error {
	//TODO: verify that DoltVersion is either empty string, a version number, or valid hash
	if c.Port > 65535 {
		return errors.New(fmt.Sprintf("Options.Port must be <= 65535, but is %d", c.Port))
	}
	return nil
}

// isASCII checks if the string is comprised of only ASCII characters.
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// normalizeIntRange checks if the int64 slice represents a valid range, containing either one or two values in the
// correct order.
func normalizeIntRange(r []int64, fieldName string) ([]int64, error) {
	if len(r) == 1 {
		r = append(r, r[0])
	} else if len(r) == 2 {
		if r[0] < 0 {
			return r, errors.New(fmt.Sprintf(errRangeNegative, fieldName))
		}
		if r[0] > r[1] {
			return r, errors.New(fmt.Sprintf(errRangeBackwards, fieldName))
		}
	} else {
		return r, errors.New(fmt.Sprintf(errInvalidRange, fieldName, len(r)))
	}
	return r, nil
}

// normalizeMultipleIntRange checks if the slice of int64 slices represents a collection of valid ranges.
func normalizeMultipleIntRange(r [][]int64, fieldName string) ([][]int64, error) {
	if len(r) == 0 {
		return r, errors.New(fmt.Sprintf(errEmptyRangeColl, fieldName))
	}
	var err error
	for i := range r {
		r[i], err = normalizeIntRange(r[i], fieldName)
		if err != nil {
			return r, errors.Wrap(err)
		}
	}
	return r, nil
}
