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
	"time"

	"github.com/dolthub/fuzzer/ranges"
	"github.com/dolthub/fuzzer/types"
)

// Base is the base set of parameters as set by the config file.
type Base struct {
	InvalidNameRegexes    InvalidNameRegexes
	Amounts               Amounts
	StatementDistribution StatementDistribution
	Options               Options
	Types                 Types
	Arguments             Arguments
}

// InvalidNameRegexes contains regexes that generated names are matched against for validity.
type InvalidNameRegexes struct {
	Branches    string
	Tables      string
	Columns     string
	Indexes     string
	Constraints string
}

// Amounts specifies the range for each element or function.
type Amounts struct {
	Branches              ranges.Int
	Tables                ranges.Int
	PrimaryKeys           ranges.Int
	Columns               ranges.Int
	Indexes               ranges.Int
	ForeignKeyConstraints ranges.Int
	Rows                  ranges.Int
	IndexDelay            ranges.Int
}

// StatementDistribution specifies the relative frequency of each statement in a cycle.
type StatementDistribution struct {
	Insert  ranges.Int
	Replace ranges.Int
	Update  ranges.Int
	Delete  ranges.Int
}

// Options are directives for all cycles.
type Options struct {
	DoltVersion       string
	AutoGC            bool
	ManualGC          bool
	IncludeReadme     bool
	LowerRowsMainOnly bool
	Logging           bool
	DeleteSuccesses   bool
	Port              int64
	ZipInternalData   bool
	DeleteAfterZip    bool
	SeedInFile        string
	SeedOutFile       string
}

// Types represents all of the MySQL types available to the program.
type Types struct {
	Bigint            types.Bigint
	BigintUnsigned    types.BigintUnsigned
	Binary            types.Binary
	Bit               types.Bit
	Blob              types.Blob
	Char              types.Char
	Date              types.Date
	Datetime          types.Datetime
	Decimal           types.Decimal
	Double            types.Double
	Enum              types.Enum
	Float             types.Float
	Int               types.Int
	IntUnsigned       types.IntUnsigned
	Longblob          types.Longblob
	Longtext          types.Longtext
	Mediumblob        types.Mediumblob
	Mediumint         types.Mediumint
	MediumintUnsigned types.MediumintUnsigned
	Mediumtext        types.Mediumtext
	Set               types.Set
	Smallint          types.Smallint
	SmallintUnsigned  types.SmallintUnsigned
	Text              types.Text
	Time              types.Time
	Timestamp         types.Timestamp
	Tinyblob          types.Tinyblob
	Tinyint           types.Tinyint
	TinyintUnsigned   types.TinyintUnsigned
	Tinytext          types.Tinytext
	Varbinary         types.Varbinary
	Varchar           types.Varchar
	Year              types.Year
}

// Arguments represents any arguments that are passed into the program at runtime.
type Arguments struct {
	NumOfCycles       int64
	Timeout           time.Duration
	FirstError        bool
	ConfigPath        string
	RepoFinishedPath  string
	RepoWorkingPath   string
	MetricsPath       string
	DontGenRandomData bool
}
