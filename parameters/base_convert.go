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
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/ranges"
)

// convertConfigBase converts a *configBase to a *Base while verifying the config file's contents.
func convertConfigBase(cBase *configBase) (*Base, error) {
	base := &Base{}

	// Invalid_Name_Regexes
	if err := cBase.InvalidNameRegexes.Validate(); err != nil {
		return nil, errors.Wrap(err)
	}
	base.InvalidNameRegexes.Branches = cBase.InvalidNameRegexes.Branches
	base.InvalidNameRegexes.Tables = cBase.InvalidNameRegexes.Tables
	base.InvalidNameRegexes.Columns = cBase.InvalidNameRegexes.Columns
	base.InvalidNameRegexes.Indexes = cBase.InvalidNameRegexes.Indexes
	base.InvalidNameRegexes.Constraints = cBase.InvalidNameRegexes.Constraints

	// Amounts
	if err := cBase.Amounts.Normalize(); err != nil {
		return nil, errors.Wrap(err)
	}
	base.Amounts.Branches = ranges.NewInt(cBase.Amounts.Branches)
	base.Amounts.Tables = ranges.NewInt(cBase.Amounts.Tables)
	base.Amounts.PrimaryKeys = ranges.NewInt(cBase.Amounts.PrimaryKeys)
	base.Amounts.Columns = ranges.NewInt(cBase.Amounts.Columns)
	base.Amounts.Indexes = ranges.NewInt(cBase.Amounts.Indexes)
	base.Amounts.ForeignKeyConstraints = ranges.NewInt(cBase.Amounts.ForeignKeyConstraints)
	base.Amounts.Rows = ranges.NewInt(cBase.Amounts.Rows)
	base.Amounts.IndexDelay = ranges.NewInt(cBase.Amounts.IndexDelay)

	// Statement_Distribution
	if err := cBase.StatementDistribution.Normalize(); err != nil {
		return nil, errors.Wrap(err)
	}
	base.StatementDistribution.Insert = ranges.NewInt(cBase.StatementDistribution.Insert)
	base.StatementDistribution.Replace = ranges.NewInt(cBase.StatementDistribution.Replace)
	base.StatementDistribution.Update = ranges.NewInt(cBase.StatementDistribution.Update)
	base.StatementDistribution.Delete = ranges.NewInt(cBase.StatementDistribution.Delete)

	// Options
	if err := cBase.Options.Validate(); err != nil {
		return nil, errors.Wrap(err)
	}
	base.Options.DoltVersion = cBase.Options.DoltVersion
	base.Options.AutoGC = cBase.Options.AutoGC
	base.Options.ManualGC = cBase.Options.ManualGC
	base.Options.IncludeReadme = cBase.Options.IncludeReadme
	base.Options.LowerRowsMainOnly = cBase.Options.LowerRowsMainOnly
	base.Options.Logging = cBase.Options.Logging
	base.Options.DeleteSuccesses = cBase.Options.DeleteSuccesses
	base.Options.Port = int64(cBase.Options.Port)
	base.Options.ZipInternalData = cBase.Options.ZipInternalData
	base.Options.DeleteAfterZip = cBase.Options.DeleteAfterZip
	base.Options.SeedInFile = cBase.Options.SeedInFile
	base.Options.SeedOutFile = cBase.Options.SeedOutFile

	// Types.Parameters
	if err := cBase.Types.Parameters.Normalize(); err != nil {
		return nil, errors.Wrap(err)
	}
	base.Types.Binary.Length = ranges.NewInt(cBase.Types.Parameters.BinaryLength)
	base.Types.Bit.Width = ranges.NewInt(cBase.Types.Parameters.BitWidth)
	base.Types.Blob.Length = ranges.NewInt(cBase.Types.Parameters.BlobLength)
	base.Types.Char.Collations = cBase.Types.Parameters.CharCollations
	base.Types.Char.Length = ranges.NewInt(cBase.Types.Parameters.CharLength)
	base.Types.Decimal.Precision = ranges.NewInt(cBase.Types.Parameters.DecimalPrecision)
	base.Types.Decimal.Scale = ranges.NewInt(cBase.Types.Parameters.DecimalScale)
	base.Types.Enum.Collations = cBase.Types.Parameters.EnumCollations
	base.Types.Enum.ElementNameLength = ranges.NewInt(cBase.Types.Parameters.EnumElementNameLength)
	base.Types.Enum.NumberOfElements = ranges.NewInt(cBase.Types.Parameters.EnumNumberOfElements)
	base.Types.Longblob.Length = ranges.NewInt(cBase.Types.Parameters.LongblobLength)
	base.Types.Longtext.Collations = cBase.Types.Parameters.LongtextCollations
	base.Types.Longtext.Length = ranges.NewInt(cBase.Types.Parameters.LongtextLength)
	base.Types.Mediumblob.Length = ranges.NewInt(cBase.Types.Parameters.MediumblobLength)
	base.Types.Mediumtext.Collations = cBase.Types.Parameters.MediumtextCollations
	base.Types.Mediumtext.Length = ranges.NewInt(cBase.Types.Parameters.MediumtextLength)
	base.Types.Set.Collations = cBase.Types.Parameters.SetCollations
	base.Types.Set.ElementNameLength = ranges.NewInt(cBase.Types.Parameters.SetElementNameLength)
	base.Types.Set.NumberOfElements = ranges.NewInt(cBase.Types.Parameters.SetNumberOfElements)
	base.Types.Text.Collations = cBase.Types.Parameters.TextCollations
	base.Types.Text.Length = ranges.NewInt(cBase.Types.Parameters.TextLength)
	base.Types.Tinyblob.Length = ranges.NewInt(cBase.Types.Parameters.TinyblobLength)
	base.Types.Tinytext.Collations = cBase.Types.Parameters.TinytextCollations
	base.Types.Tinytext.Length = ranges.NewInt(cBase.Types.Parameters.TinytextLength)
	base.Types.Varbinary.Length = ranges.NewInt(cBase.Types.Parameters.VarbinaryLength)
	base.Types.Varchar.Collations = cBase.Types.Parameters.VarcharCollations
	base.Types.Varchar.Length = ranges.NewInt(cBase.Types.Parameters.VarcharLength)

	// Types.Distribution
	if err := cBase.Types.Distribution.Normalize(); err != nil {
		return nil, errors.Wrap(err)
	}
	base.Types.Bigint.Distribution = ranges.NewInt(cBase.Types.Distribution.Bigint)
	base.Types.BigintUnsigned.Distribution = ranges.NewInt(cBase.Types.Distribution.BigintUnsigned)
	base.Types.Binary.Distribution = ranges.NewInt(cBase.Types.Distribution.Binary)
	base.Types.Bit.Distribution = ranges.NewInt(cBase.Types.Distribution.Bit)
	base.Types.Blob.Distribution = ranges.NewInt(cBase.Types.Distribution.Blob)
	base.Types.Char.Distribution = ranges.NewInt(cBase.Types.Distribution.Char)
	base.Types.Date.Distribution = ranges.NewInt(cBase.Types.Distribution.Date)
	base.Types.Datetime.Distribution = ranges.NewInt(cBase.Types.Distribution.Datetime)
	base.Types.Decimal.Distribution = ranges.NewInt(cBase.Types.Distribution.Decimal)
	base.Types.Double.Distribution = ranges.NewInt(cBase.Types.Distribution.Double)
	base.Types.Enum.Distribution = ranges.NewInt(cBase.Types.Distribution.Enum)
	base.Types.Float.Distribution = ranges.NewInt(cBase.Types.Distribution.Float)
	base.Types.Int.Distribution = ranges.NewInt(cBase.Types.Distribution.Int)
	base.Types.IntUnsigned.Distribution = ranges.NewInt(cBase.Types.Distribution.IntUnsigned)
	base.Types.Longblob.Distribution = ranges.NewInt(cBase.Types.Distribution.Longblob)
	base.Types.Longtext.Distribution = ranges.NewInt(cBase.Types.Distribution.Longtext)
	base.Types.Mediumblob.Distribution = ranges.NewInt(cBase.Types.Distribution.Mediumblob)
	base.Types.Mediumint.Distribution = ranges.NewInt(cBase.Types.Distribution.Mediumint)
	base.Types.MediumintUnsigned.Distribution = ranges.NewInt(cBase.Types.Distribution.MediumintUnsigned)
	base.Types.Mediumtext.Distribution = ranges.NewInt(cBase.Types.Distribution.Mediumtext)
	base.Types.Set.Distribution = ranges.NewInt(cBase.Types.Distribution.Set)
	base.Types.Smallint.Distribution = ranges.NewInt(cBase.Types.Distribution.Smallint)
	base.Types.SmallintUnsigned.Distribution = ranges.NewInt(cBase.Types.Distribution.SmallintUnsigned)
	base.Types.Text.Distribution = ranges.NewInt(cBase.Types.Distribution.Text)
	base.Types.Time.Distribution = ranges.NewInt(cBase.Types.Distribution.Time)
	base.Types.Timestamp.Distribution = ranges.NewInt(cBase.Types.Distribution.Timestamp)
	base.Types.Tinyblob.Distribution = ranges.NewInt(cBase.Types.Distribution.Tinyblob)
	base.Types.Tinyint.Distribution = ranges.NewInt(cBase.Types.Distribution.Tinyint)
	base.Types.TinyintUnsigned.Distribution = ranges.NewInt(cBase.Types.Distribution.TinyintUnsigned)
	base.Types.Tinytext.Distribution = ranges.NewInt(cBase.Types.Distribution.Tinytext)
	base.Types.Varbinary.Distribution = ranges.NewInt(cBase.Types.Distribution.Varbinary)
	base.Types.Varchar.Distribution = ranges.NewInt(cBase.Types.Distribution.Varchar)
	base.Types.Year.Distribution = ranges.NewInt(cBase.Types.Distribution.Year)

	return base, nil
}
