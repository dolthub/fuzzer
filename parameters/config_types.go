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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/fuzzer/errors"
)

var (
	errNoCollationForCharset = "%s has collation %s that does not have a matching character set"
	errParameterInvalidRange = "%s range bounds cannot be outside of %d-%d"
)

// configTypes represents the "Types" table in the config file.
type configTypes struct {
	Parameters   configTypeParameters   `json:"Parameters"`
	Distribution configTypeDistribution `json:"Distribution"`
}

// configTypeParameters represents the "Parameters" table in the config file, under the "Types" table.
type configTypeParameters struct {
	BinaryLength          []int64  `json:"BINARY_Length"`
	BitWidth              []int64  `json:"BIT_Width"`
	BlobLength            []int64  `json:"BLOB_Length"`
	CharCollations        []string `json:"CHAR_Collations"`
	CharLength            []int64  `json:"CHAR_Length"`
	DecimalPrecision      []int64  `json:"DECIMAL_Precision"`
	DecimalScale          []int64  `json:"DECIMAL_Scale"`
	EnumCollations        []string `json:"ENUM_Collations"`
	EnumElementNameLength []int64  `json:"ENUM_ElementNameLength"`
	EnumNumberOfElements  []int64  `json:"ENUM_NumberOfElements"`
	LongblobLength        []int64  `json:"LONGBLOB_Length"`
	LongtextCollations    []string `json:"LONGTEXT_Collations"`
	LongtextLength        []int64  `json:"LONGTEXT_Length"`
	MediumblobLength      []int64  `json:"MEDIUMBLOB_Length"`
	MediumtextCollations  []string `json:"MEDIUMTEXT_Collations"`
	MediumtextLength      []int64  `json:"MEDIUMTEXT_Length"`
	SetCollations         []string `json:"SET_Collations"`
	SetElementNameLength  []int64  `json:"SET_ElementNameLength"`
	SetNumberOfElements   []int64  `json:"SET_NumberOfElements"`
	TextCollations        []string `json:"TEXT_Collations"`
	TextLength            []int64  `json:"TEXT_Length"`
	TinyblobLength        []int64  `json:"TINYBLOB_Length"`
	TinytextCollations    []string `json:"TINYTEXT_Collations"`
	TinytextLength        []int64  `json:"TINYTEXT_Length"`
	VarbinaryLength       []int64  `json:"VARBINARY_Length"`
	VarcharCollations     []string `json:"VARCHAR_Collations"`
	VarcharLength         []int64  `json:"VARCHAR_Length"`
}

// Normalize checks if the read values are valid, while normalizing all values to their expected forms.
func (c *configTypeParameters) Normalize() error {
	var err error
	c.BinaryLength, err = normalizeIntRange(c.BinaryLength, "Types.Parameters.BINARY_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.BinaryLength[0] < 0 || c.BinaryLength[1] > 255 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "BINARY_Length", 0, 255))
	}
	c.BitWidth, err = normalizeIntRange(c.BitWidth, "Types.Parameters.BIT_Width")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.BitWidth[0] < 0 || c.BitWidth[1] > 64 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "BIT_Width", 0, 64))
	}
	c.BlobLength, err = normalizeIntRange(c.BlobLength, "Types.Parameters.BLOB_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.BlobLength[0] < 0 || c.BlobLength[1] > 65535 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "BLOB_Length", 0, 65535))
	}
	c.CharCollations, err = checkCollations(c.CharCollations, "Types.Parameters.CHAR_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.CharLength, err = normalizeIntRange(c.CharLength, "Types.Parameters.CHAR_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.CharLength[0] < 0 || c.CharLength[1] > 255 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "CHAR_Length", 0, 255))
	}
	c.DecimalPrecision, err = normalizeIntRange(c.DecimalPrecision, "Types.Parameters.DECIMAL_Precision")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.DecimalPrecision[0] < 1 || c.DecimalPrecision[1] > 65 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "DECIMAL_Precision", 1, 65))
	}
	c.DecimalScale, err = normalizeIntRange(c.DecimalScale, "Types.Parameters.DECIMAL_Scale")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.DecimalScale[0] < 0 || c.DecimalScale[1] > 30 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "DECIMAL_Scale", 0, 30))
	}
	c.EnumCollations, err = checkCollations(c.EnumCollations, "Types.Parameters.ENUM_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.EnumElementNameLength, err = normalizeIntRange(c.EnumElementNameLength, "Types.Parameters.ENUM_ElementNameLength")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.EnumElementNameLength[0] < 0 || c.EnumElementNameLength[1] > 65535 { // Arbitrary limit based on practicality
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "ENUM_ElementNameLength", 0, 65535))
	}
	c.EnumNumberOfElements, err = normalizeIntRange(c.EnumNumberOfElements, "Types.Parameters.ENUM_NumberOfElements")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.EnumNumberOfElements[0] < 0 || c.EnumNumberOfElements[1] > 65535 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "ENUM_NumberOfElements", 0, 65535))
	}
	c.LongblobLength, err = normalizeIntRange(c.LongblobLength, "Types.Parameters.LONGBLOB_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.LongblobLength[0] < 0 || c.LongblobLength[1] > 4294967295 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "LONGBLOB_Length", 0, 4294967295))
	}
	c.LongtextCollations, err = checkCollations(c.LongtextCollations, "Types.Parameters.LONGTEXT_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.LongtextLength, err = normalizeIntRange(c.LongtextLength, "Types.Parameters.LONGTEXT_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.LongtextLength[0] < 0 || c.LongtextLength[1] > 4294967295 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "LONGTEXT_Length", 0, 4294967295))
	}
	c.MediumblobLength, err = normalizeIntRange(c.MediumblobLength, "Types.Parameters.MEDIUMBLOB_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.MediumblobLength[0] < 0 || c.MediumblobLength[1] > 16777215 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "MEDIUMBLOB_Length", 0, 16777215))
	}
	c.MediumtextCollations, err = checkCollations(c.MediumtextCollations, "Types.Parameters.MEDIUMTEXT_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.MediumtextLength, err = normalizeIntRange(c.MediumtextLength, "Types.Parameters.MEDIUMTEXT_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.MediumtextLength[0] < 0 || c.MediumtextLength[1] > 16777215 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "MEDIUMTEXT_Length", 0, 16777215))
	}
	c.SetCollations, err = checkCollations(c.SetCollations, "Types.Parameters.SET_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.SetElementNameLength, err = normalizeIntRange(c.SetElementNameLength, "Types.Parameters.SET_ElementNameLength")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.SetElementNameLength[0] < 0 || c.SetElementNameLength[1] > 65535 { // Arbitrary limit based on practicality
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "SET_ElementNameLength", 0, 65535))
	}
	c.SetNumberOfElements, err = normalizeIntRange(c.SetNumberOfElements, "Types.Parameters.SET_NumberOfElements")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.SetNumberOfElements[0] < 0 || c.SetNumberOfElements[1] > 64 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "SET_NumberOfElements", 0, 64))
	}
	c.TextCollations, err = checkCollations(c.TextCollations, "Types.Parameters.TEXT_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.TextLength, err = normalizeIntRange(c.TextLength, "Types.Parameters.TEXT_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.TextLength[0] < 0 || c.TextLength[1] > 65535 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "TEXT_Length", 0, 65535))
	}
	c.TinyblobLength, err = normalizeIntRange(c.TinyblobLength, "Types.Parameters.TINYBLOB_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.TinyblobLength[0] < 0 || c.TinyblobLength[1] > 255 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "TINYBLOB_Length", 0, 255))
	}
	c.TinytextCollations, err = checkCollations(c.TinytextCollations, "Types.Parameters.TINYTEXT_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.TinytextLength, err = normalizeIntRange(c.TinytextLength, "Types.Parameters.TINYTEXT_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.TinytextLength[0] < 0 || c.TinytextLength[1] > 255 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "TINYTEXT_Length", 0, 255))
	}
	c.VarbinaryLength, err = normalizeIntRange(c.VarbinaryLength, "Types.Parameters.VARBINARY_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.VarbinaryLength[0] < 0 || c.VarbinaryLength[1] > 65535 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "VARBINARY_Length", 0, 65535))
	}
	c.VarcharCollations, err = checkCollations(c.VarcharCollations, "Types.Parameters.VARCHAR_Collations")
	if err != nil {
		return errors.Wrap(err)
	}
	c.VarcharLength, err = normalizeIntRange(c.VarcharLength, "Types.Parameters.VARCHAR_Length")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.VarcharLength[0] < 0 || c.VarcharLength[1] > 65535 {
		return errors.New(fmt.Sprintf(errParameterInvalidRange, "VARCHAR_Length", 0, 65535))
	}
	return nil
}

// configTypeDistribution represents the "Distribution" table in the config file, under the "Types" table.
type configTypeDistribution struct {
	Bigint            []int64 `json:"BIGINT"`
	BigintUnsigned    []int64 `json:"BIGINT_UNSIGNED"`
	Binary            []int64 `json:"BINARY"`
	Bit               []int64 `json:"BIT"`
	Blob              []int64 `json:"BLOB"`
	Char              []int64 `json:"CHAR"`
	Date              []int64 `json:"DATE"`
	Datetime          []int64 `json:"DATETIME"`
	Decimal           []int64 `json:"DECIMAL"`
	Double            []int64 `json:"DOUBLE"`
	Enum              []int64 `json:"ENUM"`
	Float             []int64 `json:"FLOAT"`
	Int               []int64 `json:"INT"`
	IntUnsigned       []int64 `json:"INT_UNSIGNED"`
	Longblob          []int64 `json:"LONGBLOB"`
	Longtext          []int64 `json:"LONGTEXT"`
	Mediumblob        []int64 `json:"MEDIUMBLOB"`
	Mediumint         []int64 `json:"MEDIUMINT"`
	MediumintUnsigned []int64 `json:"MEDIUMINT_UNSIGNED"`
	Mediumtext        []int64 `json:"MEDIUMTEXT"`
	Set               []int64 `json:"SET"`
	Smallint          []int64 `json:"SMALLINT"`
	SmallintUnsigned  []int64 `json:"SMALLINT_UNSIGNED"`
	Text              []int64 `json:"TEXT"`
	Time              []int64 `json:"TIME"`
	Timestamp         []int64 `json:"TIMESTAMP"`
	Tinyblob          []int64 `json:"TINYBLOB"`
	Tinyint           []int64 `json:"TINYINT"`
	TinyintUnsigned   []int64 `json:"TINYINT_UNSIGNED"`
	Tinytext          []int64 `json:"TINYTEXT"`
	Varbinary         []int64 `json:"VARBINARY"`
	Varchar           []int64 `json:"VARCHAR"`
	Year              []int64 `json:"YEAR"`
}

// Normalize checks if the read values are valid, while normalizing all values to their expected forms.
func (c *configTypeDistribution) Normalize() error {
	var err error
	atLeastOneLowerbound := false
	c.Bigint, err = normalizeIntRange(c.Bigint, "Types.Distribution.BIGINT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Bigint[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.BigintUnsigned, err = normalizeIntRange(c.BigintUnsigned, "Types.Distribution.BIGINT_UNSIGNED")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.BigintUnsigned[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Binary, err = normalizeIntRange(c.Binary, "Types.Distribution.BINARY")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Binary[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Bit, err = normalizeIntRange(c.Bit, "Types.Distribution.BIT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Bit[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Blob, err = normalizeIntRange(c.Blob, "Types.Distribution.BLOB")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Blob[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Char, err = normalizeIntRange(c.Char, "Types.Distribution.CHAR")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Char[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Date, err = normalizeIntRange(c.Date, "Types.Distribution.DATE")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Date[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Datetime, err = normalizeIntRange(c.Datetime, "Types.Distribution.DATETIME")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Datetime[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Decimal, err = normalizeIntRange(c.Decimal, "Types.Distribution.DECIMAL")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Decimal[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Double, err = normalizeIntRange(c.Double, "Types.Distribution.DOUBLE")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Double[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Enum, err = normalizeIntRange(c.Enum, "Types.Distribution.ENUM")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Enum[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Float, err = normalizeIntRange(c.Float, "Types.Distribution.FLOAT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Float[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Int, err = normalizeIntRange(c.Int, "Types.Distribution.INT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Int[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.IntUnsigned, err = normalizeIntRange(c.IntUnsigned, "Types.Distribution.INT_UNSIGNED")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.IntUnsigned[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Longblob, err = normalizeIntRange(c.Longblob, "Types.Distribution.LONGBLOB")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Longblob[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Longtext, err = normalizeIntRange(c.Longtext, "Types.Distribution.LONGTEXT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Longtext[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Mediumblob, err = normalizeIntRange(c.Mediumblob, "Types.Distribution.MEDIUMBLOB")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Mediumblob[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Mediumint, err = normalizeIntRange(c.Mediumint, "Types.Distribution.MEDIUMINT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Mediumint[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.MediumintUnsigned, err = normalizeIntRange(c.MediumintUnsigned, "Types.Distribution.MEDIUMINT_UNSIGNED")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.MediumintUnsigned[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Mediumtext, err = normalizeIntRange(c.Mediumtext, "Types.Distribution.MEDIUMTEXT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Mediumtext[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Set, err = normalizeIntRange(c.Set, "Types.Distribution.SET")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Set[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Smallint, err = normalizeIntRange(c.Smallint, "Types.Distribution.SMALLINT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Smallint[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.SmallintUnsigned, err = normalizeIntRange(c.SmallintUnsigned, "Types.Distribution.SMALLINT_UNSIGNED")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.SmallintUnsigned[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Text, err = normalizeIntRange(c.Text, "Types.Distribution.TEXT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Text[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Time, err = normalizeIntRange(c.Time, "Types.Distribution.TIME")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Time[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Timestamp, err = normalizeIntRange(c.Timestamp, "Types.Distribution.TIMESTAMP")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Timestamp[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Tinyblob, err = normalizeIntRange(c.Tinyblob, "Types.Distribution.TINYBLOB")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Tinyblob[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Tinyint, err = normalizeIntRange(c.Tinyint, "Types.Distribution.TINYINT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Tinyint[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.TinyintUnsigned, err = normalizeIntRange(c.TinyintUnsigned, "Types.Distribution.TINYINT_UNSIGNED")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.TinyintUnsigned[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Tinytext, err = normalizeIntRange(c.Tinytext, "Types.Distribution.TINYTEXT")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Tinytext[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Varbinary, err = normalizeIntRange(c.Varbinary, "Types.Distribution.VARBINARY")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Varbinary[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Varchar, err = normalizeIntRange(c.Varchar, "Types.Distribution.VARCHAR")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Varchar[0] > 0 {
		atLeastOneLowerbound = true
	}
	c.Year, err = normalizeIntRange(c.Year, "Types.Distribution.YEAR")
	if err != nil {
		return errors.Wrap(err)
	}
	if c.Year[0] > 0 {
		atLeastOneLowerbound = true
	}
	if !atLeastOneLowerbound {
		return errors.New(fmt.Sprintf(errDistLowerbound, "Types.Distribution"))
	}
	return nil
}

// checkCollations checks that all of the collations are valid.
func checkCollations(collations []string, collationFieldName string) ([]string, error) {
	if len(collations) == 0 {
		return []string{sql.Collation_Default.String()}, nil
	}
	for i := range collations {
		_, err := sql.ParseCollation(nil, &collations[i], false)
		if err != nil {
			return collations, errors.Wrap(err)
		}
	}
	return collations, nil
}
