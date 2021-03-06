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

package types

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
	"github.com/dolthub/fuzzer/utils"
)

// Mediumtext represents the MEDIUMTEXT MySQL type.
type Mediumtext struct {
	Collations   []string
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Mediumtext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumtext) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumtext) Instance() (TypeInstance, error) {
	colPos, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos %= uint64(len(m.Collations))
	collation, err := sql.ParseCollation(nil, &m.Collations[colPos], false)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	charLength := utils.MinInt64(m.Length.Upperbound, 16777215/collation.CharSet.MaxLength())
	return &MediumtextInstance{ranges.NewInt([]int64{m.Length.Lowerbound, charLength}), collation}, nil
}

// MediumtextInstance is the TypeInstance of Mediumtext.
type MediumtextInstance struct {
	length    ranges.Int
	collation sql.Collation
}

var _ TypeInstance = (*MediumtextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumtextInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return MediumtextValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumtextInstance) TypeValue() Value {
	return MediumtextValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *MediumtextInstance) Name(sqlite bool) string {
	return "MEDIUMTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumtextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.length.Upperbound))
}

// MediumtextValue is the Value type of a MediumtextInstance.
type MediumtextValue struct {
	StringValue
}

var _ Value = MediumtextValue{}

// Convert implements the Value interface.
func (v MediumtextValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case string:
		v.StringValue = StringValue(val)
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v MediumtextValue) Name() string {
	return "MEDIUMTEXT"
}

// MySQLString implements the Value interface.
func (v MediumtextValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v MediumtextValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v MediumtextValue) CSVString() string {
	return v.StringTerminating(34)
}
