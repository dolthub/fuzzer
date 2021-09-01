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

// Varchar represents the VARCHAR MySQL type.
type Varchar struct {
	Collations   []string
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Varchar)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (v *Varchar) GetOccurrenceRate() (int64, error) {
	return v.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (v *Varchar) Instance() (TypeInstance, error) {
	colPos, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos %= uint64(len(v.Collations))
	collation, err := sql.ParseCollation(nil, &v.Collations[colPos], false)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	charLength := utils.MinInt64(v.Length.Upperbound, 65535/collation.CharSet.MaxLength())
	return &VarcharInstance{ranges.NewInt([]int64{v.Length.Lowerbound, charLength}), collation}, nil
}

// VarcharInstance is the TypeInstance of Varchar.
type VarcharInstance struct {
	length    ranges.Int
	collation sql.Collation
}

var _ TypeInstance = (*VarcharInstance)(nil)

// Get implements the TypeInstance interface.
func (i *VarcharInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return VarcharValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *VarcharInstance) TypeValue() Value {
	return VarcharValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *VarcharInstance) Name(sqlite bool) string {
	if sqlite {
		return fmt.Sprintf("VARCHAR(%d)", i.length.Upperbound)
	}
	return fmt.Sprintf("VARCHAR(%d) COLLATE %s", i.length.Upperbound, i.collation.String())
}

// MaxValueCount implements the TypeInstance interface.
func (i *VarcharInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.length.Upperbound))
}

// VarcharValue is the Value type of a VarcharInstance.
type VarcharValue struct {
	StringValue
}

var _ Value = VarcharValue{}

// Convert implements the Value interface.
func (v VarcharValue) Convert(val interface{}) (Value, error) {
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
func (v VarcharValue) Name() string {
	return "VARCHAR"
}

// MySQLString implements the Value interface.
func (v VarcharValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v VarcharValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v VarcharValue) CSVString() string {
	return v.StringTerminating(34)
}
