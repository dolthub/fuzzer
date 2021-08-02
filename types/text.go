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

// Text represents the TEXT MySQL type.
type Text struct {
	Collations   []string
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Text)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Text) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Text) Instance() (TypeInstance, error) {
	charLength, err := t.Length.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos %= uint64(len(t.Collations))
	collation, err := sql.ParseCollation(nil, &t.Collations[colPos], false)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	charLength = utils.MinInt64(charLength, 65535/collation.CharSet.MaxLength())
	return &TextInstance{ranges.NewInt([]int64{0, charLength}), collation}, nil
}

// TextInstance is the TypeInstance of Text.
type TextInstance struct {
	length    ranges.Int
	collation sql.Collation
}

var _ TypeInstance = (*TextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TextInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return TextValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *TextInstance) TypeValue() Value {
	return TextValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *TextInstance) Name(sqlite bool) string {
	return "TEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.length.Upperbound))
}

// TextValue is the Value type of a TextInstance.
type TextValue struct {
	StringValue
}

var _ Value = TextValue{}

// Convert implements the Value interface.
func (v TextValue) Convert(val interface{}) (Value, error) {
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
func (v TextValue) Name() string {
	return "TEXT"
}

// MySQLString implements the Value interface.
func (v TextValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TextValue) SQLiteString() string {
	return v.String()
}
