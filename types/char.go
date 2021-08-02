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
	"strings"
	"unicode"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Char represents the CHAR MySQL type.
type Char struct {
	Collations   []string
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Char)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (c *Char) GetOccurrenceRate() (int64, error) {
	return c.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (c *Char) Instance() (TypeInstance, error) {
	charLength, err := c.Length.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos %= uint64(len(c.Collations))
	collation, err := sql.ParseCollation(nil, &c.Collations[colPos], false)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &CharInstance{ranges.NewInt([]int64{0, charLength}), collation}, nil
}

// CharInstance is the TypeInstance of Char.
type CharInstance struct {
	length    ranges.Int
	collation sql.Collation
}

var _ TypeInstance = (*CharInstance)(nil)

// Get implements the TypeInstance interface.
func (i *CharInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v = strings.TrimRightFunc(v, unicode.IsSpace)
	return CharValue{StringValue(v), int(i.length.Upperbound)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *CharInstance) TypeValue() Value {
	return CharValue{StringValue(""), int(i.length.Upperbound)}
}

// Name implements the TypeInstance interface.
func (i *CharInstance) Name(sqlite bool) string {
	if sqlite {
		return fmt.Sprintf("CHAR(%d)", i.length.Upperbound)
	}
	return fmt.Sprintf("CHAR(%d) COLLATE %s", i.length.Upperbound, i.collation.String())
}

// MaxValueCount implements the TypeInstance interface.
func (i *CharInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.length.Upperbound))
}

// CharValue is the Value type of a CharInstance.
type CharValue struct {
	StringValue
	charLength int
}

var _ Value = CharValue{}

// Convert implements the Value interface.
func (v CharValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case string:
		v.StringValue = StringValue(strings.TrimSuffix(val, " "))
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v CharValue) Name() string {
	return "CHAR"
}

// MySQLString implements the Value interface.
func (v CharValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v CharValue) SQLiteString() string {
	return v.String()
}
