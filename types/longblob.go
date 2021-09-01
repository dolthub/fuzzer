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

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Longblob represents the LONGBLOB MySQL type.
type Longblob struct {
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Longblob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (l *Longblob) GetOccurrenceRate() (int64, error) {
	return l.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (l *Longblob) Instance() (TypeInstance, error) {
	return &LongblobInstance{l.Length}, nil
}

// LongblobInstance is the TypeInstance of Longblob.
type LongblobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*LongblobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *LongblobInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return LongblobValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *LongblobInstance) TypeValue() Value {
	return LongblobValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *LongblobInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return "LONGBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *LongblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.length.Upperbound))
}

// LongblobValue is the Value type of a LongblobInstance.
type LongblobValue struct {
	StringValue
}

var _ Value = LongblobValue{}

// Convert implements the Value interface.
func (v LongblobValue) Convert(val interface{}) (Value, error) {
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
func (v LongblobValue) Name() string {
	return "LONGBLOB"
}

// MySQLString implements the Value interface.
func (v LongblobValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v LongblobValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v LongblobValue) CSVString() string {
	return v.StringTerminating(34)
}
