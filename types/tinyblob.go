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

// Tinyblob represents the TINYBLOB MySQL type.
type Tinyblob struct {
	Distribution ranges.Int
}

var _ Type = (*Tinyblob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Tinyblob) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Tinyblob) Instance() (TypeInstance, error) {
	return &TinyblobInstance{ranges.NewInt([]int64{0, 255})}, nil
}

// TinyblobInstance is the TypeInstance of Tinyblob.
type TinyblobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*TinyblobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinyblobInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return TinyblobValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *TinyblobInstance) TypeValue() Value {
	return TinyblobValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *TinyblobInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return "TINYBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinyblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 256)
}

// TinyblobValue is the Value type of a TinyblobInstance.
type TinyblobValue struct {
	StringValue
}

var _ Value = TinyblobValue{}

// Convert implements the Value interface.
func (v TinyblobValue) Convert(val interface{}) (Value, error) {
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
func (v TinyblobValue) Name() string {
	return "TINYBLOB"
}

// MySQLString implements the Value interface.
func (v TinyblobValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TinyblobValue) SQLiteString() string {
	return v.String()
}
