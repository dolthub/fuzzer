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

// Tinytext represents the TINYTEXT MySQL type.
type Tinytext struct {
	Distribution ranges.Int
}

var _ Type = (*Tinytext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Tinytext) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Tinytext) Instance() (TypeInstance, error) {
	return &TinytextInstance{ranges.NewInt([]int64{0, 63})}, nil
}

// TinytextInstance is the TypeInstance of Tinytext.
type TinytextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*TinytextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinytextInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return TinytextValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *TinytextInstance) TypeValue() Value {
	return TinytextValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *TinytextInstance) Name(sqlite bool) string {
	return "TINYTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinytextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 64)
}

// TinytextValue is the Value type of a TinytextInstance.
type TinytextValue struct {
	StringValue
}

var _ Value = TinytextValue{}

// Convert implements the Value interface.
func (v TinytextValue) Convert(val interface{}) (Value, error) {
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
func (v TinytextValue) Name() string {
	return "TINYTEXT"
}

// MySQLString implements the Value interface.
func (v TinytextValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TinytextValue) SQLiteString() string {
	return v.String()
}
