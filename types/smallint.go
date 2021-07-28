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
	"strconv"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Smallint represents the SMALLINT MySQL type.
type Smallint struct {
	Distribution ranges.Int
}

var _ Type = (*Smallint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (s *Smallint) GetOccurrenceRate() (int64, error) {
	return s.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (s *Smallint) Instance() (TypeInstance, error) {
	return &SmallintInstance{}, nil
}

// SmallintInstance is the TypeInstance of Smallint.
type SmallintInstance struct{}

var _ TypeInstance = (*SmallintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *SmallintInstance) Get() (Value, error) {
	v, err := rand.Int16()
	return SmallintValue{Int16Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *SmallintInstance) TypeValue() Value {
	return SmallintValue{Int16Value(0)}
}

// Name implements the TypeInstance interface.
func (i *SmallintInstance) Name(sqlite bool) string {
	return "SMALLINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *SmallintInstance) MaxValueCount() float64 {
	return float64(math.MaxUint16)
}

// SmallintValue is the Value type of a SmallintInstance.
type SmallintValue struct {
	Int16Value
}

var _ Value = SmallintValue{}

// Convert implements the Value interface.
func (v SmallintValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Int16Value = Int16Value(val)
	case int:
		v.Int16Value = Int16Value(val)
	case uint8:
		v.Int16Value = Int16Value(val)
	case int8:
		v.Int16Value = Int16Value(val)
	case uint16:
		v.Int16Value = Int16Value(val)
	case int16:
		v.Int16Value = Int16Value(val)
	case uint32:
		v.Int16Value = Int16Value(val)
	case int32:
		v.Int16Value = Int16Value(val)
	case uint64:
		v.Int16Value = Int16Value(val)
	case int64:
		v.Int16Value = Int16Value(val)
	case string:
		pVal, err := strconv.ParseInt(val, 10, 16)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int16Value = Int16Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&val)), 10, 16)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int16Value = Int16Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v SmallintValue) Name() string {
	return "SMALLINT"
}

// MySQLString implements the Value interface.
func (v SmallintValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v SmallintValue) SQLiteString() string {
	return v.String()
}
