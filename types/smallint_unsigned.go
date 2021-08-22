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

// SmallintUnsigned represents the SMALLINT UNSIGNED MySQL type.
type SmallintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*SmallintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (s *SmallintUnsigned) GetOccurrenceRate() (int64, error) {
	return s.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (s *SmallintUnsigned) Instance() (TypeInstance, error) {
	return &SmallintUnsignedInstance{}, nil
}

// SmallintUnsignedInstance is the TypeInstance of SmallintUnsigned.
type SmallintUnsignedInstance struct{}

var _ TypeInstance = (*SmallintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint16()
	return SmallintUnsignedValue{Uint16Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) TypeValue() Value {
	return SmallintUnsignedValue{Uint16Value(0)}
}

// Name implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) Name(sqlite bool) string {
	return "SMALLINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint16)
}

// SmallintUnsignedValue is the Value type of a SmallintUnsignedInstance.
type SmallintUnsignedValue struct {
	Uint16Value
}

var _ Value = SmallintUnsignedValue{}

// Convert implements the Value interface.
func (v SmallintUnsignedValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Uint16Value = Uint16Value(val)
	case int:
		v.Uint16Value = Uint16Value(val)
	case uint8:
		v.Uint16Value = Uint16Value(val)
	case int8:
		v.Uint16Value = Uint16Value(val)
	case uint16:
		v.Uint16Value = Uint16Value(val)
	case int16:
		v.Uint16Value = Uint16Value(val)
	case uint32:
		v.Uint16Value = Uint16Value(val)
	case int32:
		v.Uint16Value = Uint16Value(val)
	case uint64:
		v.Uint16Value = Uint16Value(val)
	case int64:
		v.Uint16Value = Uint16Value(val)
	case string:
		pVal, err := strconv.ParseUint(val, 10, 16)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint16Value = Uint16Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseUint(*(*string)(unsafe.Pointer(&val)), 10, 16)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint16Value = Uint16Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v SmallintUnsignedValue) Name() string {
	return "SMALLINT UNSIGNED"
}

// MySQLString implements the Value interface.
func (v SmallintUnsignedValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v SmallintUnsignedValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v SmallintUnsignedValue) CSVString() string {
	return v.String()
}
