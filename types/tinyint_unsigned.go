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

// TinyintUnsigned represents the TINYINT UNSIGNED MySQL type.
type TinyintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*TinyintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *TinyintUnsigned) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *TinyintUnsigned) Instance() (TypeInstance, error) {
	return &TinyintUnsignedInstance{}, nil
}

// TinyintUnsignedInstance is the TypeInstance of TinyintUnsigned.
type TinyintUnsignedInstance struct{}

var _ TypeInstance = (*TinyintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint8()
	return TinyintUnsignedValue{Uint8Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) TypeValue() Value {
	return TinyintUnsignedValue{Uint8Value(0)}
}

// Name implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) Name(sqlite bool) string {
	return "TINYINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint8)
}

// TinyintUnsignedValue is the Value type of a TinyintUnsignedInstance.
type TinyintUnsignedValue struct {
	Uint8Value
}

var _ Value = TinyintUnsignedValue{}

// Convert implements the Value interface.
func (v TinyintUnsignedValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Uint8Value = Uint8Value(val)
	case int:
		v.Uint8Value = Uint8Value(val)
	case uint8:
		v.Uint8Value = Uint8Value(val)
	case int8:
		v.Uint8Value = Uint8Value(val)
	case uint16:
		v.Uint8Value = Uint8Value(val)
	case int16:
		v.Uint8Value = Uint8Value(val)
	case uint32:
		v.Uint8Value = Uint8Value(val)
	case int32:
		v.Uint8Value = Uint8Value(val)
	case uint64:
		v.Uint8Value = Uint8Value(val)
	case int64:
		v.Uint8Value = Uint8Value(val)
	case string:
		pVal, err := strconv.ParseUint(val, 10, 8)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint8Value = Uint8Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseUint(*(*string)(unsafe.Pointer(&val)), 10, 8)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint8Value = Uint8Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v TinyintUnsignedValue) Name() string {
	return "TINYINT UNSIGNED"
}

// MySQLString implements the Value interface.
func (v TinyintUnsignedValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TinyintUnsignedValue) SQLiteString() string {
	return v.String()
}
