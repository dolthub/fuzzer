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

// Tinyint represents the TINYINT MySQL type.
type Tinyint struct {
	Distribution ranges.Int
}

var _ Type = (*Tinyint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Tinyint) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Tinyint) Instance() (TypeInstance, error) {
	return &TinyintInstance{}, nil
}

// TinyintInstance is the TypeInstance of Tinyint.
type TinyintInstance struct{}

var _ TypeInstance = (*TinyintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinyintInstance) Get() (Value, error) {
	v, err := rand.Int8()
	return TinyintValue{Int8Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *TinyintInstance) TypeValue() Value {
	return TinyintValue{Int8Value(0)}
}

// Name implements the TypeInstance interface.
func (i *TinyintInstance) Name(sqlite bool) string {
	return "TINYINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinyintInstance) MaxValueCount() float64 {
	return float64(math.MaxUint8)
}

// TinyintValue is the Value type of a TinyintInstance.
type TinyintValue struct {
	Int8Value
}

var _ Value = TinyintValue{}

// Convert implements the Value interface.
func (v TinyintValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Int8Value = Int8Value(val)
	case int:
		v.Int8Value = Int8Value(val)
	case uint8:
		v.Int8Value = Int8Value(val)
	case int8:
		v.Int8Value = Int8Value(val)
	case uint16:
		v.Int8Value = Int8Value(val)
	case int16:
		v.Int8Value = Int8Value(val)
	case uint32:
		v.Int8Value = Int8Value(val)
	case int32:
		v.Int8Value = Int8Value(val)
	case uint64:
		v.Int8Value = Int8Value(val)
	case int64:
		v.Int8Value = Int8Value(val)
	case string:
		pVal, err := strconv.ParseInt(val, 10, 8)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int8Value = Int8Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&val)), 10, 8)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int8Value = Int8Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v TinyintValue) Name() string {
	return "TINYINT"
}

// MySQLString implements the Value interface.
func (v TinyintValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TinyintValue) SQLiteString() string {
	return v.String()
}
