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
	"strconv"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Mediumint represents the MEDIUMINT MySQL type.
type Mediumint struct {
	Distribution ranges.Int
}

var _ Type = (*Mediumint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumint) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumint) Instance() (TypeInstance, error) {
	return &MediumintInstance{}, nil
}

// MediumintInstance is the TypeInstance of Mediumint.
type MediumintInstance struct{}

var _ TypeInstance = (*MediumintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumintInstance) Get() (Value, error) {
	v, err := rand.Int32()
	return MediumintValue{Int32Value(v % 8388607)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumintInstance) TypeValue() Value {
	return MediumintValue{Int32Value(0)}
}

// Name implements the TypeInstance interface.
func (i *MediumintInstance) Name(sqlite bool) string {
	return "MEDIUMINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumintInstance) MaxValueCount() float64 {
	return float64(16777216)
}

// MediumintValue is the Value type of a MediumintInstance.
type MediumintValue struct {
	Int32Value
}

var _ Value = MediumintValue{}

// Convert implements the Value interface.
func (v MediumintValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Int32Value = Int32Value(val)
	case int:
		v.Int32Value = Int32Value(val)
	case uint8:
		v.Int32Value = Int32Value(val)
	case int8:
		v.Int32Value = Int32Value(val)
	case uint16:
		v.Int32Value = Int32Value(val)
	case int16:
		v.Int32Value = Int32Value(val)
	case uint32:
		v.Int32Value = Int32Value(val)
	case int32:
		v.Int32Value = Int32Value(val)
	case uint64:
		v.Int32Value = Int32Value(val)
	case int64:
		v.Int32Value = Int32Value(val)
	case string:
		pVal, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int32Value = Int32Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&val)), 10, 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int32Value = Int32Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v MediumintValue) Name() string {
	return "MEDIUMINT"
}

// MySQLString implements the Value interface.
func (v MediumintValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v MediumintValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v MediumintValue) CSVString() string {
	return v.String()
}
