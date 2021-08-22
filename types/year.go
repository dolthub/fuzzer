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

// Year represents the YEAR MySQL type.
type Year struct {
	Distribution ranges.Int
}

var _ Type = (*Year)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Year) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Year) Instance() (TypeInstance, error) {
	return &YearInstance{}, nil
}

// YearInstance is the TypeInstance of Year.
type YearInstance struct{}

var _ TypeInstance = (*YearInstance)(nil)

// Get implements the TypeInstance interface.
func (i *YearInstance) Get() (Value, error) {
	v, err := rand.Uint8()
	return YearValue{Uint16Value(v%254) + 1901}, err
}

// TypeValue implements the TypeInstance interface.
func (i *YearInstance) TypeValue() Value {
	return YearValue{Uint16Value(0)}
}

// Name implements the TypeInstance interface.
func (i *YearInstance) Name(sqlite bool) string {
	return "YEAR"
}

// MaxValueCount implements the TypeInstance interface.
func (i *YearInstance) MaxValueCount() float64 {
	return float64(math.MaxUint8)
}

// YearValue is the Value type of a YearInstance.
type YearValue struct {
	Uint16Value
}

var _ Value = YearValue{}

// Convert implements the Value interface.
func (v YearValue) Convert(val interface{}) (Value, error) {
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
func (v YearValue) Name() string {
	return "YEAR"
}

// MySQLString implements the Value interface.
func (v YearValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v YearValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v YearValue) CSVString() string {
	return v.String()
}
