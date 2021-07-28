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

// Float represents the FLOAT MySQL type.
type Float struct {
	Distribution ranges.Int
}

var _ Type = (*Float)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (f *Float) GetOccurrenceRate() (int64, error) {
	return f.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (f *Float) Instance() (TypeInstance, error) {
	return &FloatInstance{}, nil
}

// FloatInstance is the TypeInstance of Float.
type FloatInstance struct{}

var _ TypeInstance = (*FloatInstance)(nil)

// Get implements the TypeInstance interface.
func (i *FloatInstance) Get() (Value, error) {
	v, err := rand.Float32()
	return FloatValue{Float32Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *FloatInstance) TypeValue() Value {
	return FloatValue{Float32Value(0)}
}

// Name implements the TypeInstance interface.
func (i *FloatInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(20)"
	}
	return "FLOAT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *FloatInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}

// FloatValue is the Value type of a FloatInstance.
type FloatValue struct {
	Float32Value
}

var _ Value = FloatValue{}

// Convert implements the Value interface.
func (v FloatValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case float32:
		v.Float32Value = Float32Value(val)
	case float64:
		v.Float32Value = Float32Value(val)
	case string:
		// Only SQLite returns a string, so we can reverse our bit conversion. See SQLiteString for details.
		bitPattern := uint32(0)
		for i := 0; i < len(val); i++ {
			bitPattern = (bitPattern * 10) + uint32(val[i]-'0')
		}
		bitPattern ^= (0x80000000 * (bitPattern >> 31)) + (0xffffffff - (0xffffffff * (bitPattern >> 31)))
		v.Float32Value = *(*Float32Value)(unsafe.Pointer(&bitPattern))
	case []uint8:
		pVal, err := strconv.ParseFloat(*(*string)(unsafe.Pointer(&val)), 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Float32Value = Float32Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v FloatValue) Name() string {
	return "FLOAT"
}

// MySQLString implements the Value interface.
func (v FloatValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v FloatValue) SQLiteString() string {
	// SQLite is weird with floats, so we convert to bits and store those.
	bitPattern := *(*uint32)(unsafe.Pointer(&v.Float32Value))
	// Negative numbers sort after positive and in reverse, so this ensures correct sorting
	bitPattern ^= (0xffffffff * (bitPattern >> 31)) + (0x80000000 - (0x80000000 * (bitPattern >> 31)))
	return formatUint32Sqlite(bitPattern)
}
