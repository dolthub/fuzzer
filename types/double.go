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

// Double represents the DOUBLE MySQL type.
type Double struct {
	Distribution ranges.Int
}

var _ Type = (*Double)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (d *Double) GetOccurrenceRate() (int64, error) {
	return d.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (d *Double) Instance() (TypeInstance, error) {
	return &DoubleInstance{}, nil
}

// DoubleInstance is the TypeInstance of Double.
type DoubleInstance struct{}

var _ TypeInstance = (*DoubleInstance)(nil)

// Get implements the TypeInstance interface.
func (i *DoubleInstance) Get() (Value, error) {
	v, err := rand.Float64()
	return DoubleValue{Float64Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *DoubleInstance) TypeValue() Value {
	return DoubleValue{Float64Value(0)}
}

// Name implements the TypeInstance interface.
func (i *DoubleInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(20)"
	}
	return "DOUBLE"
}

// MaxValueCount implements the TypeInstance interface.
func (i *DoubleInstance) MaxValueCount() float64 {
	return float64(math.MaxUint64)
}

// DoubleValue is the Value type of a DoubleInstance.
type DoubleValue struct {
	Float64Value
}

var _ Value = DoubleValue{}

// Convert implements the Value interface.
func (v DoubleValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case float32:
		v.Float64Value = Float64Value(val)
	case float64:
		v.Float64Value = Float64Value(val)
	case string:
		// Only SQLite returns a string, so we can reverse our bit conversion. See SQLiteString for details.
		bitPattern := uint64(0)
		for i := 0; i < len(val); i++ {
			bitPattern = (bitPattern * 10) + uint64(val[i]-'0')
		}
		bitPattern ^= (0x8000000000000000 * (bitPattern >> 63)) + (0xffffffffffffffff - (0xffffffffffffffff * (bitPattern >> 63)))
		v.Float64Value = *(*Float64Value)(unsafe.Pointer(&bitPattern))
	case []uint8:
		pVal, err := strconv.ParseFloat(*(*string)(unsafe.Pointer(&val)), 64)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Float64Value = Float64Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v DoubleValue) Name() string {
	return "DOUBLE"
}

// MySQLString implements the Value interface.
func (v DoubleValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v DoubleValue) SQLiteString() string {
	// SQLite doesn't store floats correctly, so we convert to bits and store those.
	bitPattern := *(*uint64)(unsafe.Pointer(&v.Float64Value))
	// Negative numbers sort after positive and in reverse, so this ensures correct sorting
	bitPattern ^= (0xffffffffffffffff * (bitPattern >> 63)) + (0x8000000000000000 - (0x8000000000000000 * (bitPattern >> 63)))
	return formatUint64Sqlite(bitPattern)
}
