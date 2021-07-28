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

// BigintUnsigned represents the BIGINT UNSIGNED MySQL type.
type BigintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*BigintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *BigintUnsigned) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *BigintUnsigned) Instance() (TypeInstance, error) {
	return &BigintUnsignedInstance{}, nil
}

// BigintUnsignedInstance is the TypeInstance of BigintUnsigned.
type BigintUnsignedInstance struct{}

var _ TypeInstance = (*BigintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BigintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	return BigintUnsignedValue{Uint64Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *BigintUnsignedInstance) TypeValue() Value {
	return BigintUnsignedValue{Uint64Value(0)}
}

// Name implements the TypeInstance interface.
func (i *BigintUnsignedInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(20)"
	}
	return "BIGINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BigintUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint64)
}

// BigintUnsignedValue is the Value type of a BigintUnsignedInstance.
type BigintUnsignedValue struct {
	Uint64Value
}

var _ Value = BigintUnsignedValue{}

// Convert implements the Value interface.
func (v BigintUnsignedValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Uint64Value = Uint64Value(val)
	case int:
		v.Uint64Value = Uint64Value(val)
	case uint8:
		v.Uint64Value = Uint64Value(val)
	case int8:
		v.Uint64Value = Uint64Value(val)
	case uint16:
		v.Uint64Value = Uint64Value(val)
	case int16:
		v.Uint64Value = Uint64Value(val)
	case uint32:
		v.Uint64Value = Uint64Value(val)
	case int32:
		v.Uint64Value = Uint64Value(val)
	case uint64:
		v.Uint64Value = Uint64Value(val)
	case int64:
		v.Uint64Value = Uint64Value(val)
	case string:
		// This code assumes that the string perfectly represents an uint64
		n := uint64(0)
		for i := 0; i < len(val); i++ {
			n = (n * 10) + uint64(val[i]-'0')
		}
		v.Uint64Value = Uint64Value(n)
	case []uint8:
		pVal, err := strconv.ParseUint(*(*string)(unsafe.Pointer(&val)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint64Value = Uint64Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v BigintUnsignedValue) Name() string {
	return "BIGINT UNSIGNED"
}

// MySQLString implements the Value interface.
func (v BigintUnsignedValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v BigintUnsignedValue) SQLiteString() string {
	return formatUint64Sqlite(uint64(v.Uint64Value))
}
