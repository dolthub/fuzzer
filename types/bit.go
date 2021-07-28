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

// Bit represents the BIT MySQL type.
type Bit struct {
	Distribution ranges.Int
	Width        ranges.Int
}

var _ Type = (*Bit)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Bit) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Bit) Instance() (TypeInstance, error) {
	width, err := b.Width.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &BitInstance{uint64(width)}, nil
}

// BitInstance is the TypeInstance of Bit.
type BitInstance struct {
	width uint64
}

var _ TypeInstance = (*BitInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BitInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	if i.width >= 64 {
		return BitValue{Uint64Value(v)}, nil
	}
	return BitValue{Uint64Value(v % (1 << i.width))}, err
}

// TypeValue implements the TypeInstance interface.
func (i *BitInstance) TypeValue() Value {
	return BitValue{Uint64Value(0)}
}

// Name implements the TypeInstance interface.
func (i *BitInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(20)"
	}
	return fmt.Sprintf("BIT(%d)", i.width)
}

// MaxValueCount implements the TypeInstance interface.
func (i *BitInstance) MaxValueCount() float64 {
	return math.Pow(2, float64(i.width))
}

// BitValue is the Value type of a BitInstance.
type BitValue struct {
	Uint64Value
}

var _ Value = BitValue{}

// Convert implements the Value interface.
func (v BitValue) Convert(val interface{}) (Value, error) {
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
func (v BitValue) Name() string {
	return "Bit"
}

// MySQLString implements the Value interface.
func (v BitValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v BitValue) SQLiteString() string {
	return formatUint64Sqlite(uint64(v.Uint64Value))
}
