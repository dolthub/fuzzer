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

// Int represents the INT MySQL type.
type Int struct {
	Distribution ranges.Int
}

var _ Type = (*Int)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (i *Int) GetOccurrenceRate() (int64, error) {
	return i.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (i *Int) Instance() (TypeInstance, error) {
	return &IntInstance{}, nil
}

// IntInstance is the TypeInstance of Int.
type IntInstance struct{}

var _ TypeInstance = (*IntInstance)(nil)

// Get implements the TypeInstance interface.
func (i *IntInstance) Get() (Value, error) {
	v, err := rand.Int32()
	return IntValue{Int32Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *IntInstance) TypeValue() Value {
	return IntValue{Int32Value(0)}
}

// Name implements the TypeInstance interface.
func (i *IntInstance) Name(sqlite bool) string {
	return "INT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *IntInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}

// IntValue is the Value type of a IntInstance.
type IntValue struct {
	Int32Value
}

var _ Value = IntValue{}

// Convert implements the Value interface.
func (v IntValue) Convert(val interface{}) (Value, error) {
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
func (v IntValue) Name() string {
	return "INT"
}

// MySQLString implements the Value interface.
func (v IntValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v IntValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v IntValue) CSVString() string {
	return v.String()
}
