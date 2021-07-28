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

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Varbinary represents the VARBINARY MySQL type.
type Varbinary struct {
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Varbinary)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (v *Varbinary) GetOccurrenceRate() (int64, error) {
	return v.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (v *Varbinary) Instance() (TypeInstance, error) {
	charLength, err := v.Length.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &VarbinaryInstance{int(charLength), ranges.NewInt([]int64{0, charLength})}, nil
}

// VarbinaryInstance is the TypeInstance of Varbinary.
type VarbinaryInstance struct {
	charLength int
	length     ranges.Int
}

var _ TypeInstance = (*VarbinaryInstance)(nil)

// Get implements the TypeInstance interface.
func (i *VarbinaryInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return VarbinaryValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *VarbinaryInstance) TypeValue() Value {
	return VarbinaryValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *VarbinaryInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return fmt.Sprintf("VARBINARY(%d)", i.charLength)
}

// MaxValueCount implements the TypeInstance interface.
func (i *VarbinaryInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.charLength))
}

// VarbinaryValue is the Value type of a VarbinaryInstance.
type VarbinaryValue struct {
	StringValue
}

var _ Value = VarbinaryValue{}

// Convert implements the Value interface.
func (v VarbinaryValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case string:
		v.StringValue = StringValue(val)
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v VarbinaryValue) Name() string {
	return "VARBINARY"
}

// MySQLString implements the Value interface.
func (v VarbinaryValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v VarbinaryValue) SQLiteString() string {
	return v.String()
}
