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

// Longtext represents the LONGTEXT MySQL type.
type Longtext struct {
	Distribution ranges.Int
}

var _ Type = (*Longtext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (l *Longtext) GetOccurrenceRate() (int64, error) {
	return l.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (l *Longtext) Instance() (TypeInstance, error) {
	return &LongtextInstance{ranges.NewInt([]int64{1, 4294967295})}, nil
}

// LongtextInstance is the TypeInstance of Longtext.
type LongtextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*LongtextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *LongtextInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return LongtextValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *LongtextInstance) TypeValue() Value {
	return LongtextValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *LongtextInstance) Name(sqlite bool) string {
	return "LONGTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *LongtextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 4294967296)
}

// LongtextValue is the Value type of a LongtextInstance.
type LongtextValue struct {
	StringValue
}

var _ Value = LongtextValue{}

// Convert implements the Value interface.
func (v LongtextValue) Convert(val interface{}) (Value, error) {
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
func (v LongtextValue) Name() string {
	return "LONGTEXT"
}

// MySQLString implements the Value interface.
func (v LongtextValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v LongtextValue) SQLiteString() string {
	return v.String()
}
