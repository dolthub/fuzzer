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
	"time"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Date represents the DATE MySQL type.
type Date struct {
	Distribution ranges.Int
}

var _ Type = (*Date)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (d *Date) GetOccurrenceRate() (int64, error) {
	return d.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (d *Date) Instance() (TypeInstance, error) {
	return &DateInstance{}, nil
}

// DateInstance is the TypeInstance of Date.
type DateInstance struct{}

var _ TypeInstance = (*DateInstance)(nil)

// Get implements the TypeInstance interface.
func (i *DateInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	t := time.Unix(int64(v%(maxDatetime-minDatetime))+minDatetime, 0)
	return DateValue{StringValue(t.Format("2006-01-02"))}, nil
}

// TypeValue implements the TypeInstance interface.
func (i *DateInstance) TypeValue() Value {
	return DateValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *DateInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(100)"
	}
	return "DATE"
}

// MaxValueCount implements the TypeInstance interface.
func (i *DateInstance) MaxValueCount() float64 {
	return float64(3284635)
}

// DateValue is the Value type of a DateInstance.
type DateValue struct {
	StringValue
}

var _ Value = DateValue{}

// Convert implements the Value interface.
func (v DateValue) Convert(val interface{}) (Value, error) {
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
func (v DateValue) Name() string {
	return "DATE"
}

// MySQLString implements the Value interface.
func (v DateValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v DateValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v DateValue) CSVString() string {
	return v.StringTerminating(34)
}
