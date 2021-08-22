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
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Time represents the TIME MySQL type.
type Time struct {
	Distribution ranges.Int
}

var _ Type = (*Time)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Time) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Time) Instance() (TypeInstance, error) {
	return &TimeInstance{}, nil
}

// TimeInstance is the TypeInstance of Time.
type TimeInstance struct{}

var _ TypeInstance = (*TimeInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TimeInstance) Get() (Value, error) {
	v, err := rand.Int32()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v = v % 3020399
	vAbs := v
	neg := ""
	if vAbs < 0 {
		vAbs *= -1
		neg = "-"
	}
	return TimeValue{StringValue(fmt.Sprintf("%s%02d:%02d:%02d", neg, vAbs/3600, (vAbs/60)%60, vAbs%60))}, nil
}

// TypeValue implements the TypeInstance interface.
func (i *TimeInstance) TypeValue() Value {
	return TimeValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *TimeInstance) Name(sqlite bool) string {
	if sqlite {
		return "BIGINT"
	}
	return "TIME"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TimeInstance) MaxValueCount() float64 {
	return float64(6040798)
}

// TimeValue is the Value type of a TimeInstance.
type TimeValue struct {
	StringValue
}

var _ Value = TimeValue{}

// Convert implements the Value interface.
func (v TimeValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case int64:
		vAbs := val
		neg := ""
		if vAbs < 0 {
			vAbs *= -1
			neg = "-"
		}
		v.StringValue = StringValue(fmt.Sprintf("%s%02d:%02d:%02d", neg, vAbs/3600, (vAbs/60)%60, vAbs%60))
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v TimeValue) Name() string {
	return "TIME"
}

// Compare implements the ValuePrimitive interface. This overrides the inner primitive's Compare function, as the inner
// value does not sort properly based on the specific properties of this value.
func (v TimeValue) Compare(other ValuePrimitive) int {
	if otherTime, ok := other.(TimeValue); ok {
		vVal := v.ToInt64Value()
		otherVal := otherTime.ToInt64Value()
		if vVal < otherVal {
			return -1
		} else if vVal > otherVal {
			return 1
		}
		return 0
	}
	return v.StringValue.Compare(other)
}

// MySQLString implements the Value interface.
func (v TimeValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TimeValue) SQLiteString() string {
	return v.ToInt64Value().String()
}

// CSVString implements the interface Value.
func (v TimeValue) CSVString() string {
	return v.StringTerminating(34)
}

// ToInt64Value returns this value as an Int64Value.
func (v TimeValue) ToInt64Value() Int64Value {
	divisions := strings.Split(string(v.StringValue), ":")
	negativeMult := int64(1)
	if divisions[0][0] == '-' {
		divisions[0] = divisions[0][1:]
		negativeMult = -1
	}
	hour := int64(0)
	for i := 0; i < len(divisions[0]); i++ {
		hour = (hour * 10) + int64(divisions[0][i]-'0')
	}
	minute := int64(0)
	for i := 0; i < len(divisions[1]); i++ {
		minute = (minute * 10) + int64(divisions[1][i]-'0')
	}
	second := int64(0)
	for i := 0; i < len(divisions[2]); i++ {
		second = (second * 10) + int64(divisions[2][i]-'0')
	}
	return Int64Value(negativeMult * ((hour * 3600) + (minute * 60) + second))
}
