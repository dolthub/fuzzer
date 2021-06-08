package types

import (
	"fmt"

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
	if vAbs < 0 {
		vAbs *= -1
	}
	return TimeValue{StringValue(fmt.Sprintf("%d:%02d:%02d", v/3600, (vAbs/60)%60, vAbs%60))}, nil
}

// TypeValue implements the TypeInstance interface.
func (i *TimeInstance) TypeValue() Value {
	return TimeValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *TimeInstance) Name(sqlite bool) string {
	if sqlite {
		return fmt.Sprintf("VARCHAR(20)")
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
func (v TimeValue) Name() string {
	return "TIME"
}

// MySQLString implements the Value interface.
func (v TimeValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TimeValue) SQLiteString() string {
	return v.String()
}
