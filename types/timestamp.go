package types

import (
	"fmt"
	"time"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

const (
	minTimestamp = 1
	maxTimestamp = 2147483647
)

// Timestamp represents the TIMESTAMP MySQL type.
type Timestamp struct {
	Distribution ranges.Int
}

var _ Type = (*Timestamp)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Timestamp) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Timestamp) Instance() (TypeInstance, error) {
	return &TimestampInstance{}, nil
}

// TimestampInstance is the TypeInstance of Timestamp.
type TimestampInstance struct{}

var _ TypeInstance = (*TimestampInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TimestampInstance) Get() (Value, error) {
	v, err := rand.Uint32()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	t := time.Unix(int64((v%(maxTimestamp-minTimestamp))+minTimestamp), 0)
	return TimestampValue{StringValue(t.UTC().Format("2006-01-02 15:04:05"))}, nil
}

// TypeValue implements the TypeInstance interface.
func (i *TimestampInstance) TypeValue() Value {
	return TimestampValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *TimestampInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(100)"
	}
	return "TIMESTAMP"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TimestampInstance) MaxValueCount() float64 {
	return float64(maxTimestamp - minTimestamp)
}

// TimestampValue is the Value type of a TimestampInstance.
type TimestampValue struct {
	StringValue
}

var _ Value = TimestampValue{}

// Convert implements the Value interface.
func (v TimestampValue) Convert(val interface{}) (Value, error) {
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
func (v TimestampValue) Name() string {
	return "TIMESTAMP"
}

// MySQLString implements the Value interface.
func (v TimestampValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v TimestampValue) SQLiteString() string {
	return v.String()
}
