package types

import (
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
	return StringValue(t.UTC().Format("2006-01-02 15:04:05")), nil
}

// TypeValue implements the TypeInstance interface.
func (i *TimestampInstance) TypeValue() Value {
	return StringValue("")
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
