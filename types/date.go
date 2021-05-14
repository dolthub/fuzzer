package types

import (
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
	return StringValue(t.Format("2006-01-02")), nil
}

// TypeValue implements the TypeInstance interface.
func (i *DateInstance) TypeValue() Value {
	return StringValue("")
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
