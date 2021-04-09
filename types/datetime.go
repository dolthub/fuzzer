package types

import (
	"time"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

const (
	minDatetime = -30610224000
	maxDatetime = 253402300799
)

// Datetime represents the DATETIME MySQL type.
type Datetime struct {
	Distribution ranges.Int
}

var _ Type = (*Datetime)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (d *Datetime) GetOccurrenceRate() (int64, error) {
	return d.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (d *Datetime) Instance() (TypeInstance, error) {
	return &DatetimeInstance{}, nil
}

// DatetimeInstance is the TypeInstance of Datetime.
type DatetimeInstance struct{}

var _ TypeInstance = (*DatetimeInstance)(nil)

// Get implements the TypeInstance interface.
func (i *DatetimeInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	t := time.Unix(int64(v%(maxDatetime-minDatetime))+minDatetime, 0)
	return StringValue(t.Format("2006-01-02 15:04:05")), nil
}

// Name implements the TypeInstance interface.
func (i *DatetimeInstance) Name() string {
	return "DATETIME"
}

// MaxValueCount implements the TypeInstance interface.
func (i *DatetimeInstance) MaxValueCount() float64 {
	return float64(284012524799)
}
