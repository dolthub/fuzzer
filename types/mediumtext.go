package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Mediumtext represents the MEDIUMTEXT MySQL type.
type Mediumtext struct {
	Distribution ranges.Int
}

var _ Type = (*Mediumtext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumtext) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumtext) Instance() (TypeInstance, error) {
	return &MediumtextInstance{ranges.NewInt([]int64{0, 4194303})}, nil
}

// MediumtextInstance is the TypeInstance of Mediumtext.
type MediumtextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*MediumtextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumtextInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return StringValue(v), err
}

// Name implements the TypeInstance interface.
func (i *MediumtextInstance) Name() string {
	return "MEDIUMTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumtextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 4194304)
}
