package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Mediumblob represents the MEDIUMBLOB MySQL type.
type Mediumblob struct {
	Distribution ranges.Int
}

var _ Type = (*Mediumblob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumblob) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumblob) Instance() (TypeInstance, error) {
	return &MediumblobInstance{ranges.NewInt([]int64{0, 16777215})}, nil
}

// MediumblobInstance is the TypeInstance of Mediumblob.
type MediumblobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*MediumblobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumblobInstance) Get() (Value, error) {
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
func (i *MediumblobInstance) Name() string {
	return "MEDIUMBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 16777216)
}
