package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Smallint represents the SMALLINT MySQL type.
type Smallint struct {
	Distribution ranges.Int
}

var _ Type = (*Smallint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (s *Smallint) GetOccurrenceRate() (int64, error) {
	return s.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (s *Smallint) Instance() (TypeInstance, error) {
	return &SmallintInstance{}, nil
}

// SmallintInstance is the TypeInstance of Smallint.
type SmallintInstance struct{}

var _ TypeInstance = (*SmallintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *SmallintInstance) Get() (Value, error) {
	v, err := rand.Int16()
	return Int16Value(v), err
}

// Name implements the TypeInstance interface.
func (i *SmallintInstance) Name() string {
	return "SMALLINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *SmallintInstance) MaxValueCount() float64 {
	return float64(math.MaxUint16)
}
