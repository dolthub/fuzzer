package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// SmallintUnsigned represents the SMALLINT UNSIGNED MySQL type.
type SmallintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*SmallintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (s *SmallintUnsigned) GetOccurrenceRate() (int64, error) {
	return s.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (s *SmallintUnsigned) Instance() (TypeInstance, error) {
	return &SmallintUnsignedInstance{}, nil
}

// SmallintUnsignedInstance is the TypeInstance of SmallintUnsigned.
type SmallintUnsignedInstance struct{}

var _ TypeInstance = (*SmallintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint16()
	return Uint16Value(v), err
}

// Name implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) Name() string {
	return "SMALLINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *SmallintUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint16)
}
