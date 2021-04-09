package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// BigintUnsigned represents the BIGINT UNSIGNED MySQL type.
type BigintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*BigintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *BigintUnsigned) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *BigintUnsigned) Instance() (TypeInstance, error) {
	return &BigintUnsignedInstance{}, nil
}

// BigintUnsignedInstance is the TypeInstance of BigintUnsigned.
type BigintUnsignedInstance struct{}

var _ TypeInstance = (*BigintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BigintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	return Uint64Value(v), err
}

// Name implements the TypeInstance interface.
func (i *BigintUnsignedInstance) Name() string {
	return "BIGINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BigintUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint64)
}
