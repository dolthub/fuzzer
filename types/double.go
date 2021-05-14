package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Double represents the DOUBLE MySQL type.
type Double struct {
	Distribution ranges.Int
}

var _ Type = (*Double)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (d *Double) GetOccurrenceRate() (int64, error) {
	return d.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (d *Double) Instance() (TypeInstance, error) {
	return &DoubleInstance{}, nil
}

// DoubleInstance is the TypeInstance of Double.
type DoubleInstance struct{}

var _ TypeInstance = (*DoubleInstance)(nil)

// Get implements the TypeInstance interface.
func (i *DoubleInstance) Get() (Value, error) {
	v, err := rand.Float64()
	return Float64Value(v), err
}

// TypeValue implements the TypeInstance interface.
func (i *DoubleInstance) TypeValue() Value {
	return Float64Value(0)
}

// Name implements the TypeInstance interface.
func (i *DoubleInstance) Name(sqlite bool) string {
	return "DOUBLE"
}

// MaxValueCount implements the TypeInstance interface.
func (i *DoubleInstance) MaxValueCount() float64 {
	return float64(math.MaxUint64)
}
