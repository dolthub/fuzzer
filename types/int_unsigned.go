package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// IntUnsigned represents the INT UNSIGNED MySQL type.
type IntUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*IntUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (i *IntUnsigned) GetOccurrenceRate() (int64, error) {
	return i.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (i *IntUnsigned) Instance() (TypeInstance, error) {
	return &IntUnsignedInstance{}, nil
}

// IntUnsignedInstance is the TypeInstance of IntUnsigned.
type IntUnsignedInstance struct{}

var _ TypeInstance = (*IntUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *IntUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint32()
	return Uint32Value(v), err
}

// TypeValue implements the TypeInstance interface.
func (i *IntUnsignedInstance) TypeValue() Value {
	return Uint32Value(0)
}

// Name implements the TypeInstance interface.
func (i *IntUnsignedInstance) Name(sqlite bool) string {
	return "INT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *IntUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}
