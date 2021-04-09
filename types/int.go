package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Int represents the INT MySQL type.
type Int struct {
	Distribution ranges.Int
}

var _ Type = (*Int)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (i *Int) GetOccurrenceRate() (int64, error) {
	return i.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (i *Int) Instance() (TypeInstance, error) {
	return &IntInstance{}, nil
}

// IntInstance is the TypeInstance of Int.
type IntInstance struct{}

var _ TypeInstance = (*IntInstance)(nil)

// Get implements the TypeInstance interface.
func (i *IntInstance) Get() (Value, error) {
	v, err := rand.Int32()
	return Int32Value(v), err
}

// Name implements the TypeInstance interface.
func (i *IntInstance) Name() string {
	return "INT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *IntInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}
