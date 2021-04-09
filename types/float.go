package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Float represents the FLOAT MySQL type.
type Float struct {
	Distribution ranges.Int
}

var _ Type = (*Float)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (f *Float) GetOccurrenceRate() (int64, error) {
	return f.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (f *Float) Instance() (TypeInstance, error) {
	return &FloatInstance{}, nil
}

// FloatInstance is the TypeInstance of Float.
type FloatInstance struct{}

var _ TypeInstance = (*FloatInstance)(nil)

// Get implements the TypeInstance interface.
func (i *FloatInstance) Get() (Value, error) {
	v, err := rand.Float32()
	return Float32Value(v), err
}

// Name implements the TypeInstance interface.
func (i *FloatInstance) Name() string {
	return "FLOAT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *FloatInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}
