package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Year represents the YEAR MySQL type.
type Year struct {
	Distribution ranges.Int
}

var _ Type = (*Year)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Year) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Year) Instance() (TypeInstance, error) {
	return &YearInstance{}, nil
}

// YearInstance is the TypeInstance of Year.
type YearInstance struct{}

var _ TypeInstance = (*YearInstance)(nil)

// Get implements the TypeInstance interface.
func (i *YearInstance) Get() (Value, error) {
	v, err := rand.Uint8()
	return Uint16Value(v%254) + 1901, err
}

// TypeValue implements the TypeInstance interface.
func (i *YearInstance) TypeValue() Value {
	return Uint16Value(0)
}

// Name implements the TypeInstance interface.
func (i *YearInstance) Name(sqlite bool) string {
	return "YEAR"
}

// MaxValueCount implements the TypeInstance interface.
func (i *YearInstance) MaxValueCount() float64 {
	return float64(math.MaxUint8)
}
