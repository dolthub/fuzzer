package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// TinyintUnsigned represents the TINYINT UNSIGNED MySQL type.
type TinyintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*TinyintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *TinyintUnsigned) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *TinyintUnsigned) Instance() (TypeInstance, error) {
	return &TinyintUnsignedInstance{}, nil
}

// TinyintUnsignedInstance is the TypeInstance of TinyintUnsigned.
type TinyintUnsignedInstance struct{}

var _ TypeInstance = (*TinyintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint8()
	return Uint8Value(v), err
}

// Name implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) Name() string {
	return "TINYINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinyintUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint8)
}
