package types

import (
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// MediumintUnsigned represents the MEDIUMINT UNSIGNED MySQL type.
type MediumintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*MediumintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *MediumintUnsigned) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *MediumintUnsigned) Instance() (TypeInstance, error) {
	return &MediumintUnsignedInstance{}, nil
}

// MediumintUnsignedInstance is the TypeInstance of MediumintUnsigned.
type MediumintUnsignedInstance struct{}

var _ TypeInstance = (*MediumintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint32()
	return Uint32Value(v % 16777215), err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) TypeValue() Value {
	return Uint32Value(0)
}

// Name implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) Name(sqlite bool) string {
	return "MEDIUMINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) MaxValueCount() float64 {
	return float64(16777216)
}
