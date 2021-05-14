package types

import (
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Mediumint represents the MEDIUMINT MySQL type.
type Mediumint struct {
	Distribution ranges.Int
}

var _ Type = (*Mediumint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumint) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumint) Instance() (TypeInstance, error) {
	return &MediumintInstance{}, nil
}

// MediumintInstance is the TypeInstance of Mediumint.
type MediumintInstance struct{}

var _ TypeInstance = (*MediumintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumintInstance) Get() (Value, error) {
	v, err := rand.Int32()
	return Int32Value(v % 8388607), err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumintInstance) TypeValue() Value {
	return Int32Value(0)
}

// Name implements the TypeInstance interface.
func (i *MediumintInstance) Name(sqlite bool) string {
	return "MEDIUMINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumintInstance) MaxValueCount() float64 {
	return float64(16777216)
}
