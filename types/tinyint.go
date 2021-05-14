package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Tinyint represents the TINYINT MySQL type.
type Tinyint struct {
	Distribution ranges.Int
}

var _ Type = (*Tinyint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Tinyint) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Tinyint) Instance() (TypeInstance, error) {
	return &TinyintInstance{}, nil
}

// TinyintInstance is the TypeInstance of Tinyint.
type TinyintInstance struct{}

var _ TypeInstance = (*TinyintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinyintInstance) Get() (Value, error) {
	v, err := rand.Int8()
	return Int8Value(v), err
}

// TypeValue implements the TypeInstance interface.
func (i *TinyintInstance) TypeValue() Value {
	return Int8Value(0)
}

// Name implements the TypeInstance interface.
func (i *TinyintInstance) Name(sqlite bool) string {
	return "TINYINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinyintInstance) MaxValueCount() float64 {
	return float64(math.MaxUint8)
}
