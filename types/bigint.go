package types

import (
	"math"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Bigint represents the BIGINT MySQL type.
type Bigint struct {
	Distribution ranges.Int
}

var _ Type = (*Bigint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Bigint) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Bigint) Instance() (TypeInstance, error) {
	return &BigintInstance{}, nil
}

// BigintInstance is the TypeInstance of Bigint.
type BigintInstance struct{}

var _ TypeInstance = (*BigintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BigintInstance) Get() (Value, error) {
	v, err := rand.Int64()
	return Int64Value(v), err
}

// TypeValue implements the TypeInstance interface.
func (i *BigintInstance) TypeValue() Value {
	return Int64Value(0)
}

// Name implements the TypeInstance interface.
func (i *BigintInstance) Name(sqlite bool) string {
	return "BIGINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BigintInstance) MaxValueCount() float64 {
	return float64(math.MaxUint64)
}
