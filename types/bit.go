package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Bit represents the BIT MySQL type.
type Bit struct {
	Distribution ranges.Int
	Width        ranges.Int
}

var _ Type = (*Bit)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Bit) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Bit) Instance() (TypeInstance, error) {
	width, err := b.Width.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &BitInstance{uint64(width)}, nil
}

// BitInstance is the TypeInstance of Bit.
type BitInstance struct {
	width uint64
}

var _ TypeInstance = (*BitInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BitInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	if i.width >= 64 {
		return Uint64Value(v), nil
	}
	return Uint64Value(v % (1 << i.width)), err
}

// TypeValue implements the TypeInstance interface.
func (i *BitInstance) TypeValue() Value {
	return Uint64Value(0)
}

// Name implements the TypeInstance interface.
func (i *BitInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(20)"
	}
	return fmt.Sprintf("BIT(%d)", i.width)
}

// MaxValueCount implements the TypeInstance interface.
func (i *BitInstance) MaxValueCount() float64 {
	return math.Pow(2, float64(i.width))
}
