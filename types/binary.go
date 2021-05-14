package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Binary represents the BINARY MySQL type.
type Binary struct {
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Binary)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Binary) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Binary) Instance() (TypeInstance, error) {
	charLength, err := b.Length.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &BinaryInstance{int(charLength), ranges.NewInt([]int64{0, charLength})}, nil
}

// BinaryInstance is the TypeInstance of Binary.
type BinaryInstance struct {
	charLength int
	length     ranges.Int
}

var _ TypeInstance = (*BinaryInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BinaryInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return StringValue(v), err
}

// TypeValue implements the TypeInstance interface.
func (i *BinaryInstance) TypeValue() Value {
	return StringValue("")
}

// Name implements the TypeInstance interface.
func (i *BinaryInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return fmt.Sprintf("BINARY(%d)", i.charLength)
}

// MaxValueCount implements the TypeInstance interface.
func (i *BinaryInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.charLength))
}
