package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Text represents the TEXT MySQL type.
type Text struct {
	Distribution ranges.Int
}

var _ Type = (*Text)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Text) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Text) Instance() (TypeInstance, error) {
	return &TextInstance{ranges.NewInt([]int64{0, 16383})}, nil
}

// TextInstance is the TypeInstance of Text.
type TextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*TextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TextInstance) Get() (Value, error) {
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

// Name implements the TypeInstance interface.
func (i *TextInstance) Name() string {
	return "TEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 16384)
}
