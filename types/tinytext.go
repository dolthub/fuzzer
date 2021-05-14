package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Tinytext represents the TINYTEXT MySQL type.
type Tinytext struct {
	Distribution ranges.Int
}

var _ Type = (*Tinytext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Tinytext) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Tinytext) Instance() (TypeInstance, error) {
	return &TinytextInstance{ranges.NewInt([]int64{0, 63})}, nil
}

// TinytextInstance is the TypeInstance of Tinytext.
type TinytextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*TinytextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinytextInstance) Get() (Value, error) {
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
func (i *TinytextInstance) TypeValue() Value {
	return StringValue("")
}

// Name implements the TypeInstance interface.
func (i *TinytextInstance) Name(sqlite bool) string {
	return "TINYTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinytextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 64)
}
