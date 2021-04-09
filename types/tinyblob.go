package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Tinyblob represents the TINYBLOB MySQL type.
type Tinyblob struct {
	Distribution ranges.Int
}

var _ Type = (*Tinyblob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (t *Tinyblob) GetOccurrenceRate() (int64, error) {
	return t.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (t *Tinyblob) Instance() (TypeInstance, error) {
	return &TinyblobInstance{ranges.NewInt([]int64{0, 255})}, nil
}

// TinyblobInstance is the TypeInstance of Tinyblob.
type TinyblobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*TinyblobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *TinyblobInstance) Get() (Value, error) {
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
func (i *TinyblobInstance) Name() string {
	return "TINYBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *TinyblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 256)
}
