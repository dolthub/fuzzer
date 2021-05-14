package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Longtext represents the LONGTEXT MySQL type.
type Longtext struct {
	Distribution ranges.Int
}

var _ Type = (*Longtext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (l *Longtext) GetOccurrenceRate() (int64, error) {
	return l.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (l *Longtext) Instance() (TypeInstance, error) {
	return &LongtextInstance{ranges.NewInt([]int64{1, 4294967295})}, nil
}

// LongtextInstance is the TypeInstance of Longtext.
type LongtextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*LongtextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *LongtextInstance) Get() (Value, error) {
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
func (i *LongtextInstance) TypeValue() Value {
	return StringValue("")
}

// Name implements the TypeInstance interface.
func (i *LongtextInstance) Name(sqlite bool) string {
	return "LONGTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *LongtextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 4294967296)
}
