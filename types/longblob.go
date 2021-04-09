package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Longblob represents the LONGBLOB MySQL type.
type Longblob struct {
	Distribution ranges.Int
}

var _ Type = (*Longblob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (l *Longblob) GetOccurrenceRate() (int64, error) {
	return l.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (l *Longblob) Instance() (TypeInstance, error) {
	return &LongblobInstance{ranges.NewInt([]int64{0, 4294967295})}, nil
}

// LongblobInstance is the TypeInstance of Longblob.
type LongblobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*LongblobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *LongblobInstance) Get() (Value, error) {
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
func (i *LongblobInstance) Name() string {
	return "LONGBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *LongblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 4294967296)
}
