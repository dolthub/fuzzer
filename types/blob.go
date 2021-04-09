package types

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Blob represents the BLOB MySQL type.
type Blob struct {
	Distribution ranges.Int
}

var _ Type = (*Blob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Blob) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Blob) Instance() (TypeInstance, error) {
	return &BlobInstance{ranges.NewInt([]int64{0, 65535})}, nil
}

// BlobInstance is the TypeInstance of Blob.
type BlobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*BlobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BlobInstance) Get() (Value, error) {
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
func (i *BlobInstance) Name() string {
	return "BLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BlobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 65536)
}
