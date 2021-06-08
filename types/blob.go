package types

import (
	"fmt"
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
	return VarbinaryValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *BlobInstance) TypeValue() Value {
	return VarbinaryValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *BlobInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return "BLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BlobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 65536)
}

// BlobValue is the Value type of a BlobInstance.
type BlobValue struct {
	StringValue
}

var _ Value = BlobValue{}

// Convert implements the Value interface.
func (v BlobValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case string:
		v.StringValue = StringValue(val)
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v BlobValue) Name() string {
	return "BLOB"
}

// MySQLString implements the Value interface.
func (v BlobValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v BlobValue) SQLiteString() string {
	return v.String()
}
