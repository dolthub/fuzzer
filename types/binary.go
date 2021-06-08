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
	return &BinaryInstance{int(charLength)}, nil
}

// BinaryInstance is the TypeInstance of Binary.
type BinaryInstance struct {
	charLength int
}

var _ TypeInstance = (*BinaryInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BinaryInstance) Get() (Value, error) {
	v, err := rand.String(i.charLength)
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return BinaryValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *BinaryInstance) TypeValue() Value {
	return BinaryValue{StringValue("")}
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

// BinaryValue is the Value type of a BinaryInstance.
type BinaryValue struct {
	StringValue
}

var _ Value = BinaryValue{}

// Convert implements the Value interface.
func (v BinaryValue) Convert(val interface{}) (Value, error) {
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
func (v BinaryValue) Name() string {
	return "BINARY"
}

// MySQLString implements the Value interface.
func (v BinaryValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v BinaryValue) SQLiteString() string {
	return v.String()
}
