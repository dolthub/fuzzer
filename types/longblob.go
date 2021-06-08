package types

import (
	"fmt"
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
	return LongblobValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *LongblobInstance) TypeValue() Value {
	return LongblobValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *LongblobInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return "LONGBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *LongblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 4294967296)
}

// LongblobValue is the Value type of a LongblobInstance.
type LongblobValue struct {
	StringValue
}

var _ Value = LongblobValue{}

// Convert implements the Value interface.
func (v LongblobValue) Convert(val interface{}) (Value, error) {
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
func (v LongblobValue) Name() string {
	return "LONGBLOB"
}

// MySQLString implements the Value interface.
func (v LongblobValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v LongblobValue) SQLiteString() string {
	return v.String()
}
