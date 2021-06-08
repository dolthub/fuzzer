package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Mediumblob represents the MEDIUMBLOB MySQL type.
type Mediumblob struct {
	Distribution ranges.Int
}

var _ Type = (*Mediumblob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumblob) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumblob) Instance() (TypeInstance, error) {
	return &MediumblobInstance{ranges.NewInt([]int64{0, 16777215})}, nil
}

// MediumblobInstance is the TypeInstance of Mediumblob.
type MediumblobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*MediumblobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumblobInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return MediumblobValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumblobInstance) TypeValue() Value {
	return MediumblobValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *MediumblobInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return "MEDIUMBLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumblobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 16777216)
}

// MediumblobValue is the Value type of a MediumblobInstance.
type MediumblobValue struct {
	StringValue
}

var _ Value = MediumblobValue{}

// Convert implements the Value interface.
func (v MediumblobValue) Convert(val interface{}) (Value, error) {
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
func (v MediumblobValue) Name() string {
	return "MEDIUMBLOB"
}

// MySQLString implements the Value interface.
func (v MediumblobValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v MediumblobValue) SQLiteString() string {
	return v.String()
}
