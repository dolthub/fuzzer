package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Mediumtext represents the MEDIUMTEXT MySQL type.
type Mediumtext struct {
	Distribution ranges.Int
}

var _ Type = (*Mediumtext)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *Mediumtext) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *Mediumtext) Instance() (TypeInstance, error) {
	return &MediumtextInstance{ranges.NewInt([]int64{0, 4194303})}, nil
}

// MediumtextInstance is the TypeInstance of Mediumtext.
type MediumtextInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*MediumtextInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumtextInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return MediumtextValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumtextInstance) TypeValue() Value {
	return MediumtextValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *MediumtextInstance) Name(sqlite bool) string {
	return "MEDIUMTEXT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumtextInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), 4194304)
}

// MediumtextValue is the Value type of a MediumtextInstance.
type MediumtextValue struct {
	StringValue
}

var _ Value = MediumtextValue{}

// Convert implements the Value interface.
func (v MediumtextValue) Convert(val interface{}) (Value, error) {
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
func (v MediumtextValue) Name() string {
	return "MEDIUMTEXT"
}

// MySQLString implements the Value interface.
func (v MediumtextValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v MediumtextValue) SQLiteString() string {
	return v.String()
}
