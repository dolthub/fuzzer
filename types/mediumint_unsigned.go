package types

import (
	"fmt"
	"strconv"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// MediumintUnsigned represents the MEDIUMINT UNSIGNED MySQL type.
type MediumintUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*MediumintUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (m *MediumintUnsigned) GetOccurrenceRate() (int64, error) {
	return m.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (m *MediumintUnsigned) Instance() (TypeInstance, error) {
	return &MediumintUnsignedInstance{}, nil
}

// MediumintUnsignedInstance is the TypeInstance of MediumintUnsigned.
type MediumintUnsignedInstance struct{}

var _ TypeInstance = (*MediumintUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint32()
	return MediumintUnsignedValue{Uint32Value(v % 16777215)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) TypeValue() Value {
	return MediumintUnsignedValue{Uint32Value(0)}
}

// Name implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) Name(sqlite bool) string {
	return "MEDIUMINT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *MediumintUnsignedInstance) MaxValueCount() float64 {
	return float64(16777216)
}

// MediumintUnsignedValue is the Value type of a MediumintUnsignedInstance.
type MediumintUnsignedValue struct {
	Uint32Value
}

var _ Value = MediumintUnsignedValue{}

// Convert implements the Value interface.
func (v MediumintUnsignedValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Uint32Value = Uint32Value(val)
	case int:
		v.Uint32Value = Uint32Value(val)
	case uint8:
		v.Uint32Value = Uint32Value(val)
	case int8:
		v.Uint32Value = Uint32Value(val)
	case uint16:
		v.Uint32Value = Uint32Value(val)
	case int16:
		v.Uint32Value = Uint32Value(val)
	case uint32:
		v.Uint32Value = Uint32Value(val)
	case int32:
		v.Uint32Value = Uint32Value(val)
	case uint64:
		v.Uint32Value = Uint32Value(val)
	case int64:
		v.Uint32Value = Uint32Value(val)
	case string:
		pVal, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint32Value = Uint32Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseUint(*(*string)(unsafe.Pointer(&val)), 10, 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint32Value = Uint32Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v MediumintUnsignedValue) Name() string {
	return "MEDIUMINT UNSIGNED"
}

// MySQLString implements the Value interface.
func (v MediumintUnsignedValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v MediumintUnsignedValue) SQLiteString() string {
	return v.String()
}
