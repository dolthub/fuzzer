package types

import (
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"

	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// IntUnsigned represents the INT UNSIGNED MySQL type.
type IntUnsigned struct {
	Distribution ranges.Int
}

var _ Type = (*IntUnsigned)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (i *IntUnsigned) GetOccurrenceRate() (int64, error) {
	return i.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (i *IntUnsigned) Instance() (TypeInstance, error) {
	return &IntUnsignedInstance{}, nil
}

// IntUnsignedInstance is the TypeInstance of IntUnsigned.
type IntUnsignedInstance struct{}

var _ TypeInstance = (*IntUnsignedInstance)(nil)

// Get implements the TypeInstance interface.
func (i *IntUnsignedInstance) Get() (Value, error) {
	v, err := rand.Uint32()
	return IntUnsignedValue{Uint32Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *IntUnsignedInstance) TypeValue() Value {
	return IntUnsignedValue{Uint32Value(0)}
}

// Name implements the TypeInstance interface.
func (i *IntUnsignedInstance) Name(sqlite bool) string {
	return "INT UNSIGNED"
}

// MaxValueCount implements the TypeInstance interface.
func (i *IntUnsignedInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}

// IntUnsignedValue is the Value type of a IntUnsignedInstance.
type IntUnsignedValue struct {
	Uint32Value
}

var _ Value = IntUnsignedValue{}

// Convert implements the Value interface.
func (v IntUnsignedValue) Convert(val interface{}) (Value, error) {
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
func (v IntUnsignedValue) Name() string {
	return "INT UNSIGNED"
}

// MySQLString implements the Value interface.
func (v IntUnsignedValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v IntUnsignedValue) SQLiteString() string {
	return v.String()
}
