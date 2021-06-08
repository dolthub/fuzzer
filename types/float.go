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

// Float represents the FLOAT MySQL type.
type Float struct {
	Distribution ranges.Int
}

var _ Type = (*Float)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (f *Float) GetOccurrenceRate() (int64, error) {
	return f.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (f *Float) Instance() (TypeInstance, error) {
	return &FloatInstance{}, nil
}

// FloatInstance is the TypeInstance of Float.
type FloatInstance struct{}

var _ TypeInstance = (*FloatInstance)(nil)

// Get implements the TypeInstance interface.
func (i *FloatInstance) Get() (Value, error) {
	v, err := rand.Float32()
	return FloatValue{Float32Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *FloatInstance) TypeValue() Value {
	return FloatValue{Float32Value(0)}
}

// Name implements the TypeInstance interface.
func (i *FloatInstance) Name(sqlite bool) string {
	return "FLOAT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *FloatInstance) MaxValueCount() float64 {
	return float64(math.MaxUint32)
}

// FloatValue is the Value type of a FloatInstance.
type FloatValue struct {
	Float32Value
}

var _ Value = FloatValue{}

// Convert implements the Value interface.
func (v FloatValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case float32:
		v.Float32Value = Float32Value(val)
	case float64:
		v.Float32Value = Float32Value(val)
	case string:
		pVal, err := strconv.ParseFloat(val, 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Float32Value = Float32Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseFloat(*(*string)(unsafe.Pointer(&val)), 32)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Float32Value = Float32Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v FloatValue) Name() string {
	return "FLOAT"
}

// MySQLString implements the Value interface.
func (v FloatValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v FloatValue) SQLiteString() string {
	return v.String()
}
