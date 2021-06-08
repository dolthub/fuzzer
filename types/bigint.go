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

// Bigint represents the BIGINT MySQL type.
type Bigint struct {
	Distribution ranges.Int
}

var _ Type = (*Bigint)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Bigint) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Bigint) Instance() (TypeInstance, error) {
	return &BigintInstance{}, nil
}

// BigintInstance is the TypeInstance of Bigint.
type BigintInstance struct{}

var _ TypeInstance = (*BigintInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BigintInstance) Get() (Value, error) {
	v, err := rand.Int64()
	return BigintValue{Int64Value(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *BigintInstance) TypeValue() Value {
	return BigintValue{Int64Value(0)}
}

// Name implements the TypeInstance interface.
func (i *BigintInstance) Name(sqlite bool) string {
	return "BIGINT"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BigintInstance) MaxValueCount() float64 {
	return float64(math.MaxUint64)
}

// BigintValue is the Value type of a BigintInstance.
type BigintValue struct {
	Int64Value
}

var _ Value = BigintValue{}

// Convert implements the Value interface.
func (v BigintValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Int64Value = Int64Value(val)
	case int:
		v.Int64Value = Int64Value(val)
	case uint8:
		v.Int64Value = Int64Value(val)
	case int8:
		v.Int64Value = Int64Value(val)
	case uint16:
		v.Int64Value = Int64Value(val)
	case int16:
		v.Int64Value = Int64Value(val)
	case uint32:
		v.Int64Value = Int64Value(val)
	case int32:
		v.Int64Value = Int64Value(val)
	case uint64:
		v.Int64Value = Int64Value(val)
	case int64:
		v.Int64Value = Int64Value(val)
	case string:
		pVal, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int64Value = Int64Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseInt(*(*string)(unsafe.Pointer(&val)), 10, 64)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Int64Value = Int64Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v BigintValue) Name() string {
	return "BIGINT"
}

// MySQLString implements the Value interface.
func (v BigintValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v BigintValue) SQLiteString() string {
	return v.String()
}
