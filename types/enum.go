package types

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Enum represents the ENUM MySQL type.
type Enum struct {
	Distribution      ranges.Int
	ElementNameLength ranges.Int
	NumberOfElements  ranges.Int
}

var _ Type = (*Enum)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (e *Enum) GetOccurrenceRate() (int64, error) {
	return e.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (e *Enum) Instance() (TypeInstance, error) {
	numOfElements, err := e.NumberOfElements.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	elements := make([]string, numOfElements)
	addedElements := make(map[string]struct{})
	for i := int64(0); i < numOfElements; {
		elemLength, err := e.ElementNameLength.RandomValue()
		if err != nil {
			return nil, errors.Wrap(err)
		}
		elemName, err := rand.String(int(elemLength))
		if err != nil {
			return nil, errors.Wrap(err)
		}
		lowerElemName := strings.ToLower(elemName)
		if _, ok := addedElements[lowerElemName]; !ok {
			elements[i] = elemName
			addedElements[lowerElemName] = struct{}{}
			i++
		}
	}
	return &EnumInstance{elements}, nil
}

// EnumInstance is the TypeInstance of Enum.
type EnumInstance struct {
	elements []string
}

var _ TypeInstance = (*EnumInstance)(nil)

// Get implements the TypeInstance interface.
func (i *EnumInstance) Get() (Value, error) {
	v, err := rand.Uint16()
	return EnumValue{Uint16Value(v%uint16(len(i.elements))) + 1}, err
}

// TypeValue implements the TypeInstance interface.
func (i *EnumInstance) TypeValue() Value {
	return EnumValue{Uint16Value(0)}
}

// Name implements the TypeInstance interface.
func (i *EnumInstance) Name(sqlite bool) string {
	if sqlite {
		return fmt.Sprintf("SMALLINT UNSIGNED")
	}
	return fmt.Sprintf("ENUM('%s')", strings.Join(i.elements, "','"))
}

// MaxValueCount implements the TypeInstance interface.
func (i *EnumInstance) MaxValueCount() float64 {
	return float64(len(i.elements))
}

// EnumValue is the Value type of a EnumInstance.
type EnumValue struct {
	Uint16Value
}

var _ Value = EnumValue{}

// Convert implements the Value interface.
func (v EnumValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Uint16Value = Uint16Value(val)
	case int:
		v.Uint16Value = Uint16Value(val)
	case uint8:
		v.Uint16Value = Uint16Value(val)
	case int8:
		v.Uint16Value = Uint16Value(val)
	case uint16:
		v.Uint16Value = Uint16Value(val)
	case int16:
		v.Uint16Value = Uint16Value(val)
	case uint32:
		v.Uint16Value = Uint16Value(val)
	case int32:
		v.Uint16Value = Uint16Value(val)
	case uint64:
		v.Uint16Value = Uint16Value(val)
	case int64:
		v.Uint16Value = Uint16Value(val)
	case string:
		pVal, err := strconv.ParseUint(val, 10, 16)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint16Value = Uint16Value(pVal)
	case []uint8:
		pVal, err := strconv.ParseUint(*(*string)(unsafe.Pointer(&val)), 10, 16)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		v.Uint16Value = Uint16Value(pVal)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v EnumValue) Name() string {
	return "ENUM"
}

// MySQLString implements the Value interface.
func (v EnumValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v EnumValue) SQLiteString() string {
	return v.String()
}
