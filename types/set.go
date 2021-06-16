package types

import (
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Set represents the SET MySQL type.
type Set struct {
	Distribution      ranges.Int
	ElementNameLength ranges.Int
	NumberOfElements  ranges.Int
}

var _ Type = (*Set)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (s *Set) GetOccurrenceRate() (int64, error) {
	return s.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (s *Set) Instance() (TypeInstance, error) {
	numOfElements, err := s.NumberOfElements.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	elements := make([]string, numOfElements)
	elementMap := make(map[string]uint64)
	addedElements := make(map[string]struct{})
	for i := int64(0); i < numOfElements; {
		elemLength, err := s.ElementNameLength.RandomValue()
		if err != nil {
			return nil, errors.Wrap(err)
		}
		elemName, err := rand.StringExtendedAlphanumeric(int(elemLength))
		if err != nil {
			return nil, errors.Wrap(err)
		}
		lowerElemName := strings.ToLower(elemName)
		if _, ok := addedElements[lowerElemName]; !ok {
			elements[i] = elemName
			elementMap[elemName] = 1 << uint64(i)
			addedElements[lowerElemName] = struct{}{}
			i++
		}
	}
	elementMap[""] = 0
	return &SetInstance{elements, elementMap}, nil
}

// SetInstance is the TypeInstance of Set.
type SetInstance struct {
	elements   []string
	elementMap map[string]uint64
}

var _ TypeInstance = (*SetInstance)(nil)

// Get implements the TypeInstance interface.
func (i *SetInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	if len(i.elements) >= 64 {
		return SetValue{Uint64Value(v), &i.elementMap}, nil
	}
	return SetValue{Uint64Value(v % (1 << len(i.elements))), &i.elementMap}, err
}

// TypeValue implements the TypeInstance interface.
func (i *SetInstance) TypeValue() Value {
	return SetValue{Uint64Value(0), &i.elementMap}
}

// Name implements the TypeInstance interface.
func (i *SetInstance) Name(sqlite bool) string {
	if sqlite {
		return fmt.Sprintf("VARCHAR(20)")
	}
	return fmt.Sprintf("SET('%s')", strings.Join(i.elements, "','"))
}

// MaxValueCount implements the TypeInstance interface.
func (i *SetInstance) MaxValueCount() float64 {
	return math.Pow(2, float64(len(i.elements)))
}

// SetValue is the Value type of a SetInstance.
type SetValue struct {
	Uint64Value
	elementMap *map[string]uint64 // pointer so that we can directly compare using ==
}

var _ Value = SetValue{}

// Convert implements the Value interface.
func (v SetValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case uint:
		v.Uint64Value = Uint64Value(val)
	case int:
		v.Uint64Value = Uint64Value(val)
	case uint8:
		v.Uint64Value = Uint64Value(val)
	case int8:
		v.Uint64Value = Uint64Value(val)
	case uint16:
		v.Uint64Value = Uint64Value(val)
	case int16:
		v.Uint64Value = Uint64Value(val)
	case uint32:
		v.Uint64Value = Uint64Value(val)
	case int32:
		v.Uint64Value = Uint64Value(val)
	case uint64:
		v.Uint64Value = Uint64Value(val)
	case int64:
		v.Uint64Value = Uint64Value(val)
	case string:
		// This code assumes that the string perfectly represents an uint64
		n := uint64(0)
		for i := 0; i < len(val); i++ {
			n = (n * 10) + uint64(val[i]-'0')
		}
		v.Uint64Value = Uint64Value(n)
	case []uint8:
		vals := strings.Split(*(*string)(unsafe.Pointer(&val)), ",")
		sum := uint64(0)
		for _, val := range vals {
			sum += (*v.elementMap)[val]
		}
		v.Uint64Value = Uint64Value(sum)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v SetValue) Name() string {
	return "SET"
}

// MySQLString implements the Value interface.
func (v SetValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v SetValue) SQLiteString() string {
	return formatUint64Sqlite(uint64(v.Uint64Value))
}
