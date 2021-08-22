// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Enum represents the ENUM MySQL type.
type Enum struct {
	Collations        []string
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
	colPos, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos %= uint64(len(e.Collations))
	collation, err := sql.ParseCollation(nil, &e.Collations[colPos], false)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	numOfElements, err := e.NumberOfElements.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	elements := make([]string, numOfElements)
	elementMap := make(map[string]uint16)
	addedElements := make(map[string]struct{})
	for i := int64(0); i < numOfElements; {
		elemLength, err := e.ElementNameLength.RandomValue()
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
			elementMap[elemName] = uint16(i + 1)
			addedElements[lowerElemName] = struct{}{}
			i++
		}
	}
	elementMap[""] = 0
	return &EnumInstance{elements, elementMap, collation}, nil
}

// EnumInstance is the TypeInstance of Enum.
type EnumInstance struct {
	elements   []string
	elementMap map[string]uint16
	collation  sql.Collation
}

var _ TypeInstance = (*EnumInstance)(nil)

// Get implements the TypeInstance interface.
func (i *EnumInstance) Get() (Value, error) {
	v, err := rand.Uint16()
	return EnumValue{Uint16Value(v%uint16(len(i.elements))) + 1, &i.elementMap}, err
}

// TypeValue implements the TypeInstance interface.
func (i *EnumInstance) TypeValue() Value {
	return EnumValue{Uint16Value(0), &i.elementMap}
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
	elementMap *map[string]uint16 // pointer so that we can directly compare using ==
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
		v.Uint16Value = Uint16Value((*v.elementMap)[*(*string)(unsafe.Pointer(&val))])
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

// CSVString implements the interface Value.
func (v EnumValue) CSVString() string {
	return v.String()
}
