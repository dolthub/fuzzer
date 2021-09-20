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
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"
)

// There is no BoolValue, as MySQL uses integers to stand in for the place of booleans.

// ValuePrimitive is the baseline for a value returned from a TypeInstance. Each Value will be an alias over a ValuePrimitive.
type ValuePrimitive interface {
	// Compare returns an integer comparing the current value to the given value. Returns -2 if the Values are a mismatch.
	Compare(other ValuePrimitive) int
	// Primitive returns the ValuePrimitive. When called on a ValuePrimitive, it returns itself. When called on a Value,
	// it returns the underlying ValuePrimitive, rather than using the Value as a ValuePrimitive. This is useful for
	// the comparison functions on each ValuePrimitive to automatically get the inner ValuePrimitive.
	Primitive() ValuePrimitive
	// String returns the underlying value as a string for insertion into a generic SQL file, e.g. string would include
	// the quotes.
	String() string
	// ToBytes returns the ValuePrimitive as a byte slice.
	ToBytes() []byte
}

// Value is a value that is returned from a TypeInstance. Each Value is specific to its returned type.
type Value interface {
	ValuePrimitive
	// Convert converts the given value to the calling Value's type.
	Convert(val interface{}) (Value, error)
	// Name returns the underlying name of the Value, e.g. BIGINT.
	Name() string
	// MySQLString returns the Value as a string for insertion into a MySQL file, e.g. CHAR(5) would include the quotes.
	MySQLString() string
	// SQLiteString returns the Value as a string for insertion into SQLite specifically.
	SQLiteString() string
	// CSVString returns the Value as a string for insertion into a CSV file.
	CSVString() string
}

// NilValue is the Value type of a nil. This is a full Value rather than a ValuePrimitive as it should not be built on
// top of.
type NilValue struct{}

var _ Value = NilValue{}

// Convert implements the interface Value.
func (v NilValue) Convert(val interface{}) (Value, error) {
	if val != nil {
		return nil, errors.New(fmt.Sprintf("cannot convert %T to NULL", val))
	}
	return NilValue{}, nil
}

// Name implements the interface Value.
func (v NilValue) Name() string {
	return "NULL"
}

// Primitive implements the interface Value.
func (v NilValue) Primitive() ValuePrimitive {
	return v
}

// MySQLString implements the interface Value.
func (v NilValue) String() string {
	return "NULL"
}

// MySQLString implements the interface Value.
func (v NilValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the interface Value.
func (v NilValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v NilValue) CSVString() string {
	return ""
}

// Compare implements the interface ValuePrimitive.
func (v NilValue) Compare(other ValuePrimitive) int {
	_, ok := other.Primitive().(NilValue)
	if ok {
		return 0
	}
	return -1
}

// ToBytes implements the interface ValuePrimitive.
func (v NilValue) ToBytes() []byte {
	return []byte{0}
}

// Value implements the interface driver.Value.
func (v NilValue) Value() (driver.Value, error) {
	return nil, nil
}

// Int8Value is the ValuePrimitive type of a int8.
type Int8Value int8

var _ ValuePrimitive = Int8Value(0)

// String implements the interface ValuePrimitive.
func (v Int8Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Int8Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Int8Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Int8Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Int8Value) ToBytes() []byte {
	return []byte{byte(v)}
}

// Value implements the interface driver.Value.
func (v Int8Value) Value() (driver.Value, error) {
	return int64(v), nil
}

// Int16Value is the ValuePrimitive type of a int16.
type Int16Value int16

var _ ValuePrimitive = Int16Value(0)

// String implements the interface ValuePrimitive.
func (v Int16Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Int16Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Int16Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Int16Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Int16Value) ToBytes() []byte {
	u := uint16(v)
	return []byte{byte(u), byte(u >> 8)}
}

// Value implements the interface driver.Value.
func (v Int16Value) Value() (driver.Value, error) {
	return int64(v), nil
}

// Int32Value is the ValuePrimitive type of a int32.
type Int32Value int32

var _ ValuePrimitive = Int32Value(0)

// String implements the interface ValuePrimitive.
func (v Int32Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Int32Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Int32Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Int32Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Int32Value) ToBytes() []byte {
	u := uint32(v)
	return []byte{byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24)}
}

// Value implements the interface driver.Value.
func (v Int32Value) Value() (driver.Value, error) {
	return int64(v), nil
}

// Int64Value is the ValuePrimitive type of a int64.
type Int64Value int64

var _ ValuePrimitive = Int64Value(0)

// String implements the interface ValuePrimitive.
func (v Int64Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Int64Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Int64Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Int64Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Int64Value) ToBytes() []byte {
	u := uint64(v)
	return []byte{byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24), byte(u >> 32), byte(u >> 40), byte(u >> 48), byte(u >> 56)}
}

// Value implements the interface driver.Value.
func (v Int64Value) Value() (driver.Value, error) {
	return int64(v), nil
}

// Uint8Value is the ValuePrimitive type of a uint8.
type Uint8Value uint8

var _ ValuePrimitive = Uint8Value(0)

// String implements the interface ValuePrimitive.
func (v Uint8Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Uint8Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Uint8Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Uint8Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Uint8Value) ToBytes() []byte {
	return []byte{byte(v)}
}

// Value implements the interface driver.Value.
func (v Uint8Value) Value() (driver.Value, error) {
	return uint64(v), nil
}

// Uint16Value is the ValuePrimitive type of a uint16.
type Uint16Value uint16

var _ ValuePrimitive = Uint16Value(0)

// String implements the interface ValuePrimitive.
func (v Uint16Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Uint16Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Uint16Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Uint16Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Uint16Value) ToBytes() []byte {
	return []byte{byte(v), byte(v >> 8)}
}

// Value implements the interface driver.Value.
func (v Uint16Value) Value() (driver.Value, error) {
	return uint64(v), nil
}

// Uint32Value is the ValuePrimitive type of a uint32.
type Uint32Value uint32

var _ ValuePrimitive = Uint32Value(0)

// String implements the interface ValuePrimitive.
func (v Uint32Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Uint32Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Uint32Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Uint32Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Uint32Value) ToBytes() []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
}

// Value implements the interface driver.Value.
func (v Uint32Value) Value() (driver.Value, error) {
	return uint64(v), nil
}

// formatUint32Sqlite formats a uint32 for SQLite. SQLite natively supports uint32, however this returns fixed-length
// strings.
func formatUint32Sqlite(v uint32) string {
	uintStr := strconv.FormatUint(uint64(v), 10)
	neededZeros := 10 - len(uintStr)
	out := make([]byte, 12)
	copy(out[1+neededZeros:], uintStr)
	out[0] = 39
	for i := 1; i <= neededZeros; i++ {
		out[i] = 48
	}
	out[len(out)-1] = 39
	return *(*string)(unsafe.Pointer(&out))
}

// Uint64Value is the ValuePrimitive type of a uint64.
type Uint64Value uint64

var _ ValuePrimitive = Uint64Value(0)

// String implements the interface ValuePrimitive.
func (v Uint64Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// Primitive implements the interface ValuePrimitive.
func (v Uint64Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Uint64Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Uint64Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Uint64Value) ToBytes() []byte {
	return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24), byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56)}
}

// Value implements the interface driver.Value.
func (v Uint64Value) Value() (driver.Value, error) {
	return uint64(v), nil
}

// formatUint64Sqlite formats a uint64 for SQLite. SQLite doesn't support uint64, so we get around this by returning a
// string of fixed length.
func formatUint64Sqlite(v uint64) string {
	uintStr := strconv.FormatUint(v, 10)
	neededZeros := 20 - len(uintStr)
	out := make([]byte, 22)
	copy(out[1+neededZeros:], uintStr)
	out[0] = 39
	for i := 1; i <= neededZeros; i++ {
		out[i] = 48
	}
	out[len(out)-1] = 39
	return *(*string)(unsafe.Pointer(&out))
}

// Float32Value is the ValuePrimitive type of a float32.
type Float32Value float32

var _ ValuePrimitive = Float32Value(0)

// String implements the interface ValuePrimitive.
func (v Float32Value) String() string {
	return StringValue(strconv.FormatFloat(float64(v), 'g', -1, 64)).String()
}

// Primitive implements the interface ValuePrimitive.
func (v Float32Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Float32Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Float32Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Float32Value) ToBytes() []byte {
	u := *(*uint32)(unsafe.Pointer(&v))
	return []byte{byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24)}
}

// Value implements the interface driver.Value.
func (v Float32Value) Value() (driver.Value, error) {
	return float64(v), nil
}

// Float64Value is the ValuePrimitive type of a float64.
type Float64Value float64

var _ ValuePrimitive = Float64Value(0)

// String implements the interface ValuePrimitive.
func (v Float64Value) String() string {
	return StringValue(strconv.FormatFloat(float64(v), 'g', -1, 64)).String()
}

// Primitive implements the interface ValuePrimitive.
func (v Float64Value) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v Float64Value) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case Float64Value:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v Float64Value) ToBytes() []byte {
	u := *(*uint64)(unsafe.Pointer(&v))
	return []byte{byte(u), byte(u >> 8), byte(u >> 16), byte(u >> 24), byte(u >> 32), byte(u >> 40), byte(u >> 48), byte(u >> 56)}
}

// Value implements the interface driver.Value.
func (v Float64Value) Value() (driver.Value, error) {
	return float64(v), nil
}

// StringValue is the ValuePrimitive type of a string.
type StringValue string

var _ ValuePrimitive = StringValue("")

// String implements the interface ValuePrimitive.
func (v StringValue) String() string {
	return v.StringTerminating(39)
}

// StringTerminating returns the string with the given character as the end terminals.
func (v StringValue) StringTerminating(char byte) string {
	out := make([]byte, len(v)+2)
	copy(out[1:], v)
	out[0] = char
	out[len(out)-1] = char
	return *(*string)(unsafe.Pointer(&out))
}

// Primitive implements the interface ValuePrimitive.
func (v StringValue) Primitive() ValuePrimitive {
	return v
}

// Compare implements the interface ValuePrimitive.
func (v StringValue) Compare(other ValuePrimitive) int {
	switch other := other.Primitive().(type) {
	case NilValue:
		return 1
	case StringValue:
		if v < other {
			return -1
		} else if v > other {
			return 1
		}
		return 0
	default:
		return -2
	}
}

// ToBytes implements the interface ValuePrimitive.
func (v StringValue) ToBytes() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer((*reflect.StringHeader)(unsafe.Pointer(&v)).Data)), len(v))
}

// Value implements the interface driver.Value.
func (v StringValue) Value() (driver.Value, error) {
	return string(v), nil
}
