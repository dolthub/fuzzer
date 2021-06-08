package types

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"unsafe"

	"github.com/dolthub/fuzzer/errors"
)

// There is no BoolValue, as MySQL uses integers to stand in for the place of booleans.

// ValuePrimitive is the baseline for a value returned from a TypeInstance. Each Value will be an alias over a ValuePrimitive.
type ValuePrimitive interface {
	// Compare returns an integer comparing the current value to the given value. Returns -2 if the Values are a mismatch.
	Compare(other ValuePrimitive) int
	// String returns the underlying value as a string for insertion into a generic SQL file, e.g. string would include
	// the quotes.
	String() string
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
}

// NilValue is the Value type of a nil. This is a full Value rather than a ValuePrimitive as it should not be built on
// top of.
type NilValue struct{}

var _ Value = NilValue{}

// Set implements the interface Value.
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

// Compare implements the interface ValuePrimitive.
func (v NilValue) Compare(other ValuePrimitive) int {
	_, ok := other.(NilValue)
	if ok {
		return 0
	}
	return -1
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

// Compare implements the interface ValuePrimitive.
func (v Int8Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Compare implements the interface ValuePrimitive.
func (v Int16Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Compare implements the interface ValuePrimitive.
func (v Int32Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Compare implements the interface ValuePrimitive.
func (v Int64Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Compare implements the interface ValuePrimitive.
func (v Uint8Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Compare implements the interface ValuePrimitive.
func (v Uint16Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Compare implements the interface ValuePrimitive.
func (v Uint32Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Value implements the interface driver.Value.
func (v Uint32Value) Value() (driver.Value, error) {
	return uint64(v), nil
}

// Uint64Value is the ValuePrimitive type of a uint64.
type Uint64Value uint64

var _ ValuePrimitive = Uint64Value(0)

// String implements the interface ValuePrimitive.
func (v Uint64Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// Compare implements the interface ValuePrimitive.
func (v Uint64Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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
	//TODO: is the string wrap necessary?
	return StringValue(strconv.FormatFloat(float64(v), 'g', -1, 32)).String()
}

// Compare implements the interface ValuePrimitive.
func (v Float32Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Value implements the interface driver.Value.
func (v Float32Value) Value() (driver.Value, error) {
	return float64(v), nil
}

// Float64Value is the ValuePrimitive type of a float64.
type Float64Value float64

var _ ValuePrimitive = Float64Value(0)

// String implements the interface ValuePrimitive.
func (v Float64Value) String() string {
	//TODO: is the string wrap necessary?
	return StringValue(strconv.FormatFloat(float64(v), 'g', -1, 64)).String()
}

// Compare implements the interface ValuePrimitive.
func (v Float64Value) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Value implements the interface driver.Value.
func (v Float64Value) Value() (driver.Value, error) {
	return float64(v), nil
}

// StringValue is the ValuePrimitive type of a string.
type StringValue string

var _ ValuePrimitive = StringValue("")

// String implements the interface ValuePrimitive.
func (v StringValue) String() string {
	out := make([]byte, len(v)+2)
	copy(out[1:], v)
	out[0] = 39
	out[len(out)-1] = 39
	return *(*string)(unsafe.Pointer(&out))
}

// Compare implements the interface ValuePrimitive.
func (v StringValue) Compare(other ValuePrimitive) int {
	switch other := other.(type) {
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

// Value implements the interface driver.Value.
func (v StringValue) Value() (driver.Value, error) {
	return string(v), nil
}
