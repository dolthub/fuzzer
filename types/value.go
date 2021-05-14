package types

import (
	"database/sql/driver"
	"strconv"
	"unsafe"
)

// There is no BoolValue, as MySQL uses integers to stand in for the place of booleans.

// Value is a value that is returned from a TypeInstance.
type Value interface {
	// Compare returns an integer comparing the current value to the given value. Returns -2 if the Values are a mismatch.
	Compare(other Value) int
	// Name returns the underlying name of the value, e.g. int64.
	Name() string
	// String returns the underlying value as a string for insertion into a SQL file, e.g. string would include the quotes.
	String() string
	// SQLiteString returns the underlying value as a string for insertion into SQLite specifically.
	SQLiteString() string
}

// NilValue is the Value type of a nil.
type NilValue struct{}

var _ Value = NilValue{}

// Name implements the interface Value.
func (v NilValue) Name() string {
	return "nil"
}

// String implements the interface Value.
func (v NilValue) String() string {
	return "NULL"
}

// SQLiteString implements the interface Value.
func (v NilValue) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v NilValue) Compare(other Value) int {
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

// Int8Value is the Value type of a int8.
type Int8Value int8

var _ Value = Int8Value(0)

// Name implements the interface Value.
func (v Int8Value) Name() string {
	return "int8"
}

// String implements the interface Value.
func (v Int8Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Int8Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Int8Value) Compare(other Value) int {
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

// Int16Value is the Value type of a int16.
type Int16Value int16

var _ Value = Int16Value(0)

// Name implements the interface Value.
func (v Int16Value) Name() string {
	return "int16"
}

// String implements the interface Value.
func (v Int16Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Int16Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Int16Value) Compare(other Value) int {
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

// Int32Value is the Value type of a int32.
type Int32Value int32

var _ Value = Int32Value(0)

// Name implements the interface Value.
func (v Int32Value) Name() string {
	return "int32"
}

// String implements the interface Value.
func (v Int32Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Int32Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Int32Value) Compare(other Value) int {
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

// Int64Value is the Value type of a int64.
type Int64Value int64

var _ Value = Int64Value(0)

// Name implements the interface Value.
func (v Int64Value) Name() string {
	return "int64"
}

// String implements the interface Value.
func (v Int64Value) String() string {
	return strconv.FormatInt(int64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Int64Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Int64Value) Compare(other Value) int {
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

// Uint8Value is the Value type of a uint8.
type Uint8Value uint8

var _ Value = Uint8Value(0)

// Name implements the interface Value.
func (v Uint8Value) Name() string {
	return "uint8"
}

// String implements the interface Value.
func (v Uint8Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Uint8Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Uint8Value) Compare(other Value) int {
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

// Uint16Value is the Value type of a uint16.
type Uint16Value uint16

var _ Value = Uint16Value(0)

// Name implements the interface Value.
func (v Uint16Value) Name() string {
	return "uint16"
}

// String implements the interface Value.
func (v Uint16Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Uint16Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Uint16Value) Compare(other Value) int {
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

// Uint32Value is the Value type of a uint32.
type Uint32Value uint32

var _ Value = Uint32Value(0)

// Name implements the interface Value.
func (v Uint32Value) Name() string {
	return "uint32"
}

// String implements the interface Value.
func (v Uint32Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Uint32Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Uint32Value) Compare(other Value) int {
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

// Uint64Value is the Value type of a uint64.
type Uint64Value uint64

var _ Value = Uint64Value(0)

// Name implements the interface Value.
func (v Uint64Value) Name() string {
	return "uint64"
}

// String implements the interface Value.
func (v Uint64Value) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// SQLiteString implements the interface Value.
func (v Uint64Value) SQLiteString() string {
	// SQLite doesn't support uint64, so we get around this by returning a string of fixed length
	uintStr := strconv.FormatUint(uint64(v), 10)
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

// Compare implements the interface Value.
func (v Uint64Value) Compare(other Value) int {
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

// Float32Value is the Value type of a float32.
type Float32Value float32

var _ Value = Float32Value(0)

// Name implements the interface Value.
func (v Float32Value) Name() string {
	return "float32"
}

// String implements the interface Value.
func (v Float32Value) String() string {
	return StringValue(strconv.FormatFloat(float64(v), 'g', -1, 32)).String()
}

// SQLiteString implements the interface Value.
func (v Float32Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Float32Value) Compare(other Value) int {
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

// Float64Value is the Value type of a float64.
type Float64Value float64

var _ Value = Float64Value(0)

// Name implements the interface Value.
func (v Float64Value) Name() string {
	return "float64"
}

// String implements the interface Value.
func (v Float64Value) String() string {
	return StringValue(strconv.FormatFloat(float64(v), 'g', -1, 64)).String()
}

// SQLiteString implements the interface Value.
func (v Float64Value) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v Float64Value) Compare(other Value) int {
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

// StringValue is the Value type of a string.
type StringValue string

var _ Value = StringValue("")

// Name implements the interface Value.
func (v StringValue) Name() string {
	return "string"
}

// String implements the interface Value.
func (v StringValue) String() string {
	out := make([]byte, len(v)+2)
	copy(out[1:], v)
	out[0] = 39
	out[len(out)-1] = 39
	return *(*string)(unsafe.Pointer(&out))
}

// SQLiteString implements the interface Value.
func (v StringValue) SQLiteString() string {
	return v.String()
}

// Compare implements the interface Value.
func (v StringValue) Compare(other Value) int {
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
