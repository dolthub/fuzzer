package types

import (
	"database/sql"
	"fmt"

	"github.com/dolthub/fuzzer/errors"
)

// ValueScanner handles the conversion of a value to its destination type.
type ValueScanner struct {
	Destination *Value
}

var _ sql.Scanner = ValueScanner{}

// NewValueScanner returns a new ValueScanner with the given Value pointer as the destination.
func NewValueScanner(destination *Value) ValueScanner {
	return ValueScanner{destination}
}

// Scan implements the interface sql.Scanner.
func (vs ValueScanner) Scan(val interface{}) error {
	switch dest := (*vs.Destination).(type) {
	case NilValue:
		if val != nil {
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Int8Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Int8Value(val)
		case int:
			*vs.Destination = Int8Value(val)
		case uint8:
			*vs.Destination = Int8Value(val)
		case int8:
			*vs.Destination = Int8Value(val)
		case uint16:
			*vs.Destination = Int8Value(val)
		case int16:
			*vs.Destination = Int8Value(val)
		case uint32:
			*vs.Destination = Int8Value(val)
		case int32:
			*vs.Destination = Int8Value(val)
		case uint64:
			*vs.Destination = Int8Value(val)
		case int64:
			*vs.Destination = Int8Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Int16Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Int16Value(val)
		case int:
			*vs.Destination = Int16Value(val)
		case uint8:
			*vs.Destination = Int16Value(val)
		case int8:
			*vs.Destination = Int16Value(val)
		case uint16:
			*vs.Destination = Int16Value(val)
		case int16:
			*vs.Destination = Int16Value(val)
		case uint32:
			*vs.Destination = Int16Value(val)
		case int32:
			*vs.Destination = Int16Value(val)
		case uint64:
			*vs.Destination = Int16Value(val)
		case int64:
			*vs.Destination = Int16Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Int32Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Int32Value(val)
		case int:
			*vs.Destination = Int32Value(val)
		case uint8:
			*vs.Destination = Int32Value(val)
		case int8:
			*vs.Destination = Int32Value(val)
		case uint16:
			*vs.Destination = Int32Value(val)
		case int16:
			*vs.Destination = Int32Value(val)
		case uint32:
			*vs.Destination = Int32Value(val)
		case int32:
			*vs.Destination = Int32Value(val)
		case uint64:
			*vs.Destination = Int32Value(val)
		case int64:
			*vs.Destination = Int32Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Int64Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Int64Value(val)
		case int:
			*vs.Destination = Int64Value(val)
		case uint8:
			*vs.Destination = Int64Value(val)
		case int8:
			*vs.Destination = Int64Value(val)
		case uint16:
			*vs.Destination = Int64Value(val)
		case int16:
			*vs.Destination = Int64Value(val)
		case uint32:
			*vs.Destination = Int64Value(val)
		case int32:
			*vs.Destination = Int64Value(val)
		case uint64:
			*vs.Destination = Int64Value(val)
		case int64:
			*vs.Destination = Int64Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Uint8Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Uint8Value(val)
		case int:
			*vs.Destination = Uint8Value(val)
		case uint8:
			*vs.Destination = Uint8Value(val)
		case int8:
			*vs.Destination = Uint8Value(val)
		case uint16:
			*vs.Destination = Uint8Value(val)
		case int16:
			*vs.Destination = Uint8Value(val)
		case uint32:
			*vs.Destination = Uint8Value(val)
		case int32:
			*vs.Destination = Uint8Value(val)
		case uint64:
			*vs.Destination = Uint8Value(val)
		case int64:
			*vs.Destination = Uint8Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Uint16Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Uint16Value(val)
		case int:
			*vs.Destination = Uint16Value(val)
		case uint8:
			*vs.Destination = Uint16Value(val)
		case int8:
			*vs.Destination = Uint16Value(val)
		case uint16:
			*vs.Destination = Uint16Value(val)
		case int16:
			*vs.Destination = Uint16Value(val)
		case uint32:
			*vs.Destination = Uint16Value(val)
		case int32:
			*vs.Destination = Uint16Value(val)
		case uint64:
			*vs.Destination = Uint16Value(val)
		case int64:
			*vs.Destination = Uint16Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Uint32Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Uint32Value(val)
		case int:
			*vs.Destination = Uint32Value(val)
		case uint8:
			*vs.Destination = Uint32Value(val)
		case int8:
			*vs.Destination = Uint32Value(val)
		case uint16:
			*vs.Destination = Uint32Value(val)
		case int16:
			*vs.Destination = Uint32Value(val)
		case uint32:
			*vs.Destination = Uint32Value(val)
		case int32:
			*vs.Destination = Uint32Value(val)
		case uint64:
			*vs.Destination = Uint32Value(val)
		case int64:
			*vs.Destination = Uint32Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Uint64Value:
		switch val := val.(type) {
		case uint:
			*vs.Destination = Uint64Value(val)
		case int:
			*vs.Destination = Uint64Value(val)
		case uint8:
			*vs.Destination = Uint64Value(val)
		case int8:
			*vs.Destination = Uint64Value(val)
		case uint16:
			*vs.Destination = Uint64Value(val)
		case int16:
			*vs.Destination = Uint64Value(val)
		case uint32:
			*vs.Destination = Uint64Value(val)
		case int32:
			*vs.Destination = Uint64Value(val)
		case uint64:
			*vs.Destination = Uint64Value(val)
		case int64:
			*vs.Destination = Uint64Value(val)
		case string:
			// This code assumes that the string perfectly represents an uint64
			n := uint64(0)
			for i := 0; i < len(val); i++ {
				n = (n * 10) + uint64(val[i]-'0')
			}
			*vs.Destination = Uint64Value(n)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Float32Value:
		switch val := val.(type) {
		case float32:
			*vs.Destination = Float32Value(val)
		case float64:
			*vs.Destination = Float32Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case Float64Value:
		switch val := val.(type) {
		case float32:
			*vs.Destination = Float64Value(val)
		case float64:
			*vs.Destination = Float64Value(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	case StringValue:
		switch val := val.(type) {
		case string:
			*vs.Destination = StringValue(val)
		case []byte:
			*vs.Destination = StringValue(val)
		default:
			return errors.New(fmt.Sprintf("cannot convert %T to %T", val, dest))
		}
	default:
		return errors.New(fmt.Sprintf("unknown value type %v", dest))
	}
	return nil
}
