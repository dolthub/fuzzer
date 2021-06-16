package types

import (
	"database/sql"
)

// ValueScanner handles the conversion of a value to its destination type.
type ValueScanner struct {
	Destination *Value
}

var _ sql.Scanner = ValueScanner{}

// NewValueScanner returns a new ValueScanner with the given ValuePrimitive pointer as the destination.
func NewValueScanner(destination *Value) ValueScanner {
	return ValueScanner{destination}
}

// Scan implements the interface sql.Scanner.
func (vs ValueScanner) Scan(val interface{}) error {
	var err error
	if val == nil {
		*vs.Destination = NilValue{}
	} else {
		*vs.Destination, err = (*vs.Destination).Convert(val)
	}
	return err
}
