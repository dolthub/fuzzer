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
