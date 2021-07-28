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

package run

import (
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/types"
)

// Row represents a row, similar to dolt, which consists of a key and value. Dolt uses tuples for the key and value,
// while we just use a slice of values.
type Row struct {
	Values    []types.Value
	PkColsLen int32
}

// NewRow returns a new row of random values. The row conforms to the schema of the given table.
func NewRow(table *Table) (Row, error) {
	var err error
	pkColsLen := int32(len(table.PKCols))
	nonPKColsLen := int32(len(table.NonPKCols))
	vals := make([]types.Value, pkColsLen+nonPKColsLen)
	for i := int32(0); i < pkColsLen; i++ {
		vals[i], err = table.PKCols[i].Type.Get()
		if err != nil {
			return Row{}, errors.Wrap(err)
		}
	}
	for i := int32(0); i < nonPKColsLen; i++ {
		vals[pkColsLen+i], err = table.NonPKCols[i].Type.Get()
		if err != nil {
			return Row{}, errors.Wrap(err)
		}
	}
	return Row{
		Values:    vals,
		PkColsLen: pkColsLen,
	}, nil
}

// NewRowValue updates the non-key portion of the row with random values.
func (r Row) NewRowValue(table *Table) (Row, error) {
	var err error
	newRows := r.Copy()
	nonPKColsLen := int32(len(table.NonPKCols))
	for i := int32(0); i < nonPKColsLen; i++ {
		newRows.Values[newRows.PkColsLen+i], err = table.NonPKCols[i].Type.Get()
		if err != nil {
			return Row{}, errors.Wrap(err)
		}
	}
	return newRows, nil
}

// Key returns the key portion of the row.
func (r Row) Key() []types.Value {
	return r.Values[:r.PkColsLen]
}

// Value returns the non-key portion of the row.
func (r Row) Value() []types.Value {
	return r.Values[r.PkColsLen:]
}

// IsEmpty returns whether the row contains any values.
func (r Row) IsEmpty() bool {
	return len(r.Values) == 0
}

// MySQLString returns the row as a comma-separated string. Intended for MySQL usage.
func (r Row) MySQLString() string {
	vals := make([]string, len(r.Values))
	for i := 0; i < len(vals); i++ {
		vals[i] = r.Values[i].MySQLString()
	}
	return strings.Join(vals, ",")
}

// SQLiteString returns the row as a comma-separated string. Intended for SQLite usage.
func (r Row) SQLiteString() string {
	vals := make([]string, len(r.Values))
	for i := 0; i < len(vals); i++ {
		vals[i] = r.Values[i].SQLiteString()
	}
	return strings.Join(vals, ",")
}

// Equals returns whether the given row is equivalent to the calling row.
func (r Row) Equals(otherRow Row) bool {
	if len(r.Values) != len(otherRow.Values) {
		return false
	}
	for i := 0; i < len(r.Values); i++ {
		if r.Values[i] != otherRow.Values[i] {
			return false
		}
	}
	return true
}

// Compare returns an integer indicating the ordering of this row in relation to the given row. Empty rows will always
// return a greater value than non-empty rows.
func (r Row) Compare(otherRow Row) int {
	if len(r.Values) != len(otherRow.Values) {
		if len(r.Values) == 0 {
			return 1
		} else if len(otherRow.Values) == 0 {
			return -1
		} else if len(r.Values) < len(otherRow.Values) {
			return -1
		}
		return 1
	}
	for i := 0; i < len(r.Values); i++ {
		valComp := r.Values[i].Compare(otherRow.Values[i])
		if valComp == -1 {
			return -1
		} else if valComp == 1 {
			return 1
		}
	}
	return 0
}

// PKCompare returns an integer indicating the ordering of this row in relation to the given row. This evaluates only
// the primary keys. Empty rows will always return a greater value than non-empty rows.
func (r Row) PKCompare(otherRow Row) int {
	if len(r.Values) != len(otherRow.Values) {
		if len(r.Values) == 0 {
			return 1
		} else if len(otherRow.Values) == 0 {
			return -1
		} else if len(r.Values) < len(otherRow.Values) {
			return -1
		}
		return 1
	}
	if r.PkColsLen != otherRow.PkColsLen {
		if r.PkColsLen < otherRow.PkColsLen {
			return -1
		}
		return 1
	}
	for i := int32(0); i < r.PkColsLen; i++ {
		valComp := r.Values[i].Compare(otherRow.Values[i])
		if valComp == -1 {
			return -1
		} else if valComp == 1 {
			return 1
		}
	}
	return 0
}

// Copy returns a copy of this row.
func (r Row) Copy() Row {
	vals := make([]types.Value, len(r.Values))
	copy(vals, r.Values)
	return Row{
		Values:    vals,
		PkColsLen: r.PkColsLen,
	}
}
