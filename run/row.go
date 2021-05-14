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

// MySQLString returns the row as a comma-separated string. Intended for MySQL usage.
func (r Row) MySQLString() string {
	vals := make([]string, len(r.Values))
	for i := 0; i < len(vals); i++ {
		vals[i] = r.Values[i].String()
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

// Copy returns a copy of this row.
func (r Row) Copy() Row {
	vals := make([]types.Value, len(r.Values))
	copy(vals, r.Values)
	return Row{
		Values:    vals,
		PkColsLen: r.PkColsLen,
	}
}
