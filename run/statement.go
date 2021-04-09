package run

import (
	"fmt"
	"strings"

	"github.com/dolthub/fuzzer/rand"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/ranges"
	"github.com/dolthub/fuzzer/types"
)

// Statement generates a random statement based on the given table and its data. The type of returned statement is
// dependent on the implementor.
type Statement interface {
	ranges.Distributable
	// GenerateStatement takes the given table and generates a random statement conforming to the table's data, constraints,
	// and indexes.
	GenerateStatement(table *Table) (string, error)
}

// InsertStatement returns random statements that are all INSERT statements.
type InsertStatement struct {
	r ranges.Int
}

var _ Statement = (*InsertStatement)(nil)

// GetOccurrenceRate implements the interface ranges.Distributable.
func (s *InsertStatement) GetOccurrenceRate() (int64, error) {
	return s.r.RandomValue()
}

// GenerateStatement implements the interface Statement.
func (s *InsertStatement) GenerateStatement(table *Table) (string, error) {
	for i := 0; i < 10000000; i++ {
		row, err := NewRow(table)
		if err != nil {
			return "", errors.Wrap(err)
		}
		if !table.ContainsKey(row) {
			table.Put(row)
			return fmt.Sprintf("INSERT INTO `%s` VALUES (%s);", table.Name, row.String()), nil
		}
	}
	return "", errors.New("10 million consecutive collisions on attempted INSERT, aborting cycle")
}

// ReplaceStatement returns random statements that are all REPLACE statements.
type ReplaceStatement struct {
	r ranges.Int
}

var _ Statement = (*ReplaceStatement)(nil)

// GetOccurrenceRate implements the interface ranges.Distributable.
func (s *ReplaceStatement) GetOccurrenceRate() (int64, error) {
	return s.r.RandomValue()
}

// GenerateStatement implements the interface Statement.
func (s *ReplaceStatement) GenerateStatement(table *Table) (string, error) {
	row, err := NewRow(table)
	if err != nil {
		return "", errors.Wrap(err)
	}
	table.Put(row)
	return fmt.Sprintf("REPLACE INTO `%s` VALUES (%s);", table.Name, row.String()), nil
}

// UpdateStatement returns random statements that are usually UPDATE statements. In the event that an UPDATE statement
// cannot be generated (such as with an empty table), a REPLACE statement is generated instead.
type UpdateStatement struct {
	r ranges.Int
}

//TODO: Add OR/LIKE/etc. to the WHERE clause
var _ Statement = (*UpdateStatement)(nil)

// GetOccurrenceRate implements the interface ranges.Distributable.
func (s *UpdateStatement) GetOccurrenceRate() (int64, error) {
	return s.r.RandomValue()
}

// GenerateStatement implements the interface Statement.
func (s *UpdateStatement) GenerateStatement(table *Table) (string, error) {
	row, ok, err := table.GetRandomRow()
	if err != nil {
		return "", errors.Wrap(err)
	}
	// If there are no rows then we switch to a REPLACE.
	// TODO: remove restriction for keyed tables only
	// TODO: allow updating primary keys
	if !ok || len(table.PKCols) == 0 || len(table.NonPKCols) == 0 {
		return (&ReplaceStatement{}).GenerateStatement(table)
	}
	val, err := NewRowValue(table)
	if err != nil {
		return "", errors.Wrap(err)
	}

	cut := uint16(1)
	if len(table.NonPKCols) > 1 {
		cut, err = rand.Uint16()
		if err != nil {
			return "", errors.Wrap(err)
		}
		cut = (cut % (uint16(len(table.NonPKCols)) - 1)) + 1
	}
	for i := int(cut); i < len(table.NonPKCols); i++ {
		val[i] = row.Value[i]
	}
	row.Value = val

	sets, err := valsToColumnEquals(table.NonPKCols[:cut], val[:cut])
	if err != nil {
		return "", errors.Wrap(err)
	}
	wheres, err := valsToColumnEquals(table.PKCols, row.Key)
	if err != nil {
		return "", errors.Wrap(err)
	}
	table.Put(row)
	return fmt.Sprintf("UPDATE `%s` SET %s WHERE %s;", table.Name, strings.Join(sets, ","), strings.Join(wheres, " AND ")), nil
}

// DeleteStatement returns random statements that are usually DELETE statements. In the event that a DELETE statement
// cannot be generated (such as with an empty table), a REPLACE statement is generated instead.
type DeleteStatement struct {
	r ranges.Int
}

var _ Statement = (*DeleteStatement)(nil)

// GetOccurrenceRate implements the interface ranges.Distributable.
func (s *DeleteStatement) GetOccurrenceRate() (int64, error) {
	return s.r.RandomValue()
}

// GenerateStatement implements the interface Statement.
func (s *DeleteStatement) GenerateStatement(table *Table) (string, error) {
	row, ok, err := table.GetRandomRow()
	if err != nil {
		return "", errors.Wrap(err)
	}
	// If there are no rows then we switch to a REPLACE.
	// TODO: remove restriction for keyed tables only
	if !ok || len(table.PKCols) == 0 {
		return (&ReplaceStatement{}).GenerateStatement(table)
	}

	wheres, err := valsToColumnEquals(table.PKCols, row.Key)
	if err != nil {
		return "", errors.Wrap(err)
	}
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s;", table.Name, strings.Join(wheres, " AND ")), nil
}

// valsToColumnEquals returns a slice of strings of the form "`column_name` = value" from the given parameters.
// Expects both slices to be of equal length.
func valsToColumnEquals(colNames []*Column, vals []types.Value) ([]string, error) {
	if len(colNames) != len(vals) {
		return nil, errors.New(fmt.Sprintf("length mismatch: columns %d, vals %d", len(colNames), len(vals)))
	}
	s := make([]string, len(colNames))
	for i := 0; i < len(colNames); i++ {
		s[i] = fmt.Sprintf("`%s` = %s", colNames[i].Name, vals[i].String())
	}
	return s, nil
}
