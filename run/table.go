package run

import (
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/types"
)

// Table represents a table in dolt.
type Table struct {
	Parent    *Commit
	Name      string
	PKCols    []*Column
	NonPKCols []*Column
	Indexes   []*Index
	Data      *TableData
}

// NewTable returns a *Table.
func NewTable(parent *Commit, name string, pkCols []*Column, nonPKCols []*Column, indexes []*Index) (*Table, error) {
	table := &Table{
		Parent:    parent,
		Name:      name,
		PKCols:    pkCols,
		NonPKCols: nonPKCols,
		Indexes:   indexes,
	}
	var err error
	table.Data, err = CreateTableData(name, table.CreateString(true), pkCols, nonPKCols)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return table, nil
}

// CreateString returns the table as a `CREATE TABLE` string. Setting `sqlite` to true removes collations and other
// MySQL-specific strings that SQLite fails on.
func (t *Table) CreateString(sqlite bool) string {
	needComma := false
	sb := strings.Builder{}
	sb.Grow(512)
	sb.WriteString("CREATE TABLE `")
	sb.WriteString(t.Name)
	sb.WriteString("` (")
	for _, col := range t.PKCols {
		if needComma {
			sb.WriteString(", ")
		}
		needComma = true
		sb.WriteRune('`')
		sb.WriteString(col.Name)
		sb.WriteString("` ")
		sb.WriteString(col.Type.Name(sqlite))
	}
	for _, col := range t.NonPKCols {
		if needComma {
			sb.WriteString(", ")
		}
		needComma = true
		sb.WriteRune('`')
		sb.WriteString(col.Name)
		sb.WriteString("` ")
		sb.WriteString(col.Type.Name(sqlite))
	}
	if len(t.PKCols) > 0 {
		sb.WriteString(", PRIMARY KEY (")
		for i, col := range t.PKCols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteRune('`')
			sb.WriteString(col.Name)
			sb.WriteRune('`')
		}
		sb.WriteRune(')')
	}
	for _, index := range t.Indexes {
		sb.WriteString(", ")
		sb.WriteString(index.String())
	}
	if !sqlite {
		for _, fk := range t.Parent.ForeignKeys {
			if fk.TableName != t.Name {
				continue
			}
			sb.WriteString(", ")
			sb.WriteString(fk.String())
		}
	}
	sb.WriteString(");")
	return sb.String()
}

// Copy returns a deep copy of the table.
func (t *Table) Copy() (*Table, error) {
	pkCols := make([]*Column, len(t.PKCols))
	for i := 0; i < len(t.PKCols); i++ {
		pkCols[i] = t.PKCols[i].Copy()
	}
	nonPKCols := make([]*Column, len(t.NonPKCols))
	for i := 0; i < len(t.NonPKCols); i++ {
		nonPKCols[i] = t.NonPKCols[i].Copy()
	}
	indexes := make([]*Index, len(t.Indexes))
	for i := 0; i < len(t.Indexes); i++ {
		indexes[i] = t.Indexes[i].Copy()
	}
	newData, err := t.Data.Copy()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &Table{
		Parent:    t.Parent,
		Name:      t.Name,
		PKCols:    pkCols,
		NonPKCols: nonPKCols,
		Indexes:   indexes,
		Data:      newData,
	}, nil
}

// Column represents a table column in dolt.
type Column struct {
	//TODO: allow some non-pk columns to be non-nullable
	Name string
	Type types.TypeInstance
}

// Copy returns a deep copy of the column.
func (c *Column) Copy() *Column {
	return &Column{
		Name: c.Name,
		Type: c.Type,
	}
}
