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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/gocraft/dbr/v2"

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

// DoltDataCursor returns a Dolt repository's data, one row at a time.
type DoltDataCursor struct {
	conn     *dbr.Connection
	rows     *sql.Rows
	template Row
	process  *os.Process
	errBuf   *bytes.Buffer
	once     *sync.Once
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
	table.Data, err = CreateTableData(name, table.CreateString(false, true), pkCols, nonPKCols)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return table, nil
}

// CreateString returns the table as a `CREATE TABLE` string. Setting `columnOnly` to true leaves only the column name,
// type, and primary key. Setting `sqlite` to true removes collations and other MySQL-specific strings that SQLite fails on.
func (t *Table) CreateString(columnOnly bool, sqlite bool) string {
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
	if !columnOnly {
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
	}
	sb.WriteString(");")
	return sb.String()
}

// DoltTableHasConflicts returns whether the Dolt table has any conflicts.
func (t *Table) DoltTableHasConflicts(c *Cycle) (bool, error) {
	out, err := c.CliQuery("sql", "-q", "SELECT COUNT(*) FROM dolt_conflicts_"+t.Name, "-r=json")
	if err != nil {
		return false, errors.Wrap(err)
	}
	return out != `{"rows": [{"COUNT(*)":0}]}`, nil
}

// GetDoltCursor returns a cursor over Dolt's stored table data.
func (t *Table) GetDoltCursor(c *Cycle) (*DoltDataCursor, error) {
	conn, process, stdErrBuffer, err := c.SqlServer.GetConnection()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	orderBy := ""
	for i := 1; i <= len(t.PKCols); i++ {
		if i == 1 {
			orderBy += " ORDER BY 1"
		} else {
			orderBy += fmt.Sprintf(", %d", i)
		}
	}
	outRows, err := conn.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM `%s`%s;", t.Name, orderBy))
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &DoltDataCursor{
		conn:     conn,
		rows:     outRows,
		template: t.Data.ConstructTemplateRow(),
		process:  process,
		errBuf:   stdErrBuffer,
		once:     &sync.Once{},
	}, nil
}

// GetDoltConflictsCursor returns a cursor over Dolt's conflicts for this table. This returns an error if there are no
// conflicts to iterate over, therefore it is best to check for conflicts first using DoltTableHasConflicts.
func (t *Table) GetDoltConflictsCursor(c *Cycle) (*DoltDataCursor, error) {
	conn, process, stdErrBuffer, err := c.SqlServer.GetConnection()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colsToSelect := ""
	for _, col := range t.PKCols {
		colsToSelect += fmt.Sprintf(",`base_%s`", col.Name)
	}
	for _, col := range t.NonPKCols {
		colsToSelect += fmt.Sprintf(",`base_%s`", col.Name)
	}
	for _, col := range t.PKCols {
		colsToSelect += fmt.Sprintf(",`our_%s`", col.Name)
	}
	for _, col := range t.NonPKCols {
		colsToSelect += fmt.Sprintf(",`our_%s`", col.Name)
	}
	for _, col := range t.PKCols {
		colsToSelect += fmt.Sprintf(",`their_%s`", col.Name)
	}
	for _, col := range t.NonPKCols {
		colsToSelect += fmt.Sprintf(",`their_%s`", col.Name)
	}
	allColsLen := len(t.PKCols) + len(t.NonPKCols)
	orderBy := ""
	for i := 1; i <= allColsLen*3; i++ {
		if i == 1 {
			orderBy += " ORDER BY 1"
		} else {
			orderBy += fmt.Sprintf(",%d", i)
		}
	}
	outRows, err := conn.QueryContext(context.Background(), fmt.Sprintf("SELECT %s FROM `dolt_conflicts_%s`%s;", colsToSelect[1:], t.Name, orderBy))
	if err != nil {
		return nil, errors.Wrap(err)
	}
	baselineTemplate := t.Data.ConstructTemplateRow()
	fullTemplateVals := make([]types.Value, 3*allColsLen)
	copy(fullTemplateVals[:allColsLen], baselineTemplate.Values)
	copy(fullTemplateVals[allColsLen:2*allColsLen], baselineTemplate.Values)
	copy(fullTemplateVals[2*allColsLen:], baselineTemplate.Values)
	templateRow := Row{
		Values:    fullTemplateVals,
		PkColsLen: 0,
	}
	return &DoltDataCursor{
		conn:     conn,
		rows:     outRows,
		template: templateRow,
		process:  process,
		errBuf:   stdErrBuffer,
		once:     &sync.Once{},
	}, nil
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

// NextRow returns the next row from the cursor. If there are no more rows to return, returns false.
func (ddc *DoltDataCursor) NextRow() (Row, bool, error) {
	if ddc.rows.Next() {
		row := ddc.template.Copy()
		iVals := make([]interface{}, len(row.Values))
		for i := range row.Values {
			iVals[i] = types.NewValueScanner(&row.Values[i])
		}
		err := ddc.rows.Scan(iVals...)
		if err != nil {
			return Row{}, false, errors.Wrap(err)
		}
		return row, true, nil
	}
	return Row{}, false, nil
}

// Close closes the underlying cursor and frees resources.
func (ddc *DoltDataCursor) Close() error {
	var err error
	ddc.once.Do(func() {
		rErr := ddc.rows.Close()
		cErr := ddc.conn.Close()
		pErr := ddc.process.Kill()
		if rErr != nil {
			err = errors.Wrap(rErr)
			return
		}
		if cErr != nil {
			err = errors.Wrap(cErr)
			return
		}
		if pErr != nil {
			err = errors.Wrap(pErr)
			return
		}
	})
	return err
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
