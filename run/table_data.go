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
	"context"
	"database/sql"
	"fmt"

	_ "github.com/dolthub/go-sqlite3"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/types"
)

var sqliteDb *sql.DB

func init() {
	var err error
	sqliteDb, err = sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(err)
	}
}

// TableData represents a table's data, along with its index data.
type TableData struct {
	tableName  string
	pkCols     []*Column
	nonPKCols  []*Column
	connection *sql.Conn
}

// TableDataCursor returns a table's data, one row at a time.
type TableDataCursor struct {
	rows     *sql.Rows
	template Row
	td       *TableData
}

// CreateTableData creates a new TableData and returns it.
func CreateTableData(tableName, createTableStatement string, pkCols, nonPKCols []*Column) (*TableData, error) {
	conn, err := sqliteDb.Conn(context.Background())
	if err != nil {
		return nil, errors.Wrap(err)
	}
	_, err = conn.ExecContext(context.Background(), createTableStatement)
	if err != nil {
		_ = conn.Close()
		return nil, errors.Wrap(err)
	}
	return &TableData{tableName, pkCols, nonPKCols, conn}, nil
}

// Exec executes the given statement.
func (td *TableData) Exec(statement string) error {
	_, err := td.connection.ExecContext(context.Background(), statement)
	return err
}

// ConstructTemplateRow creates a row with each value set to the equivalent types.ValuePrimitive for that position relative to its
// column on the table. This is intended to be used as a destination row for reading from table data.
func (td *TableData) ConstructTemplateRow() Row {
	pkColsLen := len(td.pkCols)
	nonPKColsLen := len(td.nonPKCols)
	vals := make([]types.Value, pkColsLen+nonPKColsLen)
	for i := 0; i < pkColsLen; i++ {
		vals[i] = td.pkCols[i].Type.TypeValue()
	}
	for i := 0; i < nonPKColsLen; i++ {
		vals[pkColsLen+i] = td.nonPKCols[i].Type.TypeValue()
	}
	return Row{
		Values:    vals,
		PkColsLen: int32(pkColsLen),
	}
}

// GetRowCount returns the number of rows in the table.
func (td *TableData) GetRowCount() (int64, error) {
	rows := td.connection.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`;", td.tableName))
	count := int64(0)
	err := rows.Scan(&count)
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return count, nil
}

// GetRandomRow returns a random row from all rows in the table. Returns true only if the table is non-empty.
func (td *TableData) GetRandomRow() (Row, bool, error) {
	rowCount, err := td.GetRowCount()
	if err != nil {
		return Row{}, false, errors.Wrap(err)
	}
	if rowCount == 0 {
		return Row{}, false, nil
	}
	randVal, err := rand.Int64()
	if err != nil {
		return Row{}, false, errors.Wrap(err)
	}
	outRow := td.connection.QueryRowContext(context.Background(), fmt.Sprintf("SELECT * FROM `%s` LIMIT 1 OFFSET %d;", td.tableName, randVal%rowCount))
	row := td.ConstructTemplateRow()
	iVals := make([]interface{}, len(row.Values))
	for i := range row.Values {
		iVals[i] = types.NewValueScanner(&row.Values[i])
	}
	err = outRow.Scan(iVals...)
	if err != nil {
		return Row{}, false, errors.Wrap(err)
	}
	return row, true, nil
}

// GetAllRows returns all of the rows in the table.
func (td *TableData) GetAllRows() ([]Row, error) {
	rowCount, err := td.GetRowCount()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	if rowCount == 0 {
		return nil, nil
	}
	outRows, err := td.connection.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM `%s`;", td.tableName))
	if err != nil {
		return nil, errors.Wrap(err)
	}
	defer outRows.Close()
	allRows := make([]Row, rowCount)
	rowIdx := int64(0)
	templateRow := td.ConstructTemplateRow()
	for ; outRows.Next() && rowIdx < rowCount; rowIdx++ {
		row := templateRow.Copy()
		iVals := make([]interface{}, len(row.Values))
		for j := range row.Values {
			iVals[j] = types.NewValueScanner(&row.Values[j])
		}
		err = outRows.Scan(iVals...)
		if err != nil {
			return nil, errors.Wrap(err)
		}
		allRows[rowIdx] = row
	}
	if rowIdx < rowCount {
		return nil, errors.New(fmt.Sprintf("expected %d rows from `%s` but only read %d", rowCount, td.tableName, rowIdx))
	}
	if outRows.Next() {
		return nil, errors.New(fmt.Sprintf("expected %d rows from `%s` but there were more", rowCount, td.tableName))
	}
	return allRows, nil
}

// GetRowCursor returns a cursor for the table data.
func (td *TableData) GetRowCursor() (*TableDataCursor, error) {
	orderBy := ""
	for i := 1; i <= len(td.pkCols); i++ {
		if i == 1 {
			orderBy += " ORDER BY 1"
		} else {
			orderBy += fmt.Sprintf(", %d", i)
		}
	}
	outRows, err := td.connection.QueryContext(context.Background(), fmt.Sprintf("SELECT * FROM `%s`%s;", td.tableName, orderBy))
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &TableDataCursor{
		rows:     outRows,
		template: td.ConstructTemplateRow(),
		td:       td,
	}, nil
}

// Copy returns an exact copy of the contained table and index data.
func (td *TableData) Copy() (*TableData, error) {
	outCreateTableStmt := td.connection.QueryRowContext(context.Background(), fmt.Sprintf("SELECT sql FROM sqlite_master WHERE name = '%s';", td.tableName))
	createTableStmt := ""
	err := outCreateTableStmt.Scan(&createTableStmt)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	pkCols := make([]*Column, len(td.pkCols))
	for i := 0; i < len(td.pkCols); i++ {
		pkCols[i] = td.pkCols[i].Copy()
	}
	nonPKCols := make([]*Column, len(td.nonPKCols))
	for i := 0; i < len(td.nonPKCols); i++ {
		nonPKCols[i] = td.nonPKCols[i].Copy()
	}
	newTableData, err := CreateTableData(td.tableName, createTableStmt, pkCols, nonPKCols)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	oldDataCursor, err := td.GetRowCursor()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	defer oldDataCursor.Close()

	row, ok, err := oldDataCursor.NextRow()
	for ; err == nil && ok; row, ok, err = oldDataCursor.NextRow() {
		err = newTableData.Exec(fmt.Sprintf("INSERT INTO `%s` VALUES (%s);", newTableData.tableName, row.SQLiteString()))
		if err != nil {
			return nil, errors.Wrap(err)
		}
	}
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return newTableData, nil
}

// Close closes the underlying connection and frees resources.
func (td *TableData) Close() {
	defer func() {
		_ = recover()
	}()
	_ = td.connection.Close()
}

// NextRow returns the next row from the cursor. If there are no more rows to return, returns false.
func (tdc *TableDataCursor) NextRow() (Row, bool, error) {
	if tdc.rows.Next() {
		row := tdc.template.Copy()
		iVals := make([]interface{}, len(row.Values))
		for i := range row.Values {
			iVals[i] = types.NewValueScanner(&row.Values[i])
		}
		err := tdc.rows.Scan(iVals...)
		if err != nil {
			return Row{}, false, errors.Wrap(err)
		}
		return row, true, nil
	}
	return Row{}, false, nil
}

// Close closes the underlying cursor and frees resources.
func (tdc *TableDataCursor) Close() {
	_ = tdc.rows.Close()
}
