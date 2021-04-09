package run

import (
	"fmt"
	"strings"
)

// ForeignKeyReferenceOption is the reference option for a foreign key. Does not include all available options, as all
// missing options are functionally equivalent to the ones present.
type ForeignKeyReferenceOption byte

const (
	ForeignKeyReferenceOption_Restrict ForeignKeyReferenceOption = iota
	ForeignKeyReferenceOption_Cascade
	ForeignKeyReferenceOption_SetNull
)

// ForeignKey represents a foreign key in dolt.
type ForeignKey struct {
	Name                string
	TableName           string
	TableCols           []string
	ReferencedTableName string
	ReferencedTableCols []string
	OnUpdate            ForeignKeyReferenceOption
	OnDelete            ForeignKeyReferenceOption
}

// String returns the foreign key as a string. May be used in a `CREATE TABLE` statement.
func (fk *ForeignKey) String() string {
	return fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)",
		fk.Name, strings.Join(fk.TableCols, "`,`"), fk.ReferencedTableName, strings.Join(fk.ReferencedTableCols, "`,`"))
}

// AlterString returns the foreign key as an `ALTER TABLE` statement.
func (fk *ForeignKey) AlterString(tableName string) string {
	return fmt.Sprintf("ALTER TABLE `%s` ADD %s", tableName, fk.String())
}

// Copy returns a deep copy of the foreign key.
func (fk *ForeignKey) Copy() *ForeignKey {
	tableCols := make([]string, len(fk.TableCols))
	copy(tableCols, fk.TableCols)
	refTableCols := make([]string, len(fk.ReferencedTableCols))
	copy(refTableCols, fk.ReferencedTableCols)
	return &ForeignKey{
		Name:                fk.Name,
		TableName:           fk.TableName,
		TableCols:           tableCols,
		ReferencedTableName: fk.ReferencedTableName,
		ReferencedTableCols: refTableCols,
		OnUpdate:            fk.OnUpdate,
		OnDelete:            fk.OnDelete,
	}
}
