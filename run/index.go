package run

import (
	"fmt"
	"strings"
)

// Index represents an index in dolt.
type Index struct {
	Name     string
	IsUnique bool
	Columns  []string
	//TODO: track data for foreign keys
}

// NewIndex returns an *Index.
func NewIndex(name string, columns []string, isUnique bool) *Index {
	return &Index{
		Name:     name,
		IsUnique: isUnique,
		Columns:  columns,
	}
}

// String returns the index as a string. May be used in a `CREATE TABLE` statement.
func (i Index) String() string {
	unique := ""
	if i.IsUnique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("%sINDEX `%s` (`%s`)", unique, i.Name, strings.Join(i.Columns, "`,`"))
}

// CreateString returns the index as a `CREATE INDEX` statement.
func (i *Index) CreateString(tableName string) string {
	unique := ""
	if i.IsUnique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX `%s` ON `%s`(`%s`)", unique, i.Name, tableName, strings.Join(i.Columns, "`,`"))
}

// Copy returns a deep copy of the index.
func (i *Index) Copy() *Index {
	columns := make([]string, len(i.Columns))
	copy(columns, i.Columns)
	return &Index{
		Name:     i.Name,
		IsUnique: i.IsUnique,
		Columns:  columns,
	}
}
