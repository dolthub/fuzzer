package run

import (
	"strings"

	"github.com/emirpasic/gods/trees/avltree"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/types"
)

// Table represents a table in dolt.
type Table struct {
	Parent    *Commit
	Name      string
	PKCols    []*Column
	NonPKCols []*Column
	Indexes   []*Index
	Data      *avltree.Tree
}

// NewTable returns a *Table.
func NewTable(parent *Commit, name string, pkCols []*Column, nonPKCols []*Column, indexes []*Index) *Table {
	table := &Table{
		Parent:    parent,
		Name:      name,
		PKCols:    pkCols,
		NonPKCols: nonPKCols,
		Indexes:   indexes,
	}
	table.Data = avltree.NewWith(func(a, b interface{}) int {
		aKey, bKey := a.([]types.Value), b.([]types.Value)
		for i := 0; i < len(aKey); i++ {
			val := aKey[i].Compare(bKey[i])
			if val != 0 {
				return val
			}
		}
		return 0
	})
	return table
}

// CreateString returns the table as a `CREATE TABLE` string.
func (t *Table) CreateString() string {
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
		sb.WriteString(col.Type.Name())
	}
	for _, col := range t.NonPKCols {
		if needComma {
			sb.WriteString(", ")
		}
		needComma = true
		sb.WriteRune('`')
		sb.WriteString(col.Name)
		sb.WriteString("` ")
		sb.WriteString(col.Type.Name())
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
	for _, fk := range t.Parent.ForeignKeys {
		if fk.TableName != t.Name {
			continue
		}
		sb.WriteString(", ")
		sb.WriteString(fk.String())
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
	dataJson, err := t.Data.ToJSON()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	newData := avltree.NewWith(t.Data.Comparator)
	err = newData.FromJSON(dataJson)
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

// GetRandomRow returns a random row that has been written to the table. Returns false if the table is empty.
func (t *Table) GetRandomRow() (Row, bool, error) {
	// Although the trees aren't complete bsts, we can pretend they are.
	// This means the random distribution is far from even, but it simplifies the logic.
	if t.Data.Size() == 0 {
		return Row{}, false, nil
	}
	uval, err := rand.Uint64()
	if err != nil {
		return Row{}, false, errors.Wrap(err)
	}
	val := int64(uval % uint64(t.Data.Size()))
	path, err := rand.Int64()
	if err != nil {
		return Row{}, false, errors.Wrap(err)
	}
	node := t.Data.Root
	for i := int64(0); i < 63; i++ {
		power := int64(1) << i
		val -= int64(power)
		if val < 0 {
			return Row{
				Key:   node.Key.([]types.Value),
				Value: node.Value.([]types.Value),
			}, true, nil
		}
		if (path&power) == 0 && node.Children[0] != nil {
			node = node.Children[0]
			continue
		}
		if node.Children[1] != nil {
			node = node.Children[1]
			continue
		}
		return Row{
			Key:   node.Key.([]types.Value),
			Value: node.Value.([]types.Value),
		}, true, nil
	}
	return Row{}, false, nil
}

// Put adds the row to the table. If a row exists with the same key, then it is overwritten. Assumes that the row
// conforms to the schema of the table.
func (t *Table) Put(row Row) {
	t.Data.Put(row.Key, row.Value)
}

// ContainsKey returns whether the key exists in the table. Assumes that the row conforms to the schema of the table.
func (t *Table) ContainsKey(row Row) bool {
	_, ok := t.Data.Get(row.Key)
	return ok
}

// Contains returns whether the full row exists in the table. Assumes that the row conforms to the schema of the table.
func (t *Table) Contains(row Row) bool {
	valInt, ok := t.Data.Get(row.Key)
	if !ok {
		return false
	}
	val := valInt.([]types.Value)
	if len(val) != len(row.Value) {
		return false
	}
	for i := 0; i < len(val); i++ {
		if row.Value[i].Compare(val[i]) != 0 {
			return false
		}
	}
	return true
}

// Remove removes a row with the matching key from the table. If the row does not exist, then no changes are made.
func (t *Table) Remove(row Row) {
	t.Data.Remove(row.Key)
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
