package run

import (
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/types"
)

// Row represents a row, similar to dolt, which consists of a key and value. Dolt uses tuples for the key and value,
// while we just use a slice of values.
type Row struct {
	Key   []types.Value
	Value []types.Value
}

// NewRow returns a new row of random values. The row conforms to the schema of the given table.
func NewRow(table *Table) (Row, error) {
	key, err := NewRowKey(table)
	if err != nil {
		return Row{}, errors.Wrap(err)
	}
	value, err := NewRowValue(table)
	if err != nil {
		return Row{}, errors.Wrap(err)
	}
	return Row{
		Key:   key,
		Value: value,
	}, nil
}

// NewRowKey returns a new row key of random values. The key conforms to the primary key(s) of the given table.
func NewRowKey(table *Table) ([]types.Value, error) {
	var err error
	key := make([]types.Value, len(table.PKCols))
	for i := 0; i < len(table.PKCols); i++ {
		key[i], err = table.PKCols[i].Type.Get()
		if err != nil {
			return nil, errors.Wrap(err)
		}
	}
	return key, nil
}

// NewRowValue returns a new row value of random values. The value conforms to the non-primary key(s) of the given table.
func NewRowValue(table *Table) ([]types.Value, error) {
	var err error
	value := make([]types.Value, len(table.NonPKCols))
	for i := 0; i < len(table.NonPKCols); i++ {
		value[i], err = table.NonPKCols[i].Type.Get()
		if err != nil {
			return nil, errors.Wrap(err)
		}
	}
	return value, nil
}

// String returns the row as a comma-separated string.
func (r Row) String() string {
	keyLen := len(r.Key)
	valLen := len(r.Value)
	vals := make([]string, keyLen+valLen)
	for i := 0; i < keyLen; i++ {
		vals[i] = r.Key[i].String()
	}
	for i := 0; i < valLen; i++ {
		vals[keyLen+i] = r.Value[i].String()
	}
	return strings.Join(vals, ",")
}
