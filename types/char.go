package types

import (
	"fmt"
	"math"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Char represents the CHAR MySQL type.
type Char struct {
	Collations   []string
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Char)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (c *Char) GetOccurrenceRate() (int64, error) {
	return c.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (c *Char) Instance() (TypeInstance, error) {
	charLength, err := c.Length.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	colPos %= uint64(len(c.Collations))
	collation, err := sql.ParseCollation(nil, &c.Collations[colPos], false)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &CharInstance{int(charLength), ranges.NewInt([]int64{0, charLength}), collation}, nil
}

// CharInstance is the TypeInstance of Char.
type CharInstance struct {
	charLength int
	length     ranges.Int
	collation  sql.Collation
}

var _ TypeInstance = (*CharInstance)(nil)

// Get implements the TypeInstance interface.
func (i *CharInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return StringValue(v), err
}

// TypeValue implements the TypeInstance interface.
func (i *CharInstance) TypeValue() Value {
	return StringValue("")
}

// Name implements the TypeInstance interface.
func (i *CharInstance) Name(sqlite bool) string {
	if sqlite {
		return fmt.Sprintf("CHAR(%d)", i.charLength)
	}
	return fmt.Sprintf("CHAR(%d) COLLATE %s", i.charLength, i.collation.String())
}

// MaxValueCount implements the TypeInstance interface.
func (i *CharInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.charLength))
}
