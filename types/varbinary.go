package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Varbinary represents the VARBINARY MySQL type.
type Varbinary struct {
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Varbinary)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (v *Varbinary) GetOccurrenceRate() (int64, error) {
	return v.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (v *Varbinary) Instance() (TypeInstance, error) {
	charLength, err := v.Length.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &VarbinaryInstance{int(charLength), ranges.NewInt([]int64{0, charLength})}, nil
}

// VarbinaryInstance is the TypeInstance of Varbinary.
type VarbinaryInstance struct {
	charLength int
	length     ranges.Int
}

var _ TypeInstance = (*VarbinaryInstance)(nil)

// Get implements the TypeInstance interface.
func (i *VarbinaryInstance) Get() (Value, error) {
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

// Name implements the TypeInstance interface.
func (i *VarbinaryInstance) Name() string {
	return fmt.Sprintf("VARBINARY(%d)", i.charLength)
}

// MaxValueCount implements the TypeInstance interface.
func (i *VarbinaryInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.charLength))
}
