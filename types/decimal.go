package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/utils"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Decimal represents the DECIMAL MySQL type.
type Decimal struct {
	Distribution ranges.Int
	Precision    ranges.Int
	Scale        ranges.Int
}

var _ Type = (*Decimal)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (d *Decimal) GetOccurrenceRate() (int64, error) {
	return d.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (d *Decimal) Instance() (TypeInstance, error) {
	precision, err := d.Precision.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	// Scale has a hard upper limit of 30 imposed by MySQL.
	scale, err := d.Precision.RandomValueRestrictUpper(utils.MinInt64(precision, 30))
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &DecimalInstance{int(precision), int(scale)}, nil
}

// DecimalInstance is the TypeInstance of Decimal.
type DecimalInstance struct {
	precision int
	scale     int
}

var _ TypeInstance = (*DecimalInstance)(nil)

// Get implements the TypeInstance interface.
func (i *DecimalInstance) Get() (Value, error) {
	beforeDecimal, err := rand.Bytes(i.precision - i.scale)
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	afterDecimal, err := rand.Bytes(i.scale)
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}

	strBytes := make([]byte, i.precision+1)
	strIdx := 0
	for idx := 0; idx < i.precision-i.scale; strIdx, idx = strIdx+1, idx+1 {
		strBytes[strIdx] = (beforeDecimal[idx] % 10) + 48
	}

	strBytes[strIdx] = '.'
	strIdx++
	for idx := 0; idx < i.scale; strIdx, idx = strIdx+1, idx+1 {
		strBytes[strIdx] = (afterDecimal[idx] % 10) + 48
	}
	return StringValue(strBytes), err
}

// Name implements the TypeInstance interface.
func (i *DecimalInstance) Name() string {
	return fmt.Sprintf("DECIMAL(%d,%d)", i.precision, i.scale)
}

// MaxValueCount implements the TypeInstance interface.
func (i *DecimalInstance) MaxValueCount() float64 {
	return 2 * math.Pow10(i.precision)
}
