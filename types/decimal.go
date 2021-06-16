package types

import (
	"fmt"
	"math"
	"strings"
	"unsafe"

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
	scale, err := d.Scale.RandomValueRestrictUpper(utils.MinInt64(precision, 30))
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

	var strBytes []byte
	strIdx := 0
	if i.precision-i.scale > 0 {
		strBytes = make([]byte, i.precision+1)
		for idx := 0; idx < i.precision-i.scale; strIdx, idx = strIdx+1, idx+1 {
			strBytes[strIdx] = (beforeDecimal[idx] % 10) + 48
		}
	} else {
		strBytes = make([]byte, i.precision+2)
		strBytes[0] = '0'
		strIdx = 1
	}

	if i.scale > 0 {
		strBytes[strIdx] = '.'
		strIdx++
		for idx := 0; idx < i.scale; strIdx, idx = strIdx+1, idx+1 {
			strBytes[strIdx] = (afterDecimal[idx] % 10) + 48
		}
	} else {
		strBytes = strBytes[:i.precision]
	}
	return DecimalValue{StringValue(strBytes), i.valuePrecision(), i.scale}, err
}

// TypeValue implements the TypeInstance interface.
func (i *DecimalInstance) TypeValue() Value {
	return DecimalValue{StringValue(""), i.valuePrecision(), i.scale}
}

// Name implements the TypeInstance interface.
func (i *DecimalInstance) Name(sqlite bool) string {
	if sqlite {
		return "VARCHAR(67)"
	}
	return fmt.Sprintf("DECIMAL(%d,%d)", i.precision, i.scale)
}

// MaxValueCount implements the TypeInstance interface.
func (i *DecimalInstance) MaxValueCount() float64 {
	return 2 * math.Pow10(i.precision)
}

// valuePrecision returns the precision to be used for DecimalValue. DecimalValue assumes that precision - scale will
// have a minimum value of one (to represent the zero value on display), so we return a modified precision in the event
// that precision - scale would equal 0.
func (i *DecimalInstance) valuePrecision() int {
	if i.precision-i.scale == 0 {
		return i.precision + 1
	}
	return i.precision
}

// DecimalValue is the Value type of a DecimalInstance.
type DecimalValue struct {
	StringValue
	precision int
	scale     int
}

var _ Value = DecimalValue{}

// Convert implements the Value interface.
func (v DecimalValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case string:
		val = strings.TrimLeft(val, "0")
		if len(val) == 0 {
			if v.scale > 0 {
				strBytes := make([]byte, v.scale+2)
				for idx := 0; idx < len(strBytes); idx += 1 {
					strBytes[idx] = '0'
				}
				strBytes[1] = '.'
				val = *(*string)(unsafe.Pointer(&strBytes))
			} else {
				val = "0"
			}
		}
		if val[0] == '.' {
			val = "0" + val
		}
		v.StringValue = StringValue(val)
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v DecimalValue) Name() string {
	return "DECIMAL"
}

// MySQLString implements the Value interface.
func (v DecimalValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v DecimalValue) SQLiteString() string {
	decimalIdx := strings.IndexRune(string(v.StringValue), '.')
	var zerosToAdd int
	if decimalIdx == -1 {
		zerosToAdd = v.precision - len(v.StringValue)
	} else {
		zerosToAdd = v.precision - v.scale - decimalIdx
	}
	if zerosToAdd == 0 {
		return v.String()
	}
	strBytes := make([]byte, len(v.StringValue)+zerosToAdd)
	copy(strBytes[zerosToAdd:], v.StringValue)
	for i := 0; i < zerosToAdd; i++ {
		strBytes[i] = '0'
	}
	return StringValue(*(*string)(unsafe.Pointer(&strBytes))).String()
}
