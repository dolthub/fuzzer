package types

import (
	"fmt"
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Enum represents the ENUM MySQL type.
type Enum struct {
	Distribution      ranges.Int
	ElementNameLength ranges.Int
	NumberOfElements  ranges.Int
}

var _ Type = (*Enum)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (e *Enum) GetOccurrenceRate() (int64, error) {
	return e.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (e *Enum) Instance() (TypeInstance, error) {
	numOfElements, err := e.NumberOfElements.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	elements := make([]string, numOfElements)
	addedElements := make(map[string]struct{})
	for i := int64(0); i < numOfElements; {
		elemLength, err := e.ElementNameLength.RandomValue()
		if err != nil {
			return nil, errors.Wrap(err)
		}
		elemName, err := rand.String(int(elemLength))
		if err != nil {
			return nil, errors.Wrap(err)
		}
		if _, ok := addedElements[elemName]; !ok {
			elements[i] = elemName
			addedElements[elemName] = struct{}{}
			i++
		}
	}
	return &EnumInstance{elements}, nil
}

// EnumInstance is the TypeInstance of Enum.
type EnumInstance struct {
	elements []string
}

var _ TypeInstance = (*EnumInstance)(nil)

// Get implements the TypeInstance interface.
func (i *EnumInstance) Get() (Value, error) {
	v, err := rand.Uint16()
	return Uint16Value(v%uint16(len(i.elements))) + 1, err
}

// Name implements the TypeInstance interface.
func (i *EnumInstance) Name() string {
	return fmt.Sprintf("ENUM('%s')", strings.Join(i.elements, "','"))
}

// MaxValueCount implements the TypeInstance interface.
func (i *EnumInstance) MaxValueCount() float64 {
	return float64(len(i.elements))
}
