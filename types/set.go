package types

import (
	"fmt"
	"math"
	"strings"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Set represents the SET MySQL type.
type Set struct {
	Distribution      ranges.Int
	ElementNameLength ranges.Int
	NumberOfElements  ranges.Int
}

var _ Type = (*Set)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (s *Set) GetOccurrenceRate() (int64, error) {
	return s.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (s *Set) Instance() (TypeInstance, error) {
	numOfElements, err := s.NumberOfElements.RandomValue()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	elements := make([]string, numOfElements)
	addedElements := make(map[string]struct{})
	for i := int64(0); i < numOfElements; {
		elemLength, err := s.ElementNameLength.RandomValue()
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
	return &SetInstance{elements}, nil
}

// SetInstance is the TypeInstance of Set.
type SetInstance struct {
	elements []string
}

var _ TypeInstance = (*SetInstance)(nil)

// Get implements the TypeInstance interface.
func (i *SetInstance) Get() (Value, error) {
	v, err := rand.Uint64()
	if len(i.elements) >= 64 {
		return Uint64Value(v), nil
	}
	return Uint64Value(v % (1 << len(i.elements))), err
}

// Name implements the TypeInstance interface.
func (i *SetInstance) Name() string {
	return fmt.Sprintf("SET('%s')", strings.Join(i.elements, "','"))
}

// MaxValueCount implements the TypeInstance interface.
func (i *SetInstance) MaxValueCount() float64 {
	return math.Pow(2, float64(len(i.elements)))
}
