package ranges

import (
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/utils"
)

// Int represents a range between two int64 values.
type Int struct {
	Lowerbound int64
	Upperbound int64
}

// NewInt converts an int64 slice into an Int. Does not verify that the array has only two values.
func NewInt(r []int64) Int {
	return Int{
		Lowerbound: r[0],
		Upperbound: r[1],
	}
}

// NewIntCollection converts a slice of int64 slices into an []Int. Does not verify that the collection is valid.
func NewIntCollection(r [][]int64) []Int {
	coll := make([]Int, len(r))
	for i := range r {
		coll[i] = NewInt(r[i])
	}
	return coll
}

// RandomValue returns a random value between the inclusive bounds of the range.
func (r *Int) RandomValue() (int64, error) {
	if r.Lowerbound == r.Upperbound {
		return r.Lowerbound, nil
	}
	v, err := rand.Uint64()
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return int64(v%uint64(utils.AbsInt64(r.Upperbound-r.Lowerbound))) + r.Lowerbound, nil
}

// RandomValueRestrictUpper returns a random value between the inclusive bounds of the range. If the upper bound
// restriction is lower than the upper bound, then it is substituted. If the restriction is lower than the lower bound,
// then it is returned.
func (r *Int) RandomValueRestrictUpper(upperRestriction int64) (int64, error) {
	lowerbound := r.Lowerbound
	upperbound := r.Upperbound
	if upperRestriction <= lowerbound {
		return upperRestriction, nil
	} else if upperRestriction < upperbound {
		upperbound = upperRestriction
	} else if lowerbound == upperbound {
		return upperbound, nil
	}
	v, err := rand.Uint64()
	if err != nil {
		return 0, errors.Wrap(err)
	}
	return int64(v%uint64(utils.AbsInt64(upperbound-lowerbound))) + lowerbound, nil
}