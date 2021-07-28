// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ranges

import (
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
)

// Float represents a range between two float64 values.
type Float struct {
	Lowerbound float64
	Upperbound float64
}

// NewFloat converts a string slice into a Float. Does not verify that the array has only two values.
func NewFloat(r []float64) Float {
	return Float{
		Lowerbound: r[0],
		Upperbound: r[1],
	}
}

// NewFloatCollection converts a slice of float64 slices into a []Float. Does not verify that the collection is valid.
func NewFloatCollection(r [][]float64) []Float {
	coll := make([]Float, len(r))
	for i := range r {
		coll[i] = NewFloat(r[i])
	}
	return coll
}

// Median returns the median of the range.
func (r *Float) Median() float64 {
	return ((r.Upperbound - r.Lowerbound) / 2) + r.Lowerbound
}

// RandomValue returns a random value between the inclusive bounds of the range.
func (r *Float) RandomValue() (float64, error) {
	if r.Lowerbound == r.Upperbound {
		return r.Lowerbound, nil
	}
	v, err := rand.Float64()
	if err != nil {
		return 0, errors.Wrap(err)
	}
	v = math.Mod(math.Abs(v), r.Upperbound-r.Lowerbound)
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return r.Lowerbound, nil
	}
	return v + r.Lowerbound, nil
}
