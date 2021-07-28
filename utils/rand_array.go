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

package utils

import (
	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
)

// RandomArray returns an iterator that will return random indexes over the given length. Using these indexes, each
// element is visited only once in a random order.
type RandomArray interface {
	// NextIndex returns the next index to be used. If all indexes have been visited, returns false.
	NextIndex() (int64, bool)
}

// randArray is an implementation of RandomArray. This isn't truly random, but it looks random. Should probably
// write a true random implementation someday.
type randArray struct {
	idx int64
	max int64
	off int64
}

var _ RandomArray = (*randArray)(nil)

// NewRandomArray returns a new RandomArray.
func NewRandomArray(numOfElements int64) (RandomArray, error) {
	offset, err := rand.Uint64()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return &randArray{
		idx: 0,
		max: numOfElements,
		off: int64(offset>>1) % numOfElements,
	}, nil
}

// NextIndex implements the interface RandomArray.
func (r *randArray) NextIndex() (int64, bool) {
	if r.idx >= r.max {
		return 0, false
	}
	ret := ((r.idx * 471277) + r.off) % r.max
	r.idx++
	return ret, true
}
