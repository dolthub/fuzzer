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
	"fmt"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
)

// Distributable implementations are randomly picked from a collection of other Distributable implementations based
// on their occurrence rate.
type Distributable interface {
	// GetOccurrenceRate returns a number representing its occurrence rate.
	GetOccurrenceRate() (int64, error)
}

// DistributionCenter handles the distribution of Distributable objects. Employs a penalty system to maintain a roughly
// even distribution, where getting an object places a penalty against it, reducing its likelihood of being picked again.
// Over repeated calls, the penalties even out.
type DistributionCenter struct {
	penaltyDists []*penaltyDistributable
}

// penaltyDistributable is a Distributable that tracks its penalties.
type penaltyDistributable struct {
	Distributable
	Penalty     float64
	PenaltyMult float64
}

// NewDistributionCenter returns a *DistributionCenter, with each Distributable set according to its given occurrence
// rate.
func NewDistributionCenter(distributables ...Distributable) (*DistributionCenter, error) {
	var dists []Distributable
	var rates []float64

	totalRate := float64(0)
	for _, distributable := range distributables {
		rate, err := distributable.GetOccurrenceRate()
		if err != nil {
			return nil, errors.Wrap(err)
		}
		if rate <= 0 {
			continue
		}
		totalRate += float64(rate)
		dists = append(dists, distributable)
		rates = append(rates, float64(rate))
	}
	// The config should guarantee that no set of Distributables may result in an all zero result, but this is backup.
	if len(rates) == 0 {
		return nil, errors.New(fmt.Sprintf("all distributions returned 0: %v", distributables))
	}

	weightedDists := make([]*penaltyDistributable, len(dists))
	for i := 0; i < len(dists); i++ {
		// Each Distributable starts out with a penalty. Otherwise, every Distributable would essentially start out with
		// the same effective chance of being picked.
		weightedDists[i] = &penaltyDistributable{
			Distributable: dists[i],
			Penalty:       totalRate / rates[i],
			PenaltyMult:   totalRate / rates[i],
		}
	}
	return &DistributionCenter{
		penaltyDists: weightedDists,
	}, nil
}

// Get returns a random Distributable from the set. The weight affects the severity of the penalty given. For example,
// if a returned Distributable would be used for multiple operations, and you want a roughly even distribution relative
// to the operation count, then set the weight to the number of operations for this Distribution. A weight of zero means
// that no penalty will be given (not the general case). If weight is less than zero, it is set to zero.
func (d *DistributionCenter) Get(weight float64) (Distributable, error) {
	if weight <= 0 {
		weight = 0
	}
	for {
		idx, err := rand.Uint64()
		if err != nil {
			return nil, errors.Wrap(err)
		}
		distributable := d.penaltyDists[idx%uint64(len(d.penaltyDists))]
		distributable.Penalty--
		if distributable.Penalty <= 0 {
			distributable.Penalty += weight * distributable.PenaltyMult
			return distributable.Distributable, nil
		}
	}
}
