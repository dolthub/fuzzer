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

package types

import (
	"fmt"
	"math"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/rand"
	"github.com/dolthub/fuzzer/ranges"
)

// Blob represents the BLOB MySQL type.
type Blob struct {
	Distribution ranges.Int
	Length       ranges.Int
}

var _ Type = (*Blob)(nil)

// GetOccurrenceRate implements the ranges.Distributable interface.
func (b *Blob) GetOccurrenceRate() (int64, error) {
	return b.Distribution.RandomValue()
}

// Instance implements the Type interface.
func (b *Blob) Instance() (TypeInstance, error) {
	return &BlobInstance{b.Length}, nil
}

// BlobInstance is the TypeInstance of Blob.
type BlobInstance struct {
	length ranges.Int
}

var _ TypeInstance = (*BlobInstance)(nil)

// Get implements the TypeInstance interface.
func (i *BlobInstance) Get() (Value, error) {
	n, err := i.length.RandomValue()
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	v, err := rand.String(int(n))
	if err != nil {
		return NilValue{}, errors.Wrap(err)
	}
	return BlobValue{StringValue(v)}, err
}

// TypeValue implements the TypeInstance interface.
func (i *BlobInstance) TypeValue() Value {
	return BlobValue{StringValue("")}
}

// Name implements the TypeInstance interface.
func (i *BlobInstance) Name(sqlite bool) string {
	if sqlite {
		return "LONGTEXT"
	}
	return "BLOB"
}

// MaxValueCount implements the TypeInstance interface.
func (i *BlobInstance) MaxValueCount() float64 {
	return math.Pow(float64(rand.StringCharSize()), float64(i.length.Upperbound))
}

// BlobValue is the Value type of a BlobInstance.
type BlobValue struct {
	StringValue
}

var _ Value = BlobValue{}

// Convert implements the Value interface.
func (v BlobValue) Convert(val interface{}) (Value, error) {
	switch val := val.(type) {
	case string:
		v.StringValue = StringValue(val)
	case []byte:
		v.StringValue = StringValue(val)
	default:
		return nil, errors.New(fmt.Sprintf("cannot convert %T to %T", val, v.Name()))
	}
	return v, nil
}

// Name implements the Value interface.
func (v BlobValue) Name() string {
	return "BLOB"
}

// MySQLString implements the Value interface.
func (v BlobValue) MySQLString() string {
	return v.String()
}

// SQLiteString implements the Value interface.
func (v BlobValue) SQLiteString() string {
	return v.String()
}

// CSVString implements the interface Value.
func (v BlobValue) CSVString() string {
	return v.StringTerminating(34)
}
