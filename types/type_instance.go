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

// TypeInstance is the instance of a type that will be used for a cycle.
type TypeInstance interface {
	// Get returns a random value for this type within the constraints of this instance.
	Get() (Value, error)
	// Name returns the MySQL name of this TypeInstance, based on its parameters (if applicable). If sqlite is true, then
	// the returned string is not specific to MySQL, but used for sqlite (which may be a different type altogether).
	Name(sqlite bool) string
	// TypeValue returns the zero value of the ValuePrimitive that is returned from Get.
	TypeValue() Value
	// MaxValueCount returns the number of potential values that are valid for this type. The number returned is an
	// approximation, as a float64 does not have enough resolution to represent every value exactly.
	MaxValueCount() float64
}
