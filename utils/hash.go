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
	"bytes"
	"crypto/sha512"
	"encoding/base32"
	"fmt"
)

const hashLen = 20

var hashEncoding = base32.NewEncoding("0123456789ABCDEFGHIJKLMNOPQRSTUV")

// Hash is a representation of a hash output by a Hasher. May be used directly in a map.
type Hash [hashLen]byte

// Hasher accumulates bytes and outputs a hash. This is not thread-safe.
type Hasher struct {
	buffer bytes.Buffer
}

// String returns the Hash as a Base32 string.
func (h Hash) String() string {
	return hashEncoding.EncodeToString(h[:])
}

// ToBytes returns the Hash as a byte slice.
func (h Hash) ToBytes() []byte {
	return h[:]
}

// NewHasher returns a new Hasher.
func NewHasher() *Hasher {
	return &Hasher{}
}

// Write writes the given byte slice to the Hasher.
func (h *Hasher) Write(data []byte) {
	n, err := h.buffer.Write(data)
	if err != nil {
		panic(err)
	}
	if n != len(data) {
		panic(fmt.Errorf("expected %d bytes written but wrote %d", len(data), n))
	}
}

// Hash returns the contents given to the Hasher as a Hash.
func (h *Hasher) Hash() Hash {
	hash := Hash{}
	sum := sha512.Sum512(h.buffer.Bytes())
	copy(hash[:], sum[:hashLen])
	return hash
}
