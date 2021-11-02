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

package run

import (
	"strings"

	"github.com/dolthub/fuzzer/errors"
)

// Commit represents either a commit or the working set in dolt.
type Commit struct {
	Hash        string
	Parents     []*Commit
	Tables      []*Table
	ForeignKeys []*ForeignKey
}

// GetTable returns the table from this commit, or nil if it does not exist. Case-insensitive.
func (c *Commit) GetTable(tableName string) *Table {
	tableName = strings.ToLower(tableName)
	for _, table := range c.Tables {
		if strings.ToLower(table.Name) == tableName {
			return table
		}
	}
	return nil
}

// Copy returns a deep copy of the calling commit.
func (c *Commit) Copy() (*Commit, error) {
	var err error
	parents := make([]*Commit, len(c.Parents))
	copy(parents, c.Parents)
	tables := make([]*Table, len(c.Tables))
	for i := 0; i < len(c.Tables); i++ {
		tables[i], err = c.Tables[i].Copy()
		if err != nil {
			return &Commit{}, errors.Wrap(err)
		}
	}
	foreignKeys := make([]*ForeignKey, len(c.ForeignKeys))
	for i := 0; i < len(c.ForeignKeys); i++ {
		foreignKeys[i] = c.ForeignKeys[i].Copy()
	}
	return &Commit{
		Hash:        c.Hash,
		Parents:     parents,
		Tables:      tables,
		ForeignKeys: foreignKeys,
	}, nil
}
