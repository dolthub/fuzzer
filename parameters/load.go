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

package parameters

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"

	"github.com/dolthub/fuzzer/errors"

	"github.com/komkom/toml"
)

// LoadFromFile loads the config file from the given path.
func LoadFromFile(path string) (*Base, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return loadFromReader(file)
}

// LoadFromString loads the given string as a config file.
func LoadFromString(contents string) (*Base, error) {
	buffer := bytes.NewBufferString(contents)
	return loadFromReader(buffer)
}

// loadFromReader loads the config file from the given reader.
func loadFromReader(reader io.Reader) (*Base, error) {
	tomlReader := toml.New(reader)
	jsonBytes, err := ioutil.ReadAll(tomlReader)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	base := &configBase{}
	err = json.Unmarshal(jsonBytes, base)
	if err != nil {
		return nil, errors.Wrap(err)
	}
	return convertConfigBase(base)
}
