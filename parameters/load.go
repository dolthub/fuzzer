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
