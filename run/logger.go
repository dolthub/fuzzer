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
	"fmt"
	"os"

	"github.com/dolthub/fuzzer/errors"
)

type LogType byte

const (
	// LogType_CLI represents a CLI command
	LogType_CLI LogType = iota
	// LogType_INFO is used for general information
	LogType_INFO
	// LogType_SQLQ represents a SQL statement sent using `dolt sql -q "string"`
	LogType_SQLQ
	// LogType_SQLB represents a SQL statement sent through `dolt sql < x.sql`
	LogType_SQLB
	// LogType_SQLS represents a SQL statement sent through the server
	LogType_SQLS
	// LogType_WARN is used for warnings
	LogType_WARN
	// LogType_ERR is used for errors
	LogType_ERR
)

// Logger represents an interface to write to a log file.
type Logger interface {
	// WriteLine writes the given string to the underlying file. Newlines are automatically appended.
	WriteLine(LogType, string) error
	// Close closes any open files.
	Close() error
}

// fileLogger logs directly to the given file.
type fileLogger struct {
	file *os.File
}

var _ Logger = (*fileLogger)(nil)

// WriteLine implements the interface Logger.
func (l *fileLogger) WriteLine(lt LogType, s string) error {
	var bytesWritten int
	var err error
	switch lt {
	case LogType_CLI:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("CLI:  %s\n", s))
	case LogType_INFO:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("INFO: %s\n", s))
	case LogType_SQLQ:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("SQLQ: %s\n", s))
	case LogType_SQLB:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("SQLB: %s\n", s))
	case LogType_SQLS:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("SQLS: %s\n", s))
	case LogType_WARN:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("WARN: %s\n", s))
	case LogType_ERR:
		bytesWritten, err = l.file.WriteString(fmt.Sprintf("ERR:  %s\n", s))
	}
	if err != nil {
		return errors.Wrap(err)
	}
	if bytesWritten != len(s)+7 {
		return errors.New(fmt.Sprintf("wrote %d bytes, expected to write %d bytes", bytesWritten, len(s)+7))
	}
	return nil
}

// Close implements the interface Logger.
func (l *fileLogger) Close() error {
	return l.file.Close()
}

// fakeLogger discards all write statements. Used if a Logger is not specified.
type fakeLogger struct{}

var _ Logger = (*fakeLogger)(nil)

// WriteLine implements the interface Logger.
func (l *fakeLogger) WriteLine(LogType, string) error {
	return nil
}

// Close implements the interface Logger.
func (l *fakeLogger) Close() error {
	return nil
}
