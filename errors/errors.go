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

package errors

import (
	"fmt"
	"io"
	"runtime"

	"github.com/pkg/errors"
)

// Error represents a wrappable error with a stack trace.
// Modified from gopkg.in/src-d/go-errors.v1
type Error struct {
	errStr     string
	nestedErr  error
	stackTrace errors.StackTrace
	ignored    bool
}

var _ error = Error{}
var _ fmt.Formatter = Error{}

// Error implements the interface error.
func (e Error) Error() string {
	if e.nestedErr == nil {
		return e.errStr
	} else if e.errStr == "" {
		return e.nestedErr.Error()
	} else {
		return fmt.Sprintf("%s\n%s", e.errStr, e.nestedErr.Error())
	}
}

// Format implements the fmt.Formatter error.
func (e Error) Format(s fmt.State, verb rune) {
	if nestedErr, ok := e.nestedErr.(Error); ok {
		nestedErr.Format(s, verb)
		return
	}

	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = io.WriteString(s, e.Error()+"\n")
			e.stackTrace.Format(s, verb)
			return
		}
		fallthrough
	case 's':
		_, _ = io.WriteString(s, e.Error())
	case 'q':
		_, _ = fmt.Fprintf(s, "%q", e.Error())
	}
}

// Ignorable sets the error to be ignored as far as a cycle is concerned. That is, if an ignorable error propagates all
// the way to the main function that controls the number of cycles that are to be run, then the cycle that returned this
// error will essentially be skipped from the perspective of metrics and the number of cycles to run. Duration-based
// limits will still apply, meaning that it's possible for zero cycles to report having run. If any nested error is
// ignorable, then all encapsulating errors are also ignorable.
func (e Error) Ignorable() Error {
	return Error{
		errStr:     e.errStr,
		nestedErr:  e.nestedErr,
		stackTrace: e.stackTrace,
		ignored:    true,
	}
}

// New returns a new Error with the given string as the error message.
func New(errStr string) Error {
	return Error{
		errStr:     errStr,
		stackTrace: stackTrace(1),
		ignored:    false,
	}
}

// Wrap wraps the given error, returning a new Error.
func Wrap(err error) Error {
	if _, ok := err.(Error); ok {
		return Error{
			nestedErr: err,
			ignored:   false,
		}
	}
	return Error{
		nestedErr:  err,
		stackTrace: stackTrace(1),
		ignored:    false,
	}
}

// ShouldIgnore returns whether the given error should be ignored.
func ShouldIgnore(err error) bool {
	fuzzerErr, ok := err.(Error)
	if !ok {
		return false
	}
	for true {
		if fuzzerErr.ignored {
			return true
		}
		if fuzzerErr, ok = fuzzerErr.nestedErr.(Error); !ok {
			break
		}
	}
	return false
}

// stackTrace returns the current stack trace, skipping the number of frames given.
func stackTrace(skip uint32) errors.StackTrace {
	var pcs [32]uintptr
	n := runtime.Callers(int(2+skip), pcs[:])
	st := make(errors.StackTrace, n)
	for i := 0; i < n; i++ {
		st[i] = errors.Frame(pcs[i])
	}
	return st
}
