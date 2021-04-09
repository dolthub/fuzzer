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

// New returns a new Error with the given string as the error message.
func New(errStr string) Error {
	return Error{
		errStr:     errStr,
		stackTrace: stackTrace(1),
	}
}

// Wrap wraps the given error, returning a new Error.
func Wrap(err error) Error {
	if _, ok := err.(Error); ok {
		return Error{
			nestedErr: err,
		}
	}
	return Error{
		nestedErr:  err,
		stackTrace: stackTrace(1),
	}
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
