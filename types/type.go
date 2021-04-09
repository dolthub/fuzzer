package types

import "github.com/dolthub/fuzzer/ranges"

type Type interface {
	ranges.Distributable
	// Instance returns an instance of this type, which can generate random values.
	Instance() (TypeInstance, error)
}
