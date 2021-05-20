package run

import (
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/dolthub/fuzzer/errors"
	"github.com/dolthub/fuzzer/parameters"
)

// Planner is the entry point that commands may use to hook into the various points of a cycle. It also creates each
// cycle.
type Planner struct {
	Hooks            *Hooks
	Base             *parameters.Base
	workingDirectory string
	lastRunStartTime time.Time
}

// NewPlanner returns a new *Planner from the given parameters.Base.
func NewPlanner(base *parameters.Base) (*Planner, error) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err)
	}
	workingDirectory = filepath.ToSlash(workingDirectory)
	hooks := &Hooks{}
	(&BlueprintManager{}).Register(hooks)
	(&RepositoryManager{}).Register(hooks)
	if base.Options.ManualGC {
		(&GCManager{}).Register(hooks)
	}
	return &Planner{
		Hooks:            hooks,
		Base:             base,
		workingDirectory: workingDirectory,
		lastRunStartTime: time.Unix(0, 0),
	}, nil
}

// NewCycle returns a new *Cycle created from this Planner.
func (p *Planner) NewCycle() (*Cycle, error) {
	// We force GC before each cycle as we put a lot of pressure on the GC each run.
	runtime.GC()
	return newCycle(p)
}
