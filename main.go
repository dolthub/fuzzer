package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/dolthub/fuzzer/commands"
	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
)

func main() {
	args := os.Args[1:]
	var hookRegistrant run.HookRegistrant
	if len(args) > 0 {
		switch strings.ToLower(args[0]) {
		case "merge":
			hookRegistrant = &commands.Merge{}
		default:
			fmt.Printf("unknown command: %v", args[0])
			os.Exit(1)
		}
	}
	//TODO: optionally load the config file if a path was provided
	base, err := parameters.LoadFromFile("./config.toml")
	exitOnErr(err)
	planner, err := run.NewPlanner(base)
	exitOnErr(err)
	if hookRegistrant != nil {
		hookRegistrant.Register(planner.Hooks)
	}
	//TODO: specify cycle count as argument
	for i := 0; i < 1; i++ {
		cycle, err := planner.NewCycle()
		exitOnErr(err)
		err = cycle.Run()
		exitOnErr(err)
	}
}

func exitOnErr(err error) {
	if err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}
