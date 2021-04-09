package main

import (
	"fmt"
	"os"

	"github.com/dolthub/fuzzer/parameters"
	"github.com/dolthub/fuzzer/run"
)

func main() {
	//TODO: optionally load the config file if a path was provided
	base, err := parameters.LoadFromFile("./config.toml")
	exitOnErr(err)
	planner, err := run.NewPlanner(base)
	exitOnErr(err)
	//TODO: specify cycle count as argument
	for i := 0; i < 2; i++ {
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
