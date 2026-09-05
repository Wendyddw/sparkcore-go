package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("explain", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	jobPath := flags.String("job", "", "path to a JSON job specification")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *jobPath == "" {
		return fmt.Errorf("-job is required")
	}

	file, err := os.Open(*jobPath)
	if err != nil {
		return fmt.Errorf("open job %q: %w", *jobPath, err)
	}
	defer file.Close()
	spec, err := jobspec.Decode(file)
	if err != nil {
		return err
	}

	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		return err
	}
	rdd, err := jobspec.Build(api.NewContext(registry, nil), spec)
	if err != nil {
		return err
	}
	lineage, err := rdd.ExplainLineage()
	if err != nil {
		return err
	}
	stagePlan, err := rdd.PlanStages(spec.Action, registry)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(output, lineage); err != nil {
		return err
	}
	_, err = io.WriteString(output, scheduler.ExplainStagePlan(stagePlan))
	return err
}
