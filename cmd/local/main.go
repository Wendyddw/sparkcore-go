package main

import (
	"context"
	"encoding/json"
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
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("local", flag.ContinueOnError)
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
	localRunner := executor.NewLocalRunner(registry, nil, spec.Source.NumPartitions)
	dagScheduler := scheduler.NewDAGScheduler(registry, executor.NewLocalTaskScheduler(localRunner))
	defer dagScheduler.Close()
	actionRunner := executor.NewSchedulerActionRunner(dagScheduler)
	apiContext := api.NewContext(registry, actionRunner)
	rdd, err := jobspec.Build(apiContext, spec)
	if err != nil {
		return err
	}

	switch spec.Action {
	case scheduler.ActionCount:
		count, err := rdd.Count(ctx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(output, count)
		return err
	case scheduler.ActionCollect:
		records, err := rdd.Collect(ctx)
		if err != nil {
			return err
		}
		return json.NewEncoder(output).Encode(records)
	default:
		return fmt.Errorf("unsupported action %q", spec.Action)
	}
}
