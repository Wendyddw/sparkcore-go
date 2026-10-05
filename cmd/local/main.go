package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output, diagnostics io.Writer) (runErr error) {
	flags := flag.NewFlagSet("local", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	shuffleRoot := flags.String("shuffle-root", "", "absolute shared shuffle directory (empty disables shuffle storage)")
	maxStageAttempts := flags.Int("max-stage-attempts", scheduler.DefaultMaxStageAttempts, "total executions per stage including the initial execution; 1 disables shuffle recovery")
	jobPath := flags.String("job", "", "path to a JSON job specification")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *maxStageAttempts <= 0 {
		return fmt.Errorf("max stage attempts must be positive")
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
	var runnerOptions []executor.RunnerOption
	if *shuffleRoot != "" {
		store, err := shuffle.NewFilesystem(*shuffleRoot)
		if err != nil {
			return fmt.Errorf("open shuffle store: %w", err)
		}
		defer func() { runErr = errors.Join(runErr, store.Close()) }()
		runnerOptions = append(runnerOptions, executor.WithShuffleStore(store))
	}
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	localRunner := executor.NewLocalRunner(registry, nil, spec.Source.NumPartitions, runnerOptions...)
	dagScheduler := scheduler.NewDAGScheduler(registry, executor.NewLocalTaskScheduler(localRunner), scheduler.WithMaxStageAttempts(*maxStageAttempts), scheduler.WithLogger(logger))
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
