package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("submit", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	coordinatorURL := flags.String("coordinator", "http://127.0.0.1:8080", "coordinator HTTP origin")
	jobPath := flags.String("job", "", "path to a JSON job specification (required)")
	timeout := flags.Duration("request-timeout", 6*time.Minute, "timeout for submission and response reads")
	maxResponseBytes := flags.Int64("max-response-bytes", 1<<20, "maximum response size in bytes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *jobPath == "" || *timeout <= 0 || *maxResponseBytes <= 0 || *maxResponseBytes == 1<<63-1 {
		return fmt.Errorf("job is required; request timeout and response limit must be positive and bounded")
	}
	endpoint, err := submissionURL(*coordinatorURL)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Read only the specification; workers resolve and read source paths.
	file, err := os.Open(*jobPath)
	if err != nil {
		return fmt.Errorf("open job: %w", err)
	}
	spec, err := protocol.DecodeAndValidate[protocol.SubmitJobRequest](file, 1<<20)
	file.Close()
	if err != nil {
		return fmt.Errorf("read job: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	result, err := submitJob(ctx, endpoint, spec, *maxResponseBytes)
	if err != nil {
		return err
	}

	// Write only the validated final action result to stdout.
	if result.Action == scheduler.ActionCount {
		_, err = fmt.Fprintln(output, result.Count)
		return err
	}
	return json.NewEncoder(output).Encode(result.Records)
}
