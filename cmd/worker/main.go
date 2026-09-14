package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("worker_failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	coordinatorURL := flags.String("coordinator", "http://127.0.0.1:8080", "coordinator HTTP origin")
	id := flags.String("id", "", "unique worker ID (required)")
	slots := flags.Int("slots", 2, "maximum concurrent task attempts")
	heartbeatInterval := flags.Duration("heartbeat-interval", 100*time.Millisecond, "interval between completed heartbeats")
	requestTimeout := flags.Duration("request-timeout", 10*time.Second, "timeout for each coordinator request")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *id == "" || *slots <= 0 || *heartbeatInterval <= 0 || *requestTimeout <= 0 {
		return fmt.Errorf("id is required; slots, heartbeat interval and request timeout must be positive")
	}
	client, err := worker.NewClient(*coordinatorURL, worker.ClientConfig{RequestTimeout: *requestTimeout})
	if err != nil {
		return err
	}
	// Each worker owns its function registry and partition executor.
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	runtime, err := worker.NewRuntime(client, worker.RuntimeConfig{
		WorkerID: plan.WorkerID(*id), Slots: *slots, HeartbeatInterval: *heartbeatInterval,
		RequestTimeout: *requestTimeout, RegisterFunctions: examplefuncs.Register, Logger: logger,
	})
	if err != nil {
		return err
	}
	logger.Info("worker_starting", "worker_id", *id, "slots", *slots, "coordinator", *coordinatorURL)
	err = runtime.Run(ctx)
	if err != nil && !(ctx.Err() != nil && errors.Is(err, ctx.Err())) {
		return fmt.Errorf("run worker %q: %w", *id, err)
	}
	logger.Info("worker_stopped", "worker_id", *id)
	return nil
}
