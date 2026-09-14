package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("coordinator_failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	addr := flags.String("listen", "127.0.0.1:8080", "HTTP listen address")
	jobTimeout := flags.Duration("job-timeout", 5*time.Minute, "maximum job execution time")
	shutdownTimeout := flags.Duration("shutdown-timeout", 10*time.Second, "maximum HTTP shutdown drain time")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *addr == "" || *jobTimeout <= 0 || *shutdownTimeout <= 0 {
		return fmt.Errorf("listen address must not be empty and timeouts must be positive")
	}
	if ctx.Err() != nil {
		return nil
	}

	// Share placement state between job scheduling and worker HTTP handlers.
	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		return err
	}
	tasks := scheduler.NewFIFOTaskScheduler()
	defer tasks.Close()
	jobs, err := coordinator.NewJobService(registry, tasks)
	if err != nil {
		return err
	}
	defer jobs.Close()
	server, err := coordinator.NewServer(tasks, jobs, coordinator.Config{Addr: *addr, JobTimeout: *jobTimeout})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	logger.Info("coordinator_listening", "address", listener.Addr().String())

	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case err := <-served:
		jobs.Close()
		_ = server.Close()
		return fmt.Errorf("serve coordinator: %w", err)
	case <-ctx.Done():
	}

	// Finish blocking submissions before draining HTTP and closing placement.
	logger.Info("coordinator_stopping")
	jobs.Close()
	drainCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(drainCtx); err != nil {
		_ = server.Close()
		<-served
		return fmt.Errorf("drain coordinator: %w", err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve coordinator: %w", err)
	}
	logger.Info("coordinator_stopped")
	return nil
}
