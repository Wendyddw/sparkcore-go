package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	for _, args := range [][]string{
		nil, {"--id", "a", "--slots", "0"}, {"--id", "a", "--slots", "-1"},
		{"--id", "a", "--heartbeat-interval", "0"}, {"--id", "a", "--request-timeout", "-1s"},
		{"--id", "a", "--coordinator", "invalid"}, {"--unknown"}, {"--id", "a", "extra"},
	} {
		if err := run(context.Background(), args, io.Discard); err == nil {
			t.Fatalf("accepted invalid flags: %v", args)
		}
	}
	var help bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &help); !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "-id") {
		t.Fatalf("help = %q, error = %v", help.String(), err)
	}
}

func TestWorkerProcess(t *testing.T) {
	if os.Getenv("SPARKCORE_WORKER_TEST_PROCESS") != "1" {
		return
	}
	os.Args = []string{"worker", "--coordinator", os.Getenv("SPARKCORE_WORKER_TEST_URL"), "--id", "process-worker", "--slots", "2", "--heartbeat-interval", "1ms", "--request-timeout", "1s"}
	main()
	os.Exit(0)
}

func TestWorkerProcessExecutesAndStopsOnSignal(t *testing.T) {
	tasks := scheduler.NewFIFOTaskScheduler()
	defer tasks.Close()
	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		t.Fatal(err)
	}
	jobs, err := coordinator.NewJobService(registry, tasks)
	if err != nil {
		t.Fatal(err)
	}
	defer jobs.Close()
	server, err := coordinator.NewServer(tasks, jobs, coordinator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler)
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkerProcess$")
	cmd.Env = append(os.Environ(), "SPARKCORE_WORKER_TEST_PROCESS=1", "SPARKCORE_WORKER_TEST_URL="+ts.URL)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	path, err := filepath.Abs(filepath.Join("..", "..", "examples", "input.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// Only the child worker executes source access and the named functions.
	result, err := jobs.Submit(ctx, jobspec.Spec{
		Source:          jobspec.SourceSpec{Path: path, NumPartitions: 4},
		Transformations: []jobspec.TransformationSpec{{Kind: "map", FunctionID: "normalize"}, {Kind: "filter", FunctionID: "non_empty"}},
		Action:          scheduler.ActionCount,
	})
	if err != nil || result.Count != 5 {
		t.Fatalf("distributed result = %+v, %v", result, err)
	}
	snapshot, err := tasks.Worker("process-worker")
	if err != nil || snapshot.TotalSlots != 2 || snapshot.ReservedSlots != 0 {
		t.Fatalf("worker = %+v, %v", snapshot, err)
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err != nil || stdout.Len() != 0 {
		t.Fatalf("process exit = %v, stdout = %q, stderr = %s", err, stdout.String(), stderr.String())
	}
	decoder := json.NewDecoder(&stderr)
	stopped := false
	for decoder.More() {
		var event struct {
			Message string `json:"msg"`
			Worker  string `json:"worker_id"`
		}
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		stopped = stopped || (event.Message == "worker_stopped" && event.Worker == "process-worker")
	}
	if !stopped {
		t.Fatal("missing worker shutdown diagnostic")
	}
}
