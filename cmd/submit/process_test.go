package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/worker"
)

func TestSubmitProcess(t *testing.T) {
	if os.Getenv("SPARKCORE_SUBMIT_TEST_PROCESS") != "1" {
		return
	}
	os.Args = []string{"submit", "--coordinator", os.Getenv("SPARKCORE_SUBMIT_TEST_URL"), "--job", os.Getenv("SPARKCORE_SUBMIT_TEST_JOB"), "--request-timeout", "5s"}
	main()
	os.Exit(0)
}

func submitProcess(ctx context.Context, url, job string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSubmitProcess$")
	cmd.Env = append(os.Environ(), "SPARKCORE_SUBMIT_TEST_PROCESS=1", "SPARKCORE_SUBMIT_TEST_URL="+url, "SPARKCORE_SUBMIT_TEST_JOB="+job)
	return cmd
}

func TestSubmitProcessRunsDistributedJobs(t *testing.T) {
	tasks := scheduler.NewFIFOTaskScheduler()
	t.Cleanup(tasks.Close)
	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		t.Fatal(err)
	}
	jobs, err := coordinator.NewJobService(registry, tasks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(jobs.Close)
	server, err := coordinator.NewServer(tasks, jobs, coordinator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.Handler)
	t.Cleanup(ts.Close)
	for _, id := range []plan.WorkerID{"a", "b"} {
		client, err := worker.NewClient(ts.URL, worker.ClientConfig{})
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := worker.NewRuntime(client, worker.RuntimeConfig{WorkerID: id, Slots: 2, HeartbeatInterval: time.Millisecond, RegisterFunctions: examplefuncs.Register})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runtime.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("worker %s: %v", id, err)
				}
			case <-time.After(3 * time.Second):
				t.Error("worker did not stop")
			}
		})
	}
	input, err := filepath.Abs(filepath.Join("..", "..", "examples", "input.txt"))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		action, path, want string
		fail               bool
	}{
		{"count", input, "5\n", false},
		{"collect", input, "[\"alpha\",\"beta\",\"alpha\",\"gamma\",\"beta\"]\n", false},
		{"count", input + ".missing", "", true},
	} {
		job := writeJob(t, fmt.Sprintf(`{"source":{"path":%q,"num_partitions":4},"transformations":[{"kind":"map","function_id":"normalize"},{"kind":"filter","function_id":"non_empty"}],"action":%q}`, test.path, test.action))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := submitProcess(ctx, ts.URL, job)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		cancel()
		if stdout.String() != test.want {
			t.Fatalf("stdout=%q, want %q; stderr=%q", stdout.String(), test.want, stderr.String())
		}
		if test.fail {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(stderr.String(), "HTTP 422 (job_failed)") {
				t.Fatalf("failure exit=%v stderr=%q", err, stderr.String())
			}
		} else if err != nil || stderr.Len() != 0 {
			t.Fatalf("success exit=%v stderr=%q", err, stderr.String())
		}
	}
}

func TestSubmitProcessInterruptCancelsHTTPRequest(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		<-r.Context().Done()
		close(canceled)
	}))
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := submitProcess(ctx, ts.URL, jobFile(t, "count"))
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("submitter did not send request")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	err := cmd.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "submit job:") {
		t.Fatalf("exit=%v stdout=%q stderr=%q", err, stdout.String(), stderr.String())
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("HTTP server did not observe cancellation")
	}
}
