package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/protocol"
)

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"--listen", ""}, {"--job-timeout", "0"}, {"--shutdown-timeout", "-1s"},
		{"--job-timeout", "bad"}, {"--unknown"}, {"unexpected"},
	} {
		if err := run(context.Background(), args, io.Discard); err == nil {
			t.Fatalf("accepted invalid flags: %v", args)
		}
	}
	var help bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &help); !errors.Is(err, flag.ErrHelp) || !strings.Contains(help.String(), "-listen") {
		t.Fatalf("help = %q, error = %v", help.String(), err)
	}
}

func TestRunReportsOccupiedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := run(context.Background(), []string{"--listen", listener.Addr().String()}, io.Discard); err == nil {
		t.Fatal("occupied listen address accepted")
	}
}

// Run the real main in a child process so signal wiring and exit status are tested.
func TestCoordinatorProcess(t *testing.T) {
	if os.Getenv("SPARKCORE_COORDINATOR_TEST_PROCESS") != "1" {
		return
	}
	os.Args = []string{"coordinator", "--listen", "127.0.0.1:0", "--shutdown-timeout", "2s"}
	main()
	os.Exit(0)
}

func TestCoordinatorInterruptCompletesBlockedSubmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCoordinatorProcess$")
	cmd.Env = append(os.Environ(), "SPARKCORE_COORDINATOR_TEST_PROCESS=1")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
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
	var started struct {
		Message string `json:"msg"`
		Address string `json:"address"`
	}
	decoder := json.NewDecoder(stderr)
	if err := decoder.Decode(&started); err != nil || started.Message != "coordinator_listening" || started.Address == "" {
		t.Fatalf("startup = %+v, %v", started, err)
	}
	url := "http://" + started.Address
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	// Register a worker manually and hold its task to establish an active job.
	resp, err := client.Post(url+protocol.RegisterWorkerPath, "application/json", strings.NewReader(`{"worker_id":"held","total_slots":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("register: %s", resp.Status)
	}
	resp.Body.Close()
	type submission struct {
		response *http.Response
		err      error
	}
	done := make(chan submission, 1)
	go func() {
		resp, err := client.Post(url+protocol.SubmitJobPath, "application/json", strings.NewReader(`{"source":{"path":"/missing/input.txt","num_partitions":4},"transformations":[{"kind":"map","function_id":"normalize"}],"action":"count"}`))
		done <- submission{resp, err}
	}()
	assigned := false
	for !assigned && ctx.Err() == nil {
		resp, err := client.Post(url+protocol.HeartbeatPath, "application/json", strings.NewReader(`{"worker_id":"held","free_slots":1,"running_attempt_ids":[]}`))
		if err != nil {
			t.Fatal(err)
		}
		heartbeat, err := protocol.DecodeAndValidate[protocol.HeartbeatResponse](resp.Body, 1<<20)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		assigned = len(heartbeat.Assignments) == 1
		if !assigned {
			runtime.Gosched()
		}
	}
	if !assigned {
		t.Fatal("coordinator never assigned the job")
	}
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.response.Body.Close()
	failure, err := protocol.DecodeAndValidate[protocol.ErrorResponse](result.response.Body, 1<<20)
	if err != nil || result.response.StatusCode != 503 || failure.Code != protocol.CodeUnavailable {
		t.Fatalf("shutdown response = %s, %+v, %v", result.response.Status, failure, err)
	}
	// Drain diagnostic logs before Wait closes the pipe.
	for decoder.More() {
		var event map[string]any
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
	}
	err = cmd.Wait()
	waited = true
	if err != nil || stdout.Len() != 0 {
		t.Fatalf("process exit = %v, stdout = %q", err, stdout.String())
	}
}
