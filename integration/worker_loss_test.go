package integration_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/worker"
)

type recoverySource struct{ calls [4]atomic.Int32 }

func (s *recoverySource) Open(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
	s.calls[p].Add(1)
	return (shuffleSource{}).Open(ctx, path, p, n)
}

func TestWorkerLossPreservesPublishedShuffleAndSurvivorCompletesJob(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Unix(1700000000, 0).UnixNano())
	tasks := scheduler.NewFIFOTaskScheduler(scheduler.WithClock(func() time.Time { return time.Unix(0, now.Load()) }))
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	client, err := worker.NewClient(ts.URL, worker.ClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.RegisterWorker(ctx, protocol.RegisterWorkerRequest{WorkerID: "lost", TotalSlots: 2}); err != nil {
		t.Fatal(err)
	}
	type completion struct {
		result protocol.JobResultResponse
		err    error
	}
	done := make(chan completion, 1)
	go func() {
		result, err := jobs.Submit(ctx, shuffleSpec("first", scheduler.ActionCollect))
		done <- completion{result, err}
	}()
	// Model a worker that publishes one map partition and then stops heartbeating
	// while holding a second assignment. The survivor is a real worker runtime.
	var assigned []protocol.TaskAssignment
	for len(assigned) == 0 {
		response, err := client.Heartbeat(ctx, protocol.HeartbeatRequest{WorkerID: "lost", FreeSlots: 2, RunningAttemptIDs: []plan.TaskAttemptID{}})
		if err != nil {
			t.Fatal(err)
		}
		assigned = response.Assignments
		if len(assigned) == 0 {
			runtime.Gosched()
		}
	}
	if len(assigned) != 2 {
		t.Fatal(assigned)
	}
	root := t.TempDir()
	store := shuffleStore(t, root)
	runner := executor.NewLocalRunner(registry, shuffleSource{}, 2, executor.WithShuffleStore(store))
	report := func(a protocol.TaskAssignment, output scheduler.TaskOutput) {
		t.Helper()
		ack, err := client.ReportSuccess(ctx, protocol.TaskSuccessRequest{JobID: a.JobID, StageID: a.StageID, Attempt: a.Attempt,
			PartitionID: a.Task.PartitionID, WorkerID: a.WorkerID, Output: protocol.TaskOutput{ShuffleOutput: output.ShuffleOutput}})
		if err != nil || !ack.Acknowledged {
			t.Fatalf("map report = %+v, %v", ack, err)
		}
	}
	accepted, err := runner.RunTask(ctx, assigned[0].Execution())
	if err != nil {
		t.Fatal(err)
	}
	report(assigned[0], accepted)
	now.Add(int64(10 * time.Second))
	if lost, err := tasks.ExpireWorkers(time.Unix(0, now.Load()), 10*time.Second); err != nil || !reflect.DeepEqual(lost, []plan.WorkerID{"lost"}) {
		t.Fatalf("expiry = %v, %v", lost, err)
	}
	source := &recoverySource{}
	w, err := worker.NewRuntime(client, worker.RuntimeConfig{WorkerID: "survivor", Slots: 1, HeartbeatInterval: time.Millisecond,
		RegisterFunctions: examplefuncs.Register, Sources: source, ShuffleStore: shuffleStore(t, root)})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	exited := make(chan error, 1)
	go func() { exited <- w.Run(workerCtx) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-exited:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("survivor stopped: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("survivor did not stop")
		}
	})
	select {
	case got := <-done:
		want := []string{`{"key":"beta","value":2}`, `{"key":"gamma","value":1}`, `{"key":"alpha","value":2}`}
		if got.err != nil {
			t.Fatal(got.err)
		}
		if len(got.result.Records) != len(want) {
			t.Fatalf("result = %+v", got.result)
		}
		// Compare by key, since partition hashing determines Collect order.
		seen := make(map[string]bool)
		for _, record := range got.result.Records {
			seen[string(record)] = true
		}
		for _, record := range want {
			if !seen[record] {
				t.Fatalf("missing %s in %+v", record, got.result)
			}
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for p := range 4 {
		want := int32(1)
		if p == 0 {
			want = 0
		}
		if got := source.calls[p].Load(); got != want {
			t.Fatalf("survivor read map %d %d times, want %d", p, got, want)
		}
	}
	// The old process can still write isolated files; its late report is ignored.
	late, err := runner.RunTask(ctx, assigned[1].Execution())
	if err != nil {
		t.Fatal(err)
	}
	report(assigned[1], late)
	if lost, _ := tasks.Worker("lost"); lost.Status != scheduler.WorkerLost || lost.ReservedSlots != 0 {
		t.Fatal(lost)
	}
	if survivor, _ := tasks.Worker("survivor"); survivor.ReservedSlots != 0 {
		t.Fatal(survivor)
	}
}
