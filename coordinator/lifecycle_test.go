package coordinator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"reflect"
	"sync"
	"testing"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

type lifecycleLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lifecycleLog) Write(data []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(data)
}

func (l *lifecycleLog) events(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	data := bytes.Clone(l.buf.Bytes())
	l.mu.Unlock()
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var events []map[string]any
	for decoder.More() {
		var event map[string]any
		if err := decoder.Decode(&event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

func loggedJobService(t *testing.T) (*coordinator.JobService, http.Handler, *lifecycleLog) {
	t.Helper()
	logs := &lifecycleLog{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	tasks := scheduler.NewFIFOTaskScheduler(scheduler.WithLogger(logger))
	t.Cleanup(tasks.Close)
	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		t.Fatal(err)
	}
	jobs, err := coordinator.NewJobService(registry, tasks, scheduler.WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(jobs.Close)
	server, err := coordinator.NewServer(tasks, jobs, coordinator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return jobs, server.Handler, logs
}

func TestLifecycleLogsTrackAcceptedTransitions(t *testing.T) {
	jobs, h, logs := loggedJobService(t)
	register(t, h, "a", 2)
	register(t, h, "b", 2)
	done := startJob(t, jobs, context.Background(), narrowJob(t, scheduler.ActionCount, 4))
	assigned := awaitAssignments(t, h, "a", 2)
	assigned = append(assigned, awaitAssignments(t, h, "b", 2)...)
	for _, p := range []int{2, 0, 3} {
		r := successReport(assigned[p], protocol.TaskOutput{Count: 1})
		acknowledge(t, h, r)
		acknowledge(t, h, r)
	}
	for _, event := range logs.events(t) {
		if event["msg"] == "stage_succeeded" || event["msg"] == "job_succeeded" {
			t.Fatal("completion logged before every partition succeeded")
		}
	}
	acknowledge(t, h, successReport(assigned[1], protocol.TaskOutput{Count: 2}))
	if got := waitJob(t, done); got.err != nil || got.result.Count != 5 {
		t.Fatalf("result = %+v", got)
	}
	counts := make(map[string]int)
	assignmentEvents := make(map[json.Number]map[string]any)
	for _, event := range logs.events(t) {
		name := event["msg"].(string)
		counts[name]++
		if event["job_id"] != json.Number("0") {
			t.Fatalf("missing zero job ID: %v", event)
		}
		switch name {
		case "task_assigned", "task_succeeded":
			for _, key := range []string{"stage_id", "stage_attempt_id", "task_id", "attempt_id", "partition_id", "worker_id"} {
				if _, ok := event[key]; !ok {
					t.Fatalf("%s missing %s", name, key)
				}
			}
			id := event["attempt_id"].(json.Number)
			if name == "task_assigned" {
				assignmentEvents[id] = event
			} else {
				assigned := assignmentEvents[id]
				for _, key := range []string{"stage_id", "stage_attempt_id", "task_id", "partition_id", "worker_id"} {
					if assigned[key] != event[key] {
						t.Fatalf("report identity differs: %v, %v", assigned, event)
					}
				}
			}
		}
	}
	want := map[string]int{"job_submitted": 1, "stage_started": 1, "task_assigned": 4, "task_succeeded": 4, "stage_succeeded": 1, "job_succeeded": 1}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("events = %v, want %v", counts, want)
	}
}

func TestLifecycleLogsDoNotReviveFailedJobs(t *testing.T) {
	for _, reason := range []string{"worker failure", "cancellation", "invalid plan"} {
		t.Run(reason, func(t *testing.T) {
			jobs, h, logs := loggedJobService(t)
			register(t, h, "a", 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			spec := narrowJob(t, scheduler.ActionCount, 4)
			if reason == "invalid plan" {
				spec.Transformations[0].FunctionID = "missing"
			}
			done := startJob(t, jobs, ctx, spec)
			var assigned []protocol.TaskAssignment
			if reason != "invalid plan" {
				assigned = awaitAssignments(t, h, "a", 2)
				if reason == "worker failure" {
					r := failureReport(successReport(assigned[0], protocol.TaskOutput{}))
					acknowledge(t, h, r)
					acknowledge(t, h, r)
				} else {
					cancel()
				}
			}
			if got := waitJob(t, done); got.err == nil {
				t.Fatal("failed job succeeded")
			}
			for _, a := range assigned {
				acknowledge(t, h, successReport(a, protocol.TaskOutput{Count: 99}))
			}
			jobs.Close()
			counts := make(map[string]int)
			for _, event := range logs.events(t) {
				counts[event["msg"].(string)]++
			}
			if counts["job_submitted"] != 1 || counts["job_failed"] != 1 || counts["task_succeeded"] != 0 || counts["stage_succeeded"] != 0 || counts["job_succeeded"] != 0 {
				t.Fatalf("failed-job events = %v", counts)
			}
			if reason == "worker failure" && counts["task_failed"] != 1 {
				t.Fatalf("failure report logged %d times", counts["task_failed"])
			}
			if reason == "invalid plan" && (counts["task_assigned"] != 0 || counts["stage_started"] != 0) {
				t.Fatalf("invalid plan started work: %v", counts)
			}
		})
	}
}
