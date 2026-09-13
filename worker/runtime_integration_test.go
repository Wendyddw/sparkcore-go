package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"sync"
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

func TestTwoRuntimesExecuteNarrowPartitionsThroughCoordinator(t *testing.T) {
	for _, action := range []scheduler.ActionKind{scheduler.ActionCount, scheduler.ActionCollect} {
		t.Run(string(action), func(t *testing.T) {
			tasks := scheduler.NewFIFOTaskScheduler()
			t.Cleanup(tasks.Close)
			server, err := coordinator.NewServer(tasks, coordinator.Config{})
			if err != nil {
				t.Fatal(err)
			}
			ts := httptest.NewServer(server.Handler)
			t.Cleanup(ts.Close)
			coordinatorRegistry := executor.NewFunctionRegistry()
			if err := examplefuncs.Register(coordinatorRegistry); err != nil {
				t.Fatal(err)
			}
			input := [][]executor.Record{{" alpha ", " "}, {" beta", "gamma "}, {}, {"delta", "epsilon "}}
			wantRecords := [][]string{{`"alpha"`}, {`"beta"`, `"gamma"`}, {}, {`"delta"`, `"epsilon"`}}
			opened := make(chan plan.WorkerID, 8)
			gate := make(chan struct{})
			var release sync.Once
			unblock := func() { release.Do(func() { close(gate) }) }
			defer unblock()
			var opens atomic.Int32
			var registries []*executor.FunctionRegistry
			var runtimes []*worker.Runtime
			for _, id := range []plan.WorkerID{"a", "b"} {
				config := runtimeConfig(1)
				config.WorkerID = id
				config.RegisterFunctions = func(r *executor.FunctionRegistry) error {
					registries = append(registries, r)
					return examplefuncs.Register(r)
				}
				config.Sources = sourceFunc(func(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
					opens.Add(1)
					opened <- id
					select {
					case <-gate:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return recordsSource(input[int(p)]...).Open(ctx, path, p, n)
				})
				runtimes = append(runtimes, newRuntime(t, newClient(t, ts.URL, worker.ClientConfig{}), config))
			}
			if opens.Load() != 0 {
				t.Fatal("worker startup opened a source")
			}
			if registries[0] == registries[1] || registries[0] == coordinatorRegistry || registries[1] == coordinatorRegistry {
				t.Fatal("registries share state")
			}
			for _, r := range append(registries, coordinatorRegistry) {
				if !r.HasMap("normalize") || !r.HasFilter("non_empty") {
					t.Fatal("missing independently registered function")
				}
			}
			set := scheduler.TaskSet{JobID: 7, StageID: 3, StageAttemptID: 2}
			for p := range 4 {
				a := runtimeAssignment(p)
				task := a.Task
				task.Operations = append(task.Operations,
					scheduler.StageOperation{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 1, Operator: plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "normalize"}}},
					scheduler.StageOperation{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 2, Operator: plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "non_empty"}}})
				task.FinalAction = &scheduler.ActionSpec{Kind: action, TargetRDD: 2}
				set.Tasks = append(set.Tasks, task)
			}
			observer := clientObserver{make(chan scheduler.TaskAttemptSuccess, 4), make(chan scheduler.TaskAttemptFailure, 4)}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tasks.ScheduleTaskSet(ctx, set, observer) }()
			var runs []*runtimeRun
			for _, w := range runtimes {
				runs = append(runs, startRuntime(t, w))
			}
			first, second := receive(t, opened), receive(t, opened)
			if first == second {
				t.Fatal("one-slot worker started two tasks before the other worker")
			}
			unblock()
			if err := receive(t, done); err != nil {
				t.Fatal(err)
			}
			if len(observer.successes) != 4 || len(observer.failures) != 0 || opens.Load() != 4 {
				t.Fatalf("success=%d failure=%d opens=%d", len(observer.successes), len(observer.failures), opens.Load())
			}
			var count int64
			seen := make(map[plan.PartitionID]bool)
			for range 4 {
				r := <-observer.successes
				if seen[r.PartitionID] {
					t.Fatal("partition completed twice")
				}
				seen[r.PartitionID] = true
				if action == scheduler.ActionCount {
					count += r.Output.Count
					if r.Output.Records != nil {
						t.Fatal("Count returned records")
					}
				} else {
					want := wantRecords[int(r.PartitionID)]
					if len(r.Output.Records) != len(want) || r.Output.Count != 0 {
						t.Fatalf("partition output=%#v", r.Output)
					}
					for i, record := range r.Output.Records {
						raw, ok := record.(json.RawMessage)
						if !ok || string(raw) != want[i] {
							t.Fatalf("record=%#v want=%s", record, want[i])
						}
					}
				}
			}
			if action == scheduler.ActionCount && count != 5 {
				t.Fatalf("partition counts sum=%d", count)
			}
			for _, run := range runs {
				run.cancel()
				if err := run.wait(t); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			for _, id := range []plan.WorkerID{"a", "b"} {
				snapshot, err := tasks.Worker(id)
				if err != nil || snapshot.ReservedSlots != 0 {
					t.Fatalf("worker %s still reserved: %#v %v", id, snapshot, err)
				}
			}
		})
	}
}

func TestRuntimeCollectEncodesEmptyAndPreciseRecords(t *testing.T) {
	for _, test := range []struct {
		name    string
		records []executor.Record
		want    []json.RawMessage
	}{
		{"empty", nil, []json.RawMessage{}},
		{"values", []executor.Record{int64(9007199254740993), executor.KeyValue{Key: "key", Value: int64(9223372036854775807)}, nil}, []json.RawMessage{json.RawMessage(`9007199254740993`), json.RawMessage(`{"key":"key","value":9223372036854775807}`), json.RawMessage(`null`)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			reports := make(chan protocol.TaskSuccessRequest, 1)
			sent := false
			client := runtimeClient{
				heartbeat: func(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
					if sent {
						return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
					}
					sent = true
					a := runtimeAssignment(0)
					a.Task.FinalAction.Kind = scheduler.ActionCollect
					return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{a}}, nil
				},
				success: func(ctx context.Context, r protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
					reports <- r
					return protocol.TaskReportResponse{Acknowledged: true}, nil
				},
			}
			config := runtimeConfig(1)
			config.Sources = recordsSource(test.records...)
			run := startRuntime(t, newRuntime(t, client, config))
			report := receive(t, reports)
			if report.Output.Count != 0 || !reflect.DeepEqual(report.Output.Records, test.want) {
				t.Fatalf("Collect output=%#v", report.Output)
			}
			run.cancel()
			run.wait(t)
		})
	}
}
