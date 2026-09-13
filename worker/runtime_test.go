package worker_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/worker"
)

type runtimeClient struct {
	register  func(context.Context, protocol.RegisterWorkerRequest) (protocol.RegisterWorkerResponse, error)
	heartbeat func(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error)
	success   func(context.Context, protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error)
	failure   func(context.Context, protocol.TaskFailureRequest) (protocol.TaskReportResponse, error)
}

func (c runtimeClient) RegisterWorker(ctx context.Context, r protocol.RegisterWorkerRequest) (protocol.RegisterWorkerResponse, error) {
	if c.register != nil {
		return c.register(ctx, r)
	}
	return protocol.RegisterWorkerResponse{WorkerID: r.WorkerID, TotalSlots: r.TotalSlots}, nil
}
func (c runtimeClient) Heartbeat(ctx context.Context, r protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	if c.heartbeat != nil {
		return c.heartbeat(ctx, r)
	}
	return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
}
func (c runtimeClient) ReportSuccess(ctx context.Context, r protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
	if c.success != nil {
		return c.success(ctx, r)
	}
	return protocol.TaskReportResponse{Acknowledged: true}, nil
}
func (c runtimeClient) ReportFailure(ctx context.Context, r protocol.TaskFailureRequest) (protocol.TaskReportResponse, error) {
	if c.failure != nil {
		return c.failure(ctx, r)
	}
	return protocol.TaskReportResponse{Acknowledged: true}, nil
}

type sourceFunc func(context.Context, string, plan.PartitionID, int) (executor.Iterator, error)

func (f sourceFunc) Open(ctx context.Context, path string, partition plan.PartitionID, n int) (executor.Iterator, error) {
	return f(ctx, path, partition, n)
}

type iteratorFunc func(context.Context) (executor.Record, bool, error)

func (f iteratorFunc) Next(ctx context.Context) (executor.Record, bool, error) { return f(ctx) }

func recordsSource(records ...executor.Record) executor.SourceReader {
	return sourceFunc(func(context.Context, string, plan.PartitionID, int) (executor.Iterator, error) {
		i := 0
		return iteratorFunc(func(ctx context.Context) (executor.Record, bool, error) {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			if i == len(records) {
				return nil, false, nil
			}
			record := records[i]
			i++
			return record, true, nil
		}), nil
	})
}

func runtimeAssignment(partition int) protocol.TaskAssignment {
	a := assignment()
	a.Attempt.ID = plan.TaskAttemptID(10 + partition)
	a.Attempt.TaskID = plan.TaskID(20 + partition)
	a.Task.ID = a.Attempt.TaskID
	a.Task.PartitionID = plan.PartitionID(partition)
	a.Task.NumPartitions = 4
	a.Task.FinalAction.Kind = scheduler.ActionCount
	return a
}

func runtimeConfig(slots int) worker.RuntimeConfig {
	return worker.RuntimeConfig{WorkerID: "a", Slots: slots, HeartbeatInterval: time.Millisecond, RequestTimeout: time.Second, Sources: recordsSource("record")}
}

func newRuntime(t *testing.T, client worker.CoordinatorClient, config worker.RuntimeConfig) *worker.Runtime {
	t.Helper()
	w, err := worker.NewRuntime(client, config)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

type runtimeRun struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func startRuntime(t *testing.T, w *worker.Runtime) *runtimeRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runtimeRun{cancel: cancel, done: make(chan struct{})}
	go func() { r.err = w.Run(ctx); close(r.done) }()
	t.Cleanup(func() { cancel(); r.wait(t) })
	return r
}
func (r *runtimeRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not exit")
		return nil
	}
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker event")
		var zero T
		return zero
	}
}

func TestRuntimeRegistersOnceAndHoldsSlotsThroughAcknowledgment(t *testing.T) {
	started := make(chan plan.PartitionID, 2)
	releaseExecution := make(chan struct{})
	releaseReports := make(chan struct{})
	defer close(releaseExecution)
	defer close(releaseReports)
	offers := make(chan protocol.HeartbeatRequest, 128)
	reports := make(chan protocol.TaskSuccessRequest, 2)
	var registrations, opens atomic.Int32
	var offered bool
	client := runtimeClient{
		register: func(ctx context.Context, r protocol.RegisterWorkerRequest) (protocol.RegisterWorkerResponse, error) {
			registrations.Add(1)
			return protocol.RegisterWorkerResponse{WorkerID: r.WorkerID, TotalSlots: r.TotalSlots}, nil
		},
		heartbeat: func(ctx context.Context, r protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
			select {
			case offers <- r:
			default:
			}
			if !offered {
				offered = true
				return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{runtimeAssignment(1), runtimeAssignment(0)}}, nil
			}
			return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
		},
		success: func(ctx context.Context, r protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
			reports <- r
			select {
			case <-releaseReports:
				return protocol.TaskReportResponse{Acknowledged: true}, nil
			case <-ctx.Done():
				return protocol.TaskReportResponse{}, ctx.Err()
			}
		},
	}
	config := runtimeConfig(2)
	config.Sources = sourceFunc(func(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
		opens.Add(1)
		started <- p
		once := false
		return iteratorFunc(func(ctx context.Context) (executor.Record, bool, error) {
			if once {
				return nil, false, nil
			}
			once = true
			select {
			case <-releaseExecution:
				return "record", true, nil
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}), nil
	})
	w := newRuntime(t, client, config)
	if opens.Load() != 0 {
		t.Fatal("constructor opened source")
	}
	run := startRuntime(t, w)
	initial := receive(t, offers)
	if initial.FreeSlots != 2 || initial.RunningAttemptIDs == nil || len(initial.RunningAttemptIDs) != 0 {
		t.Fatalf("initial offer = %#v", initial)
	}
	p0, p1 := receive(t, started), receive(t, started)
	if p0 == p1 {
		t.Fatal("same partition started twice")
	}
	full := receive(t, offers)
	if full.FreeSlots != 0 || !reflect.DeepEqual(full.RunningAttemptIDs, []plan.TaskAttemptID{10, 11}) {
		t.Fatalf("running offer = %#v", full)
	}
	releaseExecution <- struct{}{}
	releaseExecution <- struct{}{}
	for range 2 {
		report := receive(t, reports)
		a := runtimeAssignment(int(report.PartitionID))
		if report.Attempt != a.Attempt || report.JobID != a.JobID || report.StageID != a.StageID || report.WorkerID != "a" || report.Output.Count != 1 || report.Output.Records != nil {
			t.Fatalf("success = %#v", report)
		}
	}
	// A later heartbeat must still reserve both slots while reports await acknowledgment.
	full = receive(t, offers)
	if full.FreeSlots != 0 || len(full.RunningAttemptIDs) != 2 {
		t.Fatalf("unacknowledged offer = %#v", full)
	}
	releaseReports <- struct{}{}
	releaseReports <- struct{}{}
	deadline := time.Now().Add(3 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("acknowledged slots were not advertised")
		}
		next := receive(t, offers)
		if next.FreeSlots == 2 {
			if len(next.RunningAttemptIDs) != 0 {
				t.Fatal("finished IDs retained")
			}
			break
		}
	}
	run.cancel()
	if err := run.wait(t); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if registrations.Load() != 1 || opens.Load() != 2 {
		t.Fatalf("registrations=%d executions=%d", registrations.Load(), opens.Load())
	}
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("runtime restarted")
	}
}

func TestRuntimeRejectsEntireInvalidAssignmentBatch(t *testing.T) {
	for _, name := range []string{"excess slots", "foreign worker", "invalid task", "duplicate attempt"} {
		t.Run(name, func(t *testing.T) {
			batch := []protocol.TaskAssignment{runtimeAssignment(0), runtimeAssignment(1)}
			switch name {
			case "excess slots":
				batch = append(batch, runtimeAssignment(2))
			case "foreign worker":
				batch[1].WorkerID = "b"
			case "invalid task":
				batch[1].Task.FinalAction = nil
			case "duplicate attempt":
				batch[1].Attempt.ID = batch[0].Attempt.ID
			}
			var opens atomic.Int32
			config := runtimeConfig(2)
			config.Sources = sourceFunc(func(context.Context, string, plan.PartitionID, int) (executor.Iterator, error) {
				opens.Add(1)
				return nil, fmt.Errorf("must not execute")
			})
			client := runtimeClient{heartbeat: func(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
				return protocol.HeartbeatResponse{Assignments: batch}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := newRuntime(t, client, config).Run(ctx)
			if !errors.Is(err, worker.ErrInvalidResponse) || opens.Load() != 0 {
				t.Fatalf("invalid batch: err=%v opens=%d", err, opens.Load())
			}
		})
	}
}

func TestRuntimeDoesNotExecuteCompletedAttemptAgain(t *testing.T) {
	var reports atomic.Int32
	client := runtimeClient{
		heartbeat: func(_ context.Context, r protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
			if r.FreeSlots == 0 {
				return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
			}
			return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{runtimeAssignment(0)}}, nil
		},
		success: func(context.Context, protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
			reports.Add(1)
			return protocol.TaskReportResponse{Acknowledged: true}, nil
		},
	}
	run := startRuntime(t, newRuntime(t, client, runtimeConfig(1)))
	if err := run.wait(t); !errors.Is(err, worker.ErrInvalidResponse) {
		t.Fatal(err)
	}
	if reports.Load() != 1 {
		t.Fatalf("reports=%d", reports.Load())
	}
}

func TestRuntimeReportsTaskErrorsWithoutRetryingOrStopping(t *testing.T) {
	for _, name := range []string{"unknown function", "source failure", "unencodable output"} {
		t.Run(name, func(t *testing.T) {
			a := runtimeAssignment(0)
			config := runtimeConfig(1)
			want := "source failed"
			switch name {
			case "unknown function":
				a.Task.Operations = append(a.Task.Operations, scheduler.StageOperation{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 1, Operator: plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "missing"}}})
				a.Task.FinalAction.TargetRDD = 1
				want = "not registered"
			case "source failure":
				config.Sources = sourceFunc(func(context.Context, string, plan.PartitionID, int) (executor.Iterator, error) {
					return nil, errors.New(want)
				})
			case "unencodable output":
				a.Task.FinalAction.Kind = scheduler.ActionCollect
				config.Sources = recordsSource(make(chan int))
				want = "encode record"
			}
			reports := make(chan protocol.TaskFailureRequest, 2)
			idle := make(chan struct{}, 1)
			var successes atomic.Int32
			sent := false
			client := runtimeClient{
				heartbeat: func(ctx context.Context, r protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
					if !sent {
						sent = true
						return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{a}}, nil
					}
					if r.FreeSlots == 1 {
						select {
						case idle <- struct{}{}:
						default:
						}
					}
					return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
				},
				failure: func(ctx context.Context, r protocol.TaskFailureRequest) (protocol.TaskReportResponse, error) {
					reports <- r
					return protocol.TaskReportResponse{Acknowledged: true}, nil
				},
				success: func(context.Context, protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
					successes.Add(1)
					return protocol.TaskReportResponse{Acknowledged: true}, nil
				},
			}
			run := startRuntime(t, newRuntime(t, client, config))
			report := receive(t, reports)
			if report.Attempt != a.Attempt || report.JobID != a.JobID || report.StageID != a.StageID || report.PartitionID != a.Task.PartitionID || report.WorkerID != a.WorkerID || !strings.Contains(report.Error, want) {
				t.Fatalf("failure=%#v", report)
			}
			receive(t, idle)
			run.cancel()
			run.wait(t)
			if len(reports) != 0 || successes.Load() != 0 {
				t.Fatal("task was retried or incorrectly succeeded")
			}
		})
	}
}

func TestRuntimeCancellationWaitsForBoundedTerminalReport(t *testing.T) {
	started := make(chan struct{})
	reported := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	config := runtimeConfig(1)
	config.Sources = sourceFunc(func(context.Context, string, plan.PartitionID, int) (executor.Iterator, error) {
		return iteratorFunc(func(ctx context.Context) (executor.Record, bool, error) {
			close(started)
			<-ctx.Done()
			return nil, false, ctx.Err()
		}), nil
	})
	sent := false
	client := runtimeClient{
		heartbeat: func(ctx context.Context, r protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
			if !sent {
				sent = true
				return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{runtimeAssignment(0)}}, nil
			}
			return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
		},
		failure: func(ctx context.Context, r protocol.TaskFailureRequest) (protocol.TaskReportResponse, error) {
			if ctx.Err() != nil || !strings.Contains(r.Error, "canceled") {
				t.Errorf("canceled task report: ctx=%v report=%#v", ctx.Err(), r)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Error("terminal report has no deadline")
			}
			close(reported)
			select {
			case <-release:
				return protocol.TaskReportResponse{Acknowledged: true}, nil
			case <-ctx.Done():
				return protocol.TaskReportResponse{}, ctx.Err()
			}
		},
	}
	run := startRuntime(t, newRuntime(t, client, config))
	receive(t, started)
	run.cancel()
	receive(t, reported)
	select {
	case <-run.done:
		t.Fatal("Run exited before terminal report finished")
	default:
	}
	release <- struct{}{}
	if err := run.wait(t); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestRuntimeCommunicationErrorsStopWithoutRetry(t *testing.T) {
	for _, where := range []string{"register", "heartbeat", "report", "acknowledgment", "report timeout"} {
		t.Run(where, func(t *testing.T) {
			want := errors.New("coordinator unavailable")
			var calls atomic.Int32
			client := runtimeClient{}
			config := runtimeConfig(1)
			switch where {
			case "register":
				client.register = func(context.Context, protocol.RegisterWorkerRequest) (protocol.RegisterWorkerResponse, error) {
					calls.Add(1)
					return protocol.RegisterWorkerResponse{}, want
				}
			case "heartbeat":
				client.heartbeat = func(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
					calls.Add(1)
					return protocol.HeartbeatResponse{}, want
				}
			default:
				sent := false
				client.heartbeat = func(context.Context, protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
					if !sent {
						sent = true
						return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{runtimeAssignment(0)}}, nil
					}
					return protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, nil
				}
				client.success = func(ctx context.Context, r protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
					calls.Add(1)
					if where == "acknowledgment" {
						return protocol.TaskReportResponse{}, nil
					}
					if where == "report timeout" {
						<-ctx.Done()
						return protocol.TaskReportResponse{}, ctx.Err()
					}
					return protocol.TaskReportResponse{}, want
				}
			}
			if where == "report timeout" {
				config.RequestTimeout = 20 * time.Millisecond
			}
			err := startRuntime(t, newRuntime(t, client, config)).wait(t)
			if where == "acknowledgment" {
				want = worker.ErrInvalidResponse
			} else if where == "report timeout" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) || calls.Load() != 1 {
				t.Fatalf("error=%v calls=%d", err, calls.Load())
			}
		})
	}
}

func TestRuntimeConstructorValidatesAndInitializesIndependentRegistries(t *testing.T) {
	var registries []*executor.FunctionRegistry
	config := runtimeConfig(1)
	config.RegisterFunctions = func(r *executor.FunctionRegistry) error {
		registries = append(registries, r)
		return r.RegisterMap("identity", func(r executor.Record) (executor.Record, error) { return r, nil })
	}
	newRuntime(t, runtimeClient{}, config)
	newRuntime(t, runtimeClient{}, config)
	if len(registries) != 2 || registries[0] == registries[1] || !registries[0].HasMap("identity") || !registries[1].HasMap("identity") {
		t.Fatal("registry not independently initialized")
	}
	if _, err := worker.NewRuntime(nil, config); err == nil {
		t.Fatal("nil client accepted")
	}
	for _, config := range []worker.RuntimeConfig{{Slots: 1}, {WorkerID: "a"}, {WorkerID: "a", Slots: 1, HeartbeatInterval: -1}, {WorkerID: "a", Slots: 1, RequestTimeout: -1}} {
		if _, err := worker.NewRuntime(runtimeClient{}, config); err == nil {
			t.Fatalf("invalid config accepted: %#v", config)
		}
	}
	want := errors.New("bad registration")
	config.RegisterFunctions = func(*executor.FunctionRegistry) error { return want }
	if _, err := worker.NewRuntime(runtimeClient{}, config); !errors.Is(err, want) {
		t.Fatal(err)
	}
}
