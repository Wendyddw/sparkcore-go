package coordinator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
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

func liveJobServer(t *testing.T, tasks coordinator.TaskScheduler, jobs coordinator.JobSubmitter, config coordinator.Config) *httptest.Server {
	t.Helper()
	server, err := coordinator.NewServer(tasks, jobs, config)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(server.Handler)
	ts.Config = server
	ts.Start()
	t.Cleanup(ts.Close)
	return ts
}

type httpJobOutcome struct {
	response *http.Response
	err      error
}

func startHTTPJob(t *testing.T, ts *httptest.Server, ctx context.Context, spec protocol.SubmitJobRequest) <-chan httpJobOutcome {
	t.Helper()
	body, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, "POST", ts.URL+protocol.SubmitJobPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	done := make(chan httpJobOutcome, 1)
	go func() {
		resp, err := ts.Client().Do(req)
		done <- httpJobOutcome{resp, err}
	}()
	return done
}

func awaitHTTP[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(6 * time.Second):
		t.Fatal("HTTP operation did not complete")
		var zero T
		return zero
	}
}

func httpJobResponse[T protocol.Message](t *testing.T, done <-chan httpJobOutcome, status int) T {
	t.Helper()
	got := awaitHTTP(t, done)
	if got.err != nil {
		t.Fatal(got.err)
	}
	defer got.response.Body.Close()
	if got.response.StatusCode != status || got.response.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("HTTP response = %s, headers = %v", got.response.Status, got.response.Header)
	}
	result, err := protocol.DecodeAndValidate[T](got.response.Body, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestSubmitHTTPAllowsJobsLongerThanWriteTimeout(t *testing.T) {
	tasks, _ := newService(t, coordinator.Config{})
	jobs := submitFunc(func(ctx context.Context, spec protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
		select {
		case <-time.After(150 * time.Millisecond):
			return protocol.JobResultResponse{Action: spec.Action, Count: 7}, nil
		case <-ctx.Done():
			return protocol.JobResultResponse{}, ctx.Err()
		}
	})
	ts := liveJobServer(t, tasks, jobs, coordinator.Config{WriteTimeout: 50 * time.Millisecond, JobTimeout: time.Second})
	got := httpJobResponse[protocol.JobResultResponse](t, startHTTPJob(t, ts, context.Background(), narrowJob(t, scheduler.ActionCount, 1)), 200)
	if got.Count != 7 {
		t.Fatalf("result = %+v", got)
	}
}

func TestSubmitHTTPCancellationStopsScheduling(t *testing.T) {
	for _, reason := range []string{"disconnect", "deadline", "service close"} {
		t.Run(reason, func(t *testing.T) {
			jobs, tasks, _ := newJobService(t)
			finished := make(chan error, 1)
			submit := submitFunc(func(ctx context.Context, spec protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
				result, err := jobs.Submit(ctx, spec)
				finished <- err
				return result, err
			})
			ts := liveJobServer(t, tasks, submit, coordinator.Config{JobTimeout: 500 * time.Millisecond, WriteTimeout: 100 * time.Millisecond})
			// Ensure blocked jobs cannot keep server cleanup waiting after a test failure.
			t.Cleanup(jobs.Close)
			register(t, ts.Config.Handler, "a", 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := startHTTPJob(t, ts, ctx, narrowJob(t, scheduler.ActionCount, 4))
			assigned := awaitAssignments(t, ts.Config.Handler, "a", 2)
			switch reason {
			case "disconnect":
				cancel()
				if got := awaitHTTP(t, done); !errors.Is(got.err, context.Canceled) {
					if got.response != nil {
						got.response.Body.Close()
					}
					t.Fatalf("canceled HTTP request = %v", got.err)
				}
			case "deadline":
				got := httpJobResponse[protocol.ErrorResponse](t, done, 504)
				if got.Code != protocol.CodeJobFailed {
					t.Fatalf("deadline error = %+v", got)
				}
			case "service close":
				jobs.Close()
				got := httpJobResponse[protocol.ErrorResponse](t, done, 503)
				if got.Code != protocol.CodeUnavailable {
					t.Fatalf("closed service error = %+v", got)
				}
			}
			want := context.Canceled
			if reason == "deadline" {
				want = context.DeadlineExceeded
			} else if reason == "service close" {
				want = scheduler.ErrSchedulerClosed
			}
			if err := awaitHTTP(t, finished); !errors.Is(err, want) {
				t.Fatalf("job stopped with %v, want %v", err, want)
			}
			for _, a := range assigned {
				acknowledge(t, ts.Config.Handler, successReport(a, protocol.TaskOutput{Count: 999}))
			}
			reserved(t, tasks.FIFOTaskScheduler, 0)
			if more := offer(t, ts.Config.Handler, "a", 2); len(more) != 0 {
				t.Fatalf("canceled job assigned pending tasks: %+v", more)
			}
		})
	}
}

func TestSubmitHTTPRejectsUnsupportedPlansBeforeAssignment(t *testing.T) {
	jobs, tasks, _ := newJobService(t)
	ts := liveJobServer(t, tasks, jobs, coordinator.Config{})
	t.Cleanup(jobs.Close)
	for _, name := range []string{"unknown function", "shuffle"} {
		spec := narrowJob(t, scheduler.ActionCount, 4)
		if name == "unknown function" {
			spec.Transformations[0].FunctionID = "missing"
		} else {
			spec.Transformations[0].Kind = plan.OpMapToPair
			spec.Transformations[0].FunctionID = "word_pair"
			spec.Transformations[1].Kind = plan.OpReduceByKey
			spec.Transformations[1].FunctionID = "sum_int"
			spec.Transformations[1].NumPartitions = 2
		}
		got := httpJobResponse[protocol.ErrorResponse](t, startHTTPJob(t, ts, context.Background(), spec), 422)
		if got.Code != protocol.CodeJobFailed {
			t.Fatalf("%s: %+v", name, got)
		}
	}
	select {
	case set := <-tasks.sets:
		t.Fatalf("rejected plan scheduled tasks: %+v", set)
	default:
	}
}

// Gate each worker's first source read so both workers must hold a partition.
type gatedTextSource struct {
	id     plan.WorkerID
	opened chan<- plan.WorkerID
	gate   <-chan struct{}
}

func (s gatedTextSource) Open(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
	s.opened <- s.id
	select {
	case <-s.gate:
		return (executor.TextSourceReader{}).Open(ctx, path, p, n)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestSubmitHTTPRunsJobsThroughTwoWorkers(t *testing.T) {
	for _, action := range []scheduler.ActionKind{scheduler.ActionCount, scheduler.ActionCollect} {
		t.Run(string(action), func(t *testing.T) {
			jobs, tasks, _ := newJobService(t)
			ts := liveJobServer(t, tasks, jobs, coordinator.Config{})
			t.Cleanup(jobs.Close)
			spec := narrowJob(t, action, 4)
			if err := os.WriteFile(spec.Source.Path, []byte(" alpha \n \n beta\ngamma \ndelta\nepsilon \n"), 0600); err != nil {
				t.Fatal(err)
			}
			opened := make(chan plan.WorkerID, 16)
			gate := make(chan struct{})
			var release sync.Once
			unblock := func() { release.Do(func() { close(gate) }) }
			t.Cleanup(unblock)
			for _, id := range []plan.WorkerID{"a", "b"} {
				client, err := worker.NewClient(ts.URL, worker.ClientConfig{RequestTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				runtime, err := worker.NewRuntime(client, worker.RuntimeConfig{WorkerID: id, Slots: 1,
					HeartbeatInterval: time.Millisecond, RequestTimeout: time.Second, RegisterFunctions: examplefuncs.Register,
					Sources: gatedTextSource{id, opened, gate}})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				stopped := make(chan error, 1)
				go func() { stopped <- runtime.Run(ctx) }()
				t.Cleanup(func() {
					cancel()
					if err := awaitHTTP(t, stopped); !errors.Is(err, context.Canceled) {
						t.Errorf("worker %s: %v", id, err)
					}
				})
			}
			done := startHTTPJob(t, ts, context.Background(), spec)
			first, second := awaitHTTP(t, opened), awaitHTTP(t, opened)
			if first == second {
				t.Fatal("expected both one-slot workers to execute")
			}
			unblock()
			got := httpJobResponse[protocol.JobResultResponse](t, done, 200)
			if action == scheduler.ActionCount {
				if got.Count != 5 || got.Records != nil {
					t.Fatalf("count = %+v", got)
				}
			} else {
				want := []json.RawMessage{json.RawMessage(`"alpha"`), json.RawMessage(`"beta"`), json.RawMessage(`"gamma"`), json.RawMessage(`"delta"`), json.RawMessage(`"epsilon"`)}
				if !reflect.DeepEqual(got.Records, want) || got.Count != 0 {
					t.Fatalf("collect = %s, want %s", got.Records, want)
				}
			}
			// Workers remain available after a job, including after a source failure.
			spec.Source.Path += ".missing"
			failure := httpJobResponse[protocol.ErrorResponse](t, startHTTPJob(t, ts, context.Background(), spec), 422)
			if failure.Code != protocol.CodeJobFailed {
				t.Fatalf("worker failure = %+v", failure)
			}
			spec.Source.Path = spec.Source.Path[:len(spec.Source.Path)-len(".missing")]
			if err := os.WriteFile(spec.Source.Path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			empty := httpJobResponse[protocol.JobResultResponse](t, startHTTPJob(t, ts, context.Background(), spec), 200)
			if empty.Count != 0 || len(empty.Records) != 0 || (action == scheduler.ActionCollect && empty.Records == nil) {
				t.Fatalf("empty job = %+v", empty)
			}
		})
	}
}
