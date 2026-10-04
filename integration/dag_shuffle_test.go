package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
	"github.com/Wendyddw/sparkcore-go/worker"
)

func shuffleSpec(path string, action scheduler.ActionKind) jobspec.Spec {
	return jobspec.Spec{Source: jobspec.SourceSpec{Path: path, NumPartitions: 4}, Action: action, Transformations: []jobspec.TransformationSpec{
		{Kind: plan.OpMap, FunctionID: "normalize"}, {Kind: plan.OpFilter, FunctionID: "non_empty"},
		{Kind: plan.OpMapToPair, FunctionID: "word_pair"}, {Kind: plan.OpReduceByKey, FunctionID: "sum_int", NumPartitions: 2},
	}}
}

type shuffleSource struct {
	id          plan.WorkerID
	opened      chan<- plan.WorkerID
	start, held <-chan struct{}
}

func (s shuffleSource) Open(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
	if s.opened != nil {
		select {
		case s.opened <- s.id:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-s.start:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if p == 3 {
			select {
			case <-s.held:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	input := [][]executor.Record{{" alpha ", " "}, {"beta", "alpha"}, {}, {"gamma", "beta"}}
	if path == "second" {
		input = [][]executor.Record{{"other"}, {}, {"other"}, {"key"}}
	}
	return &partitionRecords{values: input[int(p)]}, nil
}

func shuffleStore(t *testing.T, root string) *shuffle.Filesystem {
	t.Helper()
	store, err := shuffle.NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func TestLocalDAGConcurrentShuffleJobs(t *testing.T) {
	registry := executor.NewFunctionRegistry()
	if err := examplefuncs.Register(registry); err != nil {
		t.Fatal(err)
	}
	runner := executor.NewLocalRunner(registry, shuffleSource{}, 4, executor.WithShuffleStore(shuffleStore(t, t.TempDir())))
	dag := scheduler.NewDAGScheduler(registry, executor.NewLocalTaskScheduler(runner))
	defer dag.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make(chan error, 2)
	for _, path := range []string{"first", "second"} {
		go func() {
			driver := api.NewContext(registry, executor.NewSchedulerActionRunner(dag))
			rdd, err := jobspec.Build(driver, shuffleSpec(path, scheduler.ActionCollect))
			if err != nil {
				errs <- err
				return
			}
			records, err := rdd.Collect(ctx)
			if err != nil {
				errs <- err
				return
			}
			got := make(map[string]int64)
			for _, record := range records {
				pair := record.(executor.KeyValue)
				switch n := pair.Value.(type) {
				case int64:
					got[pair.Key] = n
				case json.Number:
					got[pair.Key], err = n.Int64()
				default:
					err = fmt.Errorf("unexpected value type %T", n)
				}
				if err != nil {
					errs <- err
					return
				}
			}
			want := map[string]int64{"alpha": 2, "beta": 2, "gamma": 1}
			if path == "second" {
				want = map[string]int64{"other": 2, "key": 1}
			}
			if !reflect.DeepEqual(got, want) {
				errs <- fmt.Errorf("%s result %v, want %v", path, got, want)
				return
			}
			count, err := rdd.Count(ctx)
			if err == nil && count != int64(len(want)) {
				err = fmt.Errorf("%s Count=%d, want %d", path, count, len(want))
			}
			errs <- err
		}()
	}
	for range 2 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

type shuffleHTTPClient struct {
	*worker.Client
	gateOpen    *atomic.Bool
	mapReports  chan<- struct{}
	mu          sync.Mutex
	assignments []protocol.TaskAssignment
}

func (c *shuffleHTTPClient) Heartbeat(ctx context.Context, request protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	response, err := c.Client.Heartbeat(ctx, request)
	if err != nil {
		return response, err
	}
	for _, a := range response.Assignments {
		if a.Task.StageKind == scheduler.StageResult && !c.gateOpen.Load() {
			return response, fmt.Errorf("reduce assigned before held map completed")
		}
	}
	c.mu.Lock()
	c.assignments = append(c.assignments, response.Assignments...)
	c.mu.Unlock()
	return response, nil
}
func (c *shuffleHTTPClient) ReportSuccess(ctx context.Context, request protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
	ack, err := c.Client.ReportSuccess(ctx, request)
	if err != nil {
		return ack, err
	}
	// Duplicates cross the real transport and cannot advance the stage barrier twice.
	if _, err := c.Client.ReportSuccess(ctx, request); err != nil {
		return ack, err
	}
	if request.Output.ShuffleOutput != nil {
		select {
		case c.mapReports <- struct{}{}:
		case <-ctx.Done():
			return ack, ctx.Err()
		}
	}
	return ack, nil
}

func TestSubmittedShuffleJobThroughTwoWorkers(t *testing.T) {
	for _, action := range []scheduler.ActionKind{scheduler.ActionCount, scheduler.ActionCollect} {
		t.Run(string(action), func(t *testing.T) {
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
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			root := t.TempDir()
			opened := make(chan plan.WorkerID, 4)
			start, held := make(chan struct{}), make(chan struct{})
			var gateOpen atomic.Bool
			mapReports := make(chan struct{}, 4)
			var clients []*shuffleHTTPClient
			for _, id := range []plan.WorkerID{"a", "b"} {
				client, err := worker.NewClient(ts.URL, worker.ClientConfig{})
				if err != nil {
					t.Fatal(err)
				}
				checked := &shuffleHTTPClient{Client: client, gateOpen: &gateOpen, mapReports: mapReports}
				clients = append(clients, checked)
				store := shuffleStore(t, root)
				runtime, err := worker.NewRuntime(checked, worker.RuntimeConfig{WorkerID: id, Slots: 2, HeartbeatInterval: time.Millisecond,
					RegisterFunctions: examplefuncs.Register, ShuffleStore: store, Sources: shuffleSource{id: id, opened: opened, start: start, held: held}})
				if err != nil {
					t.Fatal(err)
				}
				workerCtx, stop := context.WithCancel(ctx)
				exited := make(chan error, 1)
				go func() { exited <- runtime.Run(workerCtx) }()
				t.Cleanup(func() {
					stop()
					select {
					case err := <-exited:
						if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
							t.Errorf("worker %s: %v", id, err)
						}
					case <-time.After(3 * time.Second):
						t.Errorf("worker %s failed to stop", id)
					}
				})
			}
			body, err := json.Marshal(shuffleSpec("first", action))
			if err != nil {
				t.Fatal(err)
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+protocol.SubmitJobPath, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			type response struct {
				result protocol.JobResultResponse
				err    error
			}
			completed := make(chan response, 1)
			go func() {
				r, err := ts.Client().Do(request)
				if err != nil {
					completed <- response{err: err}
					return
				}
				defer r.Body.Close()
				if r.StatusCode != http.StatusOK {
					completed <- response{err: fmt.Errorf("job HTTP status %s", r.Status)}
					return
				}
				result, err := protocol.DecodeAndValidate[protocol.JobResultResponse](r.Body, 1<<20)
				completed <- response{result, err}
			}()
			workers := make(map[plan.WorkerID]int)
			for range 4 {
				select {
				case id := <-opened:
					workers[id]++
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if workers["a"] != 2 || workers["b"] != 2 {
				t.Fatalf("map assignments bypassed worker capacity: %v", workers)
			}
			close(start)
			for range 3 {
				select {
				case <-mapReports:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case r := <-completed:
				t.Fatalf("job finished before held map: %+v", r)
			default:
			}
			gateOpen.Store(true)
			close(held)
			var outcome response
			select {
			case outcome = <-completed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if outcome.err != nil {
				t.Fatal(outcome.err)
			}
			if action == scheduler.ActionCount {
				if outcome.result.Count != 3 {
					t.Fatalf("Count=%d, want 3 keys", outcome.result.Count)
				}
			} else {
				var got []string
				for _, raw := range outcome.result.Records {
					var pair struct {
						Key   string
						Value int64
					}
					if err := json.Unmarshal(raw, &pair); err != nil {
						t.Fatal(err)
					}
					got = append(got, fmt.Sprintf("%s:%d", pair.Key, pair.Value))
				}
				var want []string
				for p := range 2 {
					for _, key := range []string{"alpha", "beta", "gamma"} {
						bucket, _ := shuffle.PartitionFor(key, 2)
						if bucket == plan.PartitionID(p) {
							count := 2
							if key == "gamma" {
								count = 1
							}
							want = append(want, fmt.Sprintf("%s:%d", key, count))
						}
					}
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("Collect=%v, want partition/key order %v", got, want)
				}
			}
			maps, reduces := 0, 0
			for _, client := range clients {
				client.mu.Lock()
				for _, a := range client.assignments {
					if a.Task.StageKind == scheduler.StageShuffleMap {
						maps++
					} else {
						reduces++
						if a.ShuffleInputs == nil || len(a.ShuffleInputs.Outputs) != 4 || a.ShuffleInputs.JobID != a.JobID {
							t.Error("incomplete or foreign reduce inputs")
						}
					}
				}
				client.mu.Unlock()
			}
			if maps != 4 || reduces != 2 {
				t.Fatalf("assignments=%d maps, %d reduces", maps, reduces)
			}
		})
	}
}
