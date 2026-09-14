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

// Observe actual HTTP exchanges; inject stale and duplicate reports at the boundary.
type checkedWorkerClient struct {
	*worker.Client
	mu                               sync.Mutex
	active                           map[plan.TaskAttemptID]bool
	assignments                      map[plan.TaskAttemptID]protocol.TaskAssignment
	peak, reports, stale, duplicates int
}

func (c *checkedWorkerClient) Heartbeat(ctx context.Context, r protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	response, err := c.Client.Heartbeat(ctx, r)
	if err != nil {
		return response, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(response.Assignments) > r.FreeSlots {
		return response, fmt.Errorf("assignment exceeds offered slots")
	}
	for _, a := range response.Assignments {
		if _, exists := c.assignments[a.Attempt.ID]; exists {
			return response, fmt.Errorf("attempt assigned twice")
		}
		c.assignments[a.Attempt.ID] = a
		c.active[a.Attempt.ID] = true
	}
	c.peak = max(c.peak, len(c.active))
	if c.peak > 2 {
		return response, fmt.Errorf("worker over capacity")
	}
	return response, nil
}

func (c *checkedWorkerClient) ReportSuccess(ctx context.Context, r protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
	stale := r
	stale.Attempt.StageAttemptID++
	_, err := c.Client.ReportSuccess(ctx, stale)
	var rejection *worker.HTTPError
	if !errors.As(err, &rejection) || rejection.StatusCode != 409 {
		return protocol.TaskReportResponse{}, fmt.Errorf("stale identity accepted: %v", err)
	}
	ack, err := c.Client.ReportSuccess(ctx, r)
	if err != nil {
		return ack, err
	}
	duplicate := r
	// A conflicting duplicate must not overwrite already accepted output.
	if duplicate.Output.Records == nil {
		duplicate.Output.Count = 999
	} else {
		duplicate.Output.Records = []json.RawMessage{json.RawMessage(`"wrong"`)}
	}
	if _, err := c.Client.ReportSuccess(ctx, duplicate); err != nil {
		return ack, err
	}
	c.mu.Lock()
	delete(c.active, r.Attempt.ID)
	c.reports++
	c.stale++
	c.duplicates++
	c.mu.Unlock()
	return ack, nil
}

type barrierSource struct {
	opened  chan<- plan.PartitionID
	release <-chan struct{}
	mu      sync.Mutex
	reads   map[plan.PartitionID]int
}

func (s *barrierSource) Open(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
	if path != "worker-only-input" || n != 4 {
		return nil, fmt.Errorf("source metadata changed")
	}
	s.mu.Lock()
	s.reads[p]++
	s.mu.Unlock()
	select {
	case s.opened <- p:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	input := [][]executor.Record{{" alpha ", " "}, {"beta", "gamma"}, {}, {"delta", "epsilon "}}
	return &partitionRecords{values: input[int(p)]}, nil
}

type partitionRecords struct{ values []executor.Record }

func (i *partitionRecords) Next(ctx context.Context) (executor.Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if len(i.values) == 0 {
		return nil, false, nil
	}
	value := i.values[0]
	i.values = i.values[1:]
	return value, true, nil
}

func TestDistributedNarrowExecution(t *testing.T) {
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
			opened, release := make(chan plan.PartitionID, 4), make(chan struct{})
			source := &barrierSource{opened: opened, release: release, reads: make(map[plan.PartitionID]int)}
			var clients []*checkedWorkerClient
			var stops []context.CancelFunc
			var exits []<-chan error
			for _, id := range []plan.WorkerID{"a", "b"} {
				client, err := worker.NewClient(ts.URL, worker.ClientConfig{})
				if err != nil {
					t.Fatal(err)
				}
				checked := &checkedWorkerClient{Client: client, active: make(map[plan.TaskAttemptID]bool), assignments: make(map[plan.TaskAttemptID]protocol.TaskAssignment)}
				clients = append(clients, checked)
				runtime, err := worker.NewRuntime(checked, worker.RuntimeConfig{WorkerID: id, Slots: 2, HeartbeatInterval: time.Millisecond, RegisterFunctions: examplefuncs.Register, Sources: source})
				if err != nil {
					t.Fatal(err)
				}
				workerCtx, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- runtime.Run(workerCtx) }()
				stops = append(stops, stop)
				exits = append(exits, done)
			}
			// Join every runtime before closing its HTTP and scheduler dependencies.
			joined := false
			join := func() {
				if joined {
					return
				}
				joined = true
				for _, stop := range stops {
					stop()
				}
				for _, exit := range exits {
					select {
					case err := <-exit:
						if !errors.Is(err, context.Canceled) {
							t.Errorf("worker exit: %v", err)
						}
					case <-time.After(3 * time.Second):
						t.Error("worker failed to stop")
					}
				}
			}
			t.Cleanup(join)
			body := fmt.Sprintf(`{"source":{"path":"worker-only-input","num_partitions":4},"transformations":[{"kind":"map","function_id":"normalize"},{"kind":"filter","function_id":"non_empty"}],"action":%q}`, action)
			req, err := http.NewRequestWithContext(ctx, "POST", ts.URL+protocol.SubmitJobPath, bytes.NewBufferString(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			type completion struct {
				result protocol.JobResultResponse
				err    error
			}
			completed := make(chan completion, 1)
			go func() {
				resp, err := ts.Client().Do(req)
				if err != nil {
					completed <- completion{err: err}
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					completed <- completion{err: fmt.Errorf("submit status: %s", resp.Status)}
					return
				}
				result, err := protocol.DecodeAndValidate[protocol.JobResultResponse](resp.Body, 1<<20)
				completed <- completion{result, err}
			}()
			// Four blocked tasks force both two-slot workers to participate.
			for range 4 {
				select {
				case <-opened:
				case <-ctx.Done():
					t.Fatal("four partitions never started")
				}
			}
			for _, c := range clients {
				c.mu.Lock()
				active, peak := len(c.active), c.peak
				c.mu.Unlock()
				if active != 2 || peak != 2 {
					t.Fatalf("barrier capacity: active=%d peak=%d", active, peak)
				}
			}
			select {
			case result := <-completed:
				t.Fatalf("job finished before source release: %+v", result)
			default:
			}
			close(release)
			var result protocol.JobResultResponse
			select {
			case got := <-completed:
				if got.err != nil {
					t.Fatal(got.err)
				}
				result = got.result
			case <-ctx.Done():
				t.Fatal("submission did not finish")
			}
			join()
			if action == scheduler.ActionCount {
				if result.Count != 5 || result.Records != nil {
					t.Fatalf("count = %+v", result)
				}
			} else {
				want := []json.RawMessage{json.RawMessage(`"alpha"`), json.RawMessage(`"beta"`), json.RawMessage(`"gamma"`), json.RawMessage(`"delta"`), json.RawMessage(`"epsilon"`)}
				if !reflect.DeepEqual(result.Records, want) || result.Count != 0 {
					t.Fatalf("collect = %s", result.Records)
				}
			}
			attempts := make(map[plan.TaskAttemptID]bool)
			partitions := make(map[plan.PartitionID]bool)
			for i, c := range clients {
				// Runtime exit joins reports, so these observations are now immutable.
				if len(c.active) != 0 || c.reports != 2 || c.stale != 2 || c.duplicates != 2 {
					t.Fatalf("incomplete reports: %+v", c)
				}
				for id, a := range c.assignments {
					if attempts[id] || partitions[a.Task.PartitionID] {
						t.Fatal("duplicate attempt or partition assignment")
					}
					attempts[id] = true
					partitions[a.Task.PartitionID] = true
				}
				id := []plan.WorkerID{"a", "b"}[i]
				snapshot, err := tasks.Worker(id)
				if err != nil || snapshot.ReservedSlots != 0 {
					t.Fatalf("leaked reservation: %+v %v", snapshot, err)
				}
			}
			if len(attempts) != 4 || len(partitions) != 4 {
				t.Fatal("missing logical partitions")
			}
			for p := plan.PartitionID(0); p < 4; p++ {
				if source.reads[p] != 1 {
					t.Fatalf("partition %d executed %d times", p, source.reads[p])
				}
			}
		})
	}
}
