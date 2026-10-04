package integration_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/worker"
)

type retrySource struct {
	mode  string
	calls [4]atomic.Int32
}

func (s *retrySource) Open(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
	call := s.calls[p].Add(1)
	if p == 0 && (call == 1 || s.mode != "map and reduce fail once") {
		err := errors.New("injected source failure")
		if s.mode == "permanent" {
			err = scheduler.PermanentFailure(err)
		}
		return nil, err
	}
	return (shuffleSource{}).Open(ctx, path, p, n)
}

func TestWorkerShuffleTaskRetries(t *testing.T) {
	for _, mode := range []string{"map and reduce fail once", "exhausted", "permanent"} {
		t.Run(mode, func(t *testing.T) {
			tasks := scheduler.NewFIFOTaskScheduler()
			t.Cleanup(tasks.Close)
			var reduceCalls atomic.Int32
			register := func(r *executor.FunctionRegistry) error {
				if err := examplefuncs.Register(r); err != nil {
					return err
				}
				sum, err := r.Reduce("sum_int")
				if err != nil {
					return err
				}
				return r.RegisterReduce("retry_sum", func(a, b executor.Record) (executor.Record, error) {
					if reduceCalls.Add(1) == 1 {
						return nil, errors.New("injected reducer failure")
					}
					return sum(a, b)
				})
			}
			registry := executor.NewFunctionRegistry()
			if err := register(registry); err != nil {
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
			client, err := worker.NewClient(ts.URL, worker.ClientConfig{})
			if err != nil {
				t.Fatal(err)
			}
			source := &retrySource{mode: mode}
			store := shuffleStore(t, t.TempDir())
			w, err := worker.NewRuntime(client, worker.RuntimeConfig{WorkerID: "a", Slots: 2, HeartbeatInterval: time.Millisecond,
				RegisterFunctions: register, Sources: source, ShuffleStore: store})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			exited := make(chan error, 1)
			go func() { exited <- w.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-exited:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("worker stopped unexpectedly: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("worker did not stop")
				}
			})
			spec := shuffleSpec("first", scheduler.ActionCount)
			spec.Transformations[3].FunctionID = "retry_sum"
			result, err := jobs.Submit(ctx, spec)
			if mode == "map and reduce fail once" {
				if err != nil || result.Count != 3 {
					t.Fatalf("result = %+v, %v", result, err)
				}
				for p := range 4 {
					want := int32(1)
					if p == 0 {
						want = 2
					}
					if got := source.calls[p].Load(); got != want {
						t.Fatalf("partition %d executed %d times, want %d", p, got, want)
					}
				}
				if reduceCalls.Load() != 3 {
					t.Fatalf("reduce calls = %d, want one failed and two successful merges", reduceCalls.Load())
				}
			} else {
				if !errors.Is(err, coordinator.ErrJobFailed) || !strings.Contains(err.Error(), "injected source failure") {
					t.Fatalf("job failure = %v", err)
				}
				want := int32(scheduler.DefaultMaxTaskAttempts)
				if mode == "permanent" {
					want = 1
				}
				if got := source.calls[0].Load(); got != want {
					t.Fatalf("attempts = %d, want %d", got, want)
				}
				if reduceCalls.Load() != 0 {
					t.Fatal("reducers ran after failed map stage")
				}
			}
		})
	}
}
