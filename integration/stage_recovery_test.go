package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
	"github.com/Wendyddw/sparkcore-go/worker"
)

// Damage a real published bucket only when an accepted snapshot reaches its
// reducer. All injection stays in this test store; production has no fault flag.
type damagedShuffleStore struct {
	shuffle.Store
	root, mode string
	repeat     bool
	mu         sync.Mutex
	damaged    map[plan.StageAttemptID]bool
	writes     []shuffle.AttemptIdentity
}

func (s *damagedShuffleStore) Begin(ctx context.Context, a shuffle.AttemptIdentity, maps, reduces int) (shuffle.AttemptWriter, error) {
	s.mu.Lock()
	s.writes = append(s.writes, a)
	s.mu.Unlock()
	return s.Store.Begin(ctx, a, maps, reduces)
}

func (s *damagedShuffleStore) OpenBucket(ctx context.Context, output shuffle.MapOutput, p plan.PartitionID) (shuffle.BucketReader, error) {
	s.mu.Lock()
	var err error
	a := output.Attempt
	if a.MapPartitionID == 0 && p == 0 && !s.damaged[a.StageAttemptID] && (s.repeat || len(s.damaged) == 0) {
		s.damaged[a.StageAttemptID] = true
		path := filepath.Join(s.root, a.RunID, fmt.Sprintf("job-%d", a.JobID), fmt.Sprintf("shuffle-%d", a.ShuffleID),
			fmt.Sprintf("stage-%d", a.StageID), fmt.Sprintf("stage-attempt-%d", a.StageAttemptID), fmt.Sprintf("map-%d", a.MapPartitionID),
			fmt.Sprintf("task-attempt-%d", a.TaskAttemptID), fmt.Sprintf("bucket-%d.jsonl", p))
		if s.mode == "missing" {
			err = os.Remove(path)
		} else {
			err = os.WriteFile(path, []byte("corrupt\n"), 0600)
		}
	}
	s.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("inject shuffle damage: %w", err)
	}
	return s.Store.OpenBucket(ctx, output, p)
}

func (s *damagedShuffleStore) verifyAttempts(t *testing.T, want int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	stages := make(map[plan.StageAttemptID][]shuffle.AttemptIdentity)
	physical := make(map[plan.TaskAttemptID]bool)
	for _, a := range s.writes {
		if physical[a.TaskAttemptID] {
			t.Fatal("physical attempt reused a publication path")
		}
		physical[a.TaskAttemptID] = true
		stages[a.StageAttemptID] = append(stages[a.StageAttemptID], a)
	}
	if len(stages) != want {
		t.Fatalf("map stage executions = %d, want %d", len(stages), want)
	}
	var original []shuffle.AttemptIdentity
	for _, attempts := range stages {
		if len(attempts) != 4 {
			t.Fatalf("map stage ran %d partitions, want 4", len(attempts))
		}
		sort.Slice(attempts, func(i, j int) bool { return attempts[i].MapPartitionID < attempts[j].MapPartitionID })
		if original == nil {
			original = attempts
			continue
		}
		for p, a := range attempts {
			b := original[p]
			if a.RunID != b.RunID || a.JobID != b.JobID || a.ShuffleID != b.ShuffleID || a.StageID != b.StageID || a.TaskID != b.TaskID || a.MapPartitionID != b.MapPartitionID {
				t.Fatal("recovery changed logical identity")
			}
		}
	}
}

func TestShuffleStageRecoveryThroughLocalAndHTTPExecution(t *testing.T) {
	for _, remote := range []bool{false, true} {
		for _, scenario := range []string{"missing", "corrupt", "repeated loss", "disabled"} {
			t.Run(fmt.Sprintf("remote=%t/%s", remote, scenario), func(t *testing.T) {
				registry := executor.NewFunctionRegistry()
				if err := examplefuncs.Register(registry); err != nil {
					t.Fatal(err)
				}
				root := t.TempDir()
				store := &damagedShuffleStore{Store: shuffleStore(t, root), root: root, mode: "missing", repeat: scenario == "repeated loss", damaged: make(map[plan.StageAttemptID]bool)}
				if scenario == "corrupt" {
					store.mode = "corrupt"
				}
				limit := scheduler.DefaultMaxStageAttempts
				if scenario == "disabled" {
					limit = 1
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(cancel)
				var count int64
				var records []json.RawMessage
				action := scheduler.ActionCount
				if scenario == "corrupt" {
					action = scheduler.ActionCollect
				}
				var err error
				if !remote {
					runner := executor.NewLocalRunner(registry, shuffleSource{}, 4, executor.WithShuffleStore(store))
					dag := scheduler.NewDAGScheduler(registry, executor.NewLocalTaskScheduler(runner), scheduler.WithMaxStageAttempts(limit))
					t.Cleanup(dag.Close)
					rdd, buildErr := jobspec.Build(api.NewContext(registry, executor.NewSchedulerActionRunner(dag)), shuffleSpec("first", action))
					if buildErr != nil {
						t.Fatal(buildErr)
					}
					if action == scheduler.ActionCount {
						count, err = rdd.Count(ctx)
					} else {
						var values []executor.Record
						values, err = rdd.Collect(ctx)
						for _, value := range values {
							data, encodeErr := json.Marshal(value)
							if encodeErr != nil {
								t.Fatal(encodeErr)
							}
							records = append(records, data)
						}
					}
					dag.Close() // Join canceled old attempts before inspecting store history.
				} else {
					tasks := scheduler.NewFIFOTaskScheduler(scheduler.WithMaxTaskAttempts(1))
					t.Cleanup(tasks.Close)
					jobs, createErr := coordinator.NewJobService(registry, tasks, scheduler.WithMaxStageAttempts(limit))
					if createErr != nil {
						t.Fatal(createErr)
					}
					t.Cleanup(jobs.Close)
					server, createErr := coordinator.NewServer(tasks, jobs, coordinator.Config{})
					if createErr != nil {
						t.Fatal(createErr)
					}
					ts := httptest.NewServer(server.Handler)
					t.Cleanup(ts.Close)
					client, createErr := worker.NewClient(ts.URL, worker.ClientConfig{})
					if createErr != nil {
						t.Fatal(createErr)
					}
					w, createErr := worker.NewRuntime(client, worker.RuntimeConfig{WorkerID: "a", Slots: 2, HeartbeatInterval: time.Millisecond,
						RegisterFunctions: examplefuncs.Register, Sources: shuffleSource{}, ShuffleStore: store})
					if createErr != nil {
						t.Fatal(createErr)
					}
					workerCtx, stop := context.WithCancel(ctx)
					exited := make(chan error, 1)
					go func() { exited <- w.Run(workerCtx) }()
					t.Cleanup(func() {
						stop()
						select {
						case err := <-exited:
							if !errors.Is(err, context.Canceled) {
								t.Errorf("worker: %v", err)
							}
						case <-time.After(3 * time.Second):
							t.Error("worker did not stop")
						}
					})
					result, submitErr := jobs.Submit(ctx, shuffleSpec("first", action))
					count, err = result.Count, submitErr
					records = result.Records
				}
				if scenario == "repeated loss" || scenario == "disabled" {
					if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("exhausted its %d stage attempts", limit)) {
						t.Fatalf("unbounded/wrong failure: %v", err)
					}
				} else if action == scheduler.ActionCount && (err != nil || count != 3) {
					t.Fatalf("recovered count = %d, %v", count, err)
				} else if action == scheduler.ActionCollect {
					var got []string
					for _, record := range records {
						got = append(got, string(record))
					}
					sort.Strings(got)
					want := []string{`{"key":"alpha","value":2}`, `{"key":"beta","value":2}`, `{"key":"gamma","value":1}`}
					if err != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("recovered collect = %v, %v", got, err)
					}
				}
				store.verifyAttempts(t, limit)
			})
		}
	}
}
