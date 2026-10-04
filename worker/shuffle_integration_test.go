package worker_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

// Task sets are submitted explicitly; automatic stage readiness is the next batch.
func TestRuntimeShuffleReportsThroughHTTP(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "missing input"}[missing], func(t *testing.T) {
			tasks, client := coordinatorClient(t)
			root := t.TempDir()
			store, err := shuffle.NewFilesystem(root)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Error(err)
				}
			})
			config := runtimeConfig(2)
			config.ShuffleStore = store
			config.RegisterFunctions = examplefuncs.Register
			config.Sources = sourceFunc(func(ctx context.Context, path string, p plan.PartitionID, n int) (executor.Iterator, error) {
				records := []executor.Record{"alpha", "alpha"}
				if p == 1 {
					records = []executor.Record{"beta"}
				}
				return recordsSource(records...).Open(ctx, path, p, n)
			})
			startRuntime(t, newRuntime(t, client, config))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			maps := scheduler.TaskSet{JobID: 7, StageID: 3, StageAttemptID: 2}
			for p := range 2 {
				maps.Tasks = append(maps.Tasks, scheduler.Task{ID: plan.TaskID(p), StageID: 3, StageKind: scheduler.StageShuffleMap, PartitionID: plan.PartitionID(p), NumPartitions: 2,
					ShuffleWrite: &scheduler.ShuffleWriteSpec{ShuffleID: 4, Partitioner: plan.HashPartitioner(1), AggregatorID: "sum_int", MapSideCombine: true},
					Operations: []scheduler.StageOperation{
						{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 0, Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "test-input"}}},
						{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 1, Operator: plan.OperatorSpec{Kind: plan.OpMapToPair, FunctionID: "word_pair"}}},
					}})
			}
			observer := clientObserver{make(chan scheduler.TaskAttemptSuccess, 2), make(chan scheduler.TaskAttemptFailure, 2)}
			if err := tasks.ScheduleTaskSet(ctx, maps, observer); err != nil {
				t.Fatal(err)
			}
			inputs := shuffle.InputSnapshot{JobID: 7, ShuffleID: 4, StageID: 3, StageAttemptID: 2, NumMapPartitions: 2, NumReducePartitions: 1, Outputs: make([]shuffle.MapOutput, 2)}
			for range 2 {
				report := receive(t, observer.successes)
				if report.Output.ShuffleOutput == nil || report.Output.Records != nil {
					t.Fatalf("map report lost descriptor: %+v", report)
				}
				inputs.Outputs[report.PartitionID] = *report.Output.ShuffleOutput
				inputs.RunID = report.Output.ShuffleOutput.Attempt.RunID
			}
			if err := inputs.Validate(); err != nil {
				t.Fatal(err)
			}
			if missing {
				if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.Name() == "bucket-0.jsonl" {
						return os.Remove(path)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			reduces := scheduler.TaskSet{JobID: 7, StageID: 4, ShuffleInputs: &inputs, Tasks: []scheduler.Task{{ID: 2, StageID: 4, StageKind: scheduler.StageResult, NumPartitions: 1,
				FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCollect, TargetRDD: 2}, Operations: []scheduler.StageOperation{
					{Kind: scheduler.StageOperationShuffleRead, ShuffleRead: &scheduler.ShuffleReadSpec{ShuffleID: 4, Partitioner: plan.HashPartitioner(1)}},
					{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 2, Operator: plan.OperatorSpec{Kind: plan.OpReduceByKey, FunctionID: "sum_int"}}},
				}}}}
			err = tasks.ScheduleTaskSet(ctx, reduces, observer)
			if missing {
				if err == nil {
					t.Fatal("missing input succeeded")
				}
				report := receive(t, observer.failures)
				if report.Kind != scheduler.FailureShuffleInput || report.ShuffleInput == nil || report.ShuffleInput.Attempt != inputs.Outputs[0].Attempt || report.ShuffleInput.PartitionID != 0 {
					t.Fatalf("failure identity lost through HTTP: %+v", report)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				report := receive(t, observer.successes)
				got := make(map[string]int64)
				for _, raw := range report.Output.Records {
					var pair struct {
						Key   string
						Value int64
					}
					if err := json.Unmarshal(raw.(json.RawMessage), &pair); err != nil {
						t.Fatal(err)
					}
					got[pair.Key] = pair.Value
				}
				if !reflect.DeepEqual(got, map[string]int64{"alpha": 2, "beta": 1}) {
					t.Fatalf("unexpected reduction: %v", got)
				}
			}
			snapshot, err := tasks.Worker("a")
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.ReservedSlots != 0 {
				t.Fatalf("terminal reports left %d reservations", snapshot.ReservedSlots)
			}
		})
	}
}
