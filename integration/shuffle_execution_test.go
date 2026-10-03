package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/internal/examplefuncs"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

// Dispatch is explicit until Session 4 adds the DAG's accepted-output barrier.
func TestPlannedShuffleExecution(t *testing.T) {
	for _, combine := range []bool{false, true} {
		for _, empty := range []bool{false, true} {
			t.Run(fmt.Sprintf("combine=%t/empty=%t", combine, empty), func(t *testing.T) {
				registry := executor.NewFunctionRegistry()
				if err := examplefuncs.Register(registry); err != nil {
					t.Fatal(err)
				}
				sum, _ := registry.Reduce("sum_int")
				if err := registry.RegisterValueMap("double_count", func(v executor.Record) (executor.Record, error) { return sum(v, v) }); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(t.TempDir(), "words.txt")
				data := " alpha \nbeta\n\nalpha\ngamma\nbeta\n"
				if empty {
					data = ""
				}
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
				ctx := api.NewContext(registry, nil)
				rdd, err := ctx.TextFile(path, 4)
				if err != nil {
					t.Fatal(err)
				}
				rdd, err = rdd.Map("normalize")
				if err != nil {
					t.Fatal(err)
				}
				rdd, err = rdd.Filter("non_empty")
				if err != nil {
					t.Fatal(err)
				}
				rdd, err = rdd.MapToPair("word_pair")
				if err != nil {
					t.Fatal(err)
				}
				rdd, err = rdd.ReduceByKey("sum_int", plan.HashPartitioner(2))
				if err != nil {
					t.Fatal(err)
				}
				rdd, err = rdd.MapValues("double_count")
				if err != nil {
					t.Fatal(err)
				}
				stages, err := rdd.PlanStages(scheduler.ActionCollect, registry)
				if err != nil {
					t.Fatal(err)
				}
				if len(stages.Stages) != 2 {
					t.Fatalf("stages: %+v", stages)
				}
				stages.Stages[0].ShuffleWrite.MapSideCombine = combine
				tasks, err := scheduler.GenerateTasks(stages)
				if err != nil {
					t.Fatal(err)
				}
				root := t.TempDir()
				writerStore, err := shuffle.NewFilesystem(root)
				if err != nil {
					t.Fatal(err)
				}
				readerStore, err := shuffle.NewFilesystem(root)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := writerStore.Close(); err != nil {
						t.Error(err)
					}
					if err := readerStore.Close(); err != nil {
						t.Error(err)
					}
				})
				mapper := executor.NewLocalRunner(registry, nil, 2, executor.WithShuffleStore(writerStore))
				reducer := executor.NewLocalRunner(registry, nil, 2, executor.WithShuffleStore(readerStore))
				run := scheduler.NewRunID()
				inputs := shuffle.InputSnapshot{RunID: run, JobID: 2, ShuffleID: stages.Stages[0].ShuffleWrite.ShuffleID,
					StageID: stages.Stages[0].ID, NumMapPartitions: 4, NumReducePartitions: 2}
				var collected []any
				var count int64
				for _, task := range tasks {
					e := scheduler.TaskExecution{RunID: run, JobID: 2, Task: task, WorkerID: "direct-test",
						Attempt: scheduler.TaskAttemptIdentity{ID: plan.TaskAttemptID(task.ID), TaskID: task.ID}}
					if task.StageKind == scheduler.StageShuffleMap {
						output, err := mapper.RunTask(context.Background(), e)
						if err != nil {
							t.Fatal(err)
						}
						inputs.Outputs = append(inputs.Outputs, *output.ShuffleOutput)
						continue
					}
					e.ShuffleInputs = &inputs
					output, err := reducer.RunTask(context.Background(), e)
					if err != nil {
						t.Fatal(err)
					}
					var keys []string
					for _, record := range output.Records {
						keys = append(keys, record.(executor.KeyValue).Key)
					}
					if !sort.StringsAreSorted(keys) {
						t.Fatalf("partition keys not sorted: %v", keys)
					}
					collected = append(collected, output.Records...)
					e.Task.FinalAction = &scheduler.ActionSpec{Kind: scheduler.ActionCount, TargetRDD: task.FinalAction.TargetRDD}
					output, err = reducer.RunTask(context.Background(), e)
					if err != nil {
						t.Fatal(err)
					}
					count += output.Count
				}
				want := map[string]int64{"alpha": 4, "beta": 4, "gamma": 2}
				if empty {
					want = map[string]int64{}
				}
				got := make(map[string]int64)
				for _, record := range collected {
					pair := record.(executor.KeyValue)
					if _, exists := got[pair.Key]; exists {
						t.Fatalf("duplicate reduced key %s", pair.Key)
					}
					got[pair.Key] = pair.Value.(int64)
				}
				if !reflect.DeepEqual(got, want) || count != int64(len(want)) {
					t.Fatalf("got %v, count %d; want %v", got, count, want)
				}
			})
		}
	}
}
