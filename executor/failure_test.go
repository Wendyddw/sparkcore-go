package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestRunnerFailureClassification(t *testing.T) {
	for _, name := range []string{"identity", "registry", "concurrency", "missing function", "missing store", "pipeline", "pair shape", "user error", "marked user error"} {
		t.Run(name, func(t *testing.T) {
			registry := NewFunctionRegistry()
			execution := runnerExecution(narrowTasks(t, scheduler.ActionCount, 1)[0])
			cause := errors.New("function failed")
			fn := func(r Record) (Record, error) { return r, nil }
			if name == "user error" {
				fn = func(Record) (Record, error) { return nil, cause }
			}
			if name == "marked user error" {
				fn = func(Record) (Record, error) { return nil, scheduler.PermanentFailure(cause) }
			}
			mustRegisterRunnerMap(t, registry, "double", fn)
			mustRegisterRunnerFilter(t, registry, "over-four", func(Record) (bool, error) { return true, nil })
			runner := NewLocalRunner(registry, memorySourceReader{partitions: map[plan.PartitionID][]Record{0: {1}}}, 1)
			switch name {
			case "identity":
				execution.RunID = "invalid"
			case "registry":
				runner.registry = nil
			case "concurrency":
				runner = NewLocalRunner(registry, nil, 0)
			case "missing function":
				execution.Task.Operations[1].RDD.Operator.FunctionID = "missing"
			case "missing store":
				execution = mapExecution(false)
			case "pipeline":
				execution.Task.Operations[1].Kind = "unsupported"
			case "pair shape":
				if err := registry.RegisterValueMap("value", fn); err != nil {
					t.Fatal(err)
				}
				execution.Task.Operations[1].RDD.Operator.Kind = plan.OpMapValues
				execution.Task.Operations[1].RDD.Operator.FunctionID = "value"
			}
			_, err := runner.RunTask(context.Background(), execution)
			if err == nil {
				t.Fatal("expected failure")
			}
			want := scheduler.FailurePermanent
			if name == "user error" {
				want = scheduler.FailureExecution
			}
			if kind, input := scheduler.ClassifyFailure(context.Background(), err); kind != want || input != nil {
				t.Fatalf("failure = %s (%v), want %s", kind, err, want)
			}
			if (name == "user error" || name == "marked user error") && !errors.Is(err, cause) {
				t.Fatal("lost user error cause")
			}
		})
	}
}
