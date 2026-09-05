package api

import (
	"testing"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestPlanStagesDoesNotRunAction(t *testing.T) {
	runner := &recordingActionRunner{}
	registry := executor.NewFunctionRegistry()
	if err := registry.RegisterMap("identity", func(record executor.Record) (executor.Record, error) {
		return record, nil
	}); err != nil {
		t.Fatalf("RegisterMap() error = %v", err)
	}
	ctx := NewContext(registry, runner)
	source, err := ctx.TextFile("missing.txt", 4)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	target, err := source.Map("identity")
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}

	stagePlan, err := target.PlanStages(scheduler.ActionCount, registry)
	if err != nil {
		t.Fatalf("PlanStages() error = %v", err)
	}
	if len(stagePlan.Stages) != 1 || stagePlan.Stages[0].NumPartitions != 4 {
		t.Fatalf("stage plan = %#v", stagePlan)
	}
	if runner.collectCalls != 0 || runner.countCalls != 0 {
		t.Fatalf("runner calls = Collect:%d Count:%d", runner.collectCalls, runner.countCalls)
	}
}
