package executor

import (
	"context"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// SchedulerActionRunner adapts DAGScheduler to the API action-runner contract.
type SchedulerActionRunner struct {
	scheduler *scheduler.DAGScheduler
}

// NewSchedulerActionRunner creates an API-facing scheduler adapter.
func NewSchedulerActionRunner(dagScheduler *scheduler.DAGScheduler) *SchedulerActionRunner {
	return &SchedulerActionRunner{scheduler: dagScheduler}
}

// Collect plans and executes a Collect action.
func (r *SchedulerActionRunner) Collect(
	ctx context.Context,
	graph *plan.RDDGraph,
	target plan.RDDID,
) ([]Record, error) {
	result, err := r.run(ctx, graph, scheduler.ActionCollect, target)
	if err != nil {
		return nil, err
	}
	records := make([]Record, len(result.Records))
	for index, record := range result.Records {
		records[index] = record
	}
	return records, nil
}

// Count plans and executes a Count action.
func (r *SchedulerActionRunner) Count(
	ctx context.Context,
	graph *plan.RDDGraph,
	target plan.RDDID,
) (int64, error) {
	result, err := r.run(ctx, graph, scheduler.ActionCount, target)
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

func (r *SchedulerActionRunner) run(
	ctx context.Context,
	graph *plan.RDDGraph,
	action scheduler.ActionKind,
	target plan.RDDID,
) (scheduler.JobResult, error) {
	if r == nil || r.scheduler == nil {
		return scheduler.JobResult{}, fmt.Errorf("scheduler action runner is not configured")
	}
	return r.scheduler.Run(ctx, graph, scheduler.ActionSpec{Kind: action, TargetRDD: target})
}
