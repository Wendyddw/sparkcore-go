package api

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// PlanStages validates and plans this RDD for action without executing tasks.
func (r RDD) PlanStages(
	action scheduler.ActionKind,
	functions scheduler.FunctionLookup,
) (scheduler.StagePlan, error) {
	if r.ctx == nil {
		return scheduler.StagePlan{}, fmt.Errorf("plan RDD %d: missing context", r.id)
	}
	stagePlan, err := scheduler.NewPlanner(functions).Plan(
		r.ctx.graph,
		scheduler.ActionSpec{Kind: action, TargetRDD: r.id},
	)
	if err != nil {
		return scheduler.StagePlan{}, fmt.Errorf("plan RDD %d: %w", r.id, err)
	}
	return stagePlan, nil
}
