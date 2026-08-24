// Package api exposes the driver-facing lazy RDD API.
package api

import (
	"context"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
)

// ActionRunner executes actions against an existing lineage graph.
type ActionRunner interface {
	Collect(context.Context, *plan.RDDGraph, plan.RDDID) ([]executor.Record, error)
	Count(context.Context, *plan.RDDGraph, plan.RDDID) (int64, error)
}

// Context owns driver-side lineage and execution dependencies.
type Context struct {
	graph    *plan.RDDGraph
	registry *executor.FunctionRegistry
	runner   ActionRunner
}

// NewContext creates a driver context with an empty lineage graph.
// A nil registry is replaced with an empty registry. The runner may be nil
// when callers only need to construct or inspect lineage.
func NewContext(registry *executor.FunctionRegistry, runner ActionRunner) *Context {
	if registry == nil {
		registry = executor.NewFunctionRegistry()
	}

	return &Context{
		graph:    plan.NewRDDGraph(),
		registry: registry,
		runner:   runner,
	}
}
