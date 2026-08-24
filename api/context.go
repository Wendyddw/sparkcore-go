// Package api exposes the driver-facing lazy RDD API.
package api

import (
	"context"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
)

// ActionRunner executes actions against an existing lineage graph.
type ActionRunner interface {
	Collect(context.Context, *plan.RDDGraph, plan.RDDID) ([]executor.Record, error)
	Count(context.Context, *plan.RDDGraph, plan.RDDID) (int64, error)
}

// TextFile records a lazy text source in the lineage graph.
// The path is not opened until an action executes the RDD.
func (c *Context) TextFile(path string, numPartitions int) (RDD, error) {
	id, err := c.graph.AddNode(plan.RDDNode{
		Name: "TextFile",
		Operator: plan.OperatorSpec{
			Kind:       plan.OpSource,
			SourcePath: path,
		},
		NumPartitions: numPartitions,
	})
	if err != nil {
		return RDD{}, fmt.Errorf("create TextFile RDD: %w", err)
	}

	return RDD{id: id, ctx: c}, nil
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
