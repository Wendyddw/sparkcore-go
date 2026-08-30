package scheduler

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/plan"
)

// FunctionLookup resolves the named functions required by a job.
type FunctionLookup interface {
	Map(string) (executor.MapFunc, error)
	Filter(string) (executor.FilterFunc, error)
	PairMap(string) (executor.PairMapFunc, error)
	ValueMap(string) (executor.ValueMapFunc, error)
	Reduce(string) (executor.ReduceFunc, error)
}

// validateFunctions checks target-reachable functions before stage generation.
func validateFunctions(graph *plan.RDDGraph, target plan.RDDID, functions FunctionLookup) error {
	if err := graph.Validate(target); err != nil {
		return err
	}
	if functions == nil {
		return fmt.Errorf("validate functions for RDD %d: function lookup is nil", target)
	}

	visited := make(map[plan.RDDID]bool)
	var validateFrom func(plan.RDDID) error
	validateFrom = func(id plan.RDDID) error {
		if visited[id] {
			return nil
		}
		visited[id] = true

		node, _ := graph.Node(id)
		if err := validateNodeFunction(node, functions); err != nil {
			return fmt.Errorf("validate RDD %d (%s): %w", node.ID, node.Name, err)
		}
		for _, dependency := range node.Dependencies {
			if err := validateFrom(dependency.ParentID); err != nil {
				return err
			}
		}
		return nil
	}

	return validateFrom(target)
}

func validateNodeFunction(node plan.RDDNode, functions FunctionLookup) error {
	id := node.Operator.FunctionID
	var err error

	switch node.Operator.Kind {
	case plan.OpSource:
		return nil
	case plan.OpMap:
		_, err = functions.Map(id)
	case plan.OpFilter:
		_, err = functions.Filter(id)
	case plan.OpMapToPair:
		_, err = functions.PairMap(id)
	case plan.OpMapValues:
		_, err = functions.ValueMap(id)
	case plan.OpReduceByKey:
		_, err = functions.Reduce(id)
	}
	if err != nil {
		return fmt.Errorf("function %q: %w", id, err)
	}
	return nil
}
