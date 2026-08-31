package scheduler

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// FunctionLookup resolves the named functions required by a job.
type FunctionLookup interface {
	HasMap(string) bool
	HasFilter(string) bool
	HasPairMap(string) bool
	HasValueMap(string) bool
	HasReduce(string) bool
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
	var exists bool
	var kind string

	switch node.Operator.Kind {
	case plan.OpSource:
		return nil
	case plan.OpMap:
		kind, exists = "map", functions.HasMap(id)
	case plan.OpFilter:
		kind, exists = "filter", functions.HasFilter(id)
	case plan.OpMapToPair:
		kind, exists = "pair-map", functions.HasPairMap(id)
	case plan.OpMapValues:
		kind, exists = "value-map", functions.HasValueMap(id)
	case plan.OpReduceByKey:
		kind, exists = "reduce", functions.HasReduce(id)
	}
	if !exists {
		return fmt.Errorf("%s function %q is not registered", kind, id)
	}
	return nil
}
