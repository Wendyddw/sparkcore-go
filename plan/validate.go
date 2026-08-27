package plan

import "fmt"

// Validate checks the lineage required to compute target.
func (g *RDDGraph) Validate(target RDDID) error {
	if g == nil {
		return fmt.Errorf("validate target RDD %d: graph is nil", target)
	}
	if _, ok := g.nodes[target]; !ok {
		return fmt.Errorf("validate target RDD %d: does not exist", target)
	}

	states := make(map[RDDID]visitState)
	return g.validateFrom(target, states)
}

type visitState uint8

const (
	visitUnseen visitState = iota
	visitActive
	visitDone
)

func (g *RDDGraph) validateFrom(id RDDID, states map[RDDID]visitState) error {
	node := g.nodes[id]
	switch states[id] {
	case visitActive:
		return nodeValidationError(node, "lineage contains a cycle")
	case visitDone:
		return nil
	}

	states[id] = visitActive
	if err := validateNodeShape(node); err != nil {
		return nodeValidationError(node, "%v", err)
	}
	if err := validateOperator(node); err != nil {
		return nodeValidationError(node, "%v", err)
	}

	for i, dependency := range node.Dependencies {
		parent, ok := g.nodes[dependency.ParentID]
		if !ok {
			return nodeValidationError(
				node,
				"dependency %d references missing parent RDD %d",
				i,
				dependency.ParentID,
			)
		}
		if dependency.Kind == DependencyShuffle &&
			dependency.Shuffle.Partitioner.NumPartitions != node.NumPartitions {
			return nodeValidationError(
				node,
				"shuffle reduce count %d does not match partitioner count %d",
				node.NumPartitions,
				dependency.Shuffle.Partitioner.NumPartitions,
			)
		}
		if err := g.validateFrom(parent.ID, states); err != nil {
			return err
		}
	}

	states[id] = visitDone
	return nil
}

func validateOperator(node RDDNode) error {
	switch node.Operator.Kind {
	case OpSource:
		if node.Operator.SourcePath == "" {
			return fmt.Errorf("source path must not be empty")
		}
		if len(node.Dependencies) != 0 {
			return fmt.Errorf("source must not have dependencies")
		}
	case OpMap, OpFilter, OpMapToPair, OpMapValues:
		if node.Operator.FunctionID == "" {
			return fmt.Errorf("%s function ID must not be empty", node.Operator.Kind)
		}
		if len(node.Dependencies) != 1 || node.Dependencies[0].Kind != DependencyNarrow {
			return fmt.Errorf("%s requires one narrow dependency", node.Operator.Kind)
		}
	case OpReduceByKey:
		if node.Operator.FunctionID == "" {
			return fmt.Errorf("%s function ID must not be empty", node.Operator.Kind)
		}
		if len(node.Dependencies) != 1 || node.Dependencies[0].Kind != DependencyShuffle {
			return fmt.Errorf("%s requires one shuffle dependency", node.Operator.Kind)
		}
	default:
		return fmt.Errorf("unsupported operator kind %q", node.Operator.Kind)
	}

	return nil
}

func nodeValidationError(node RDDNode, format string, args ...any) error {
	return fmt.Errorf("validate RDD %d (%s): %s", node.ID, node.Name, fmt.Sprintf(format, args...))
}
