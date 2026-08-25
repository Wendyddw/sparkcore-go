package api

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// RDD is a lightweight handle to an immutable lineage node.
// Data remains unevaluated until an action runs through the owning context.
type RDD struct {
	id  plan.RDDID
	ctx *Context
}

// Map records a lazy one-to-one transformation.
// Function lookup and execution are deferred until an action is planned.
func (r RDD) Map(functionID string) (RDD, error) {
	return r.addNarrowTransformation("Map", plan.OpMap, functionID, false)
}

// Filter records a lazy transformation that retains selected records.
func (r RDD) Filter(functionID string) (RDD, error) {
	return r.addNarrowTransformation("Filter", plan.OpFilter, functionID, true)
}

// MapToPair records a lazy transformation from records to key-value records.
func (r RDD) MapToPair(functionID string) (RDD, error) {
	return r.addNarrowTransformation("MapToPair", plan.OpMapToPair, functionID, false)
}

// MapValues records a lazy value transformation that preserves keys.
func (r RDD) MapValues(functionID string) (RDD, error) {
	return r.addNarrowTransformation("MapValues", plan.OpMapValues, functionID, true)
}

// ReduceByKey records a lazy shuffle that combines values with the same key.
func (r RDD) ReduceByKey(functionID string, partitioner plan.PartitionerSpec) (RDD, error) {
	if r.ctx == nil {
		return RDD{}, fmt.Errorf("reduce-by-key RDD %d: missing context", r.id)
	}

	if _, ok := r.ctx.graph.Node(r.id); !ok {
		return RDD{}, fmt.Errorf("reduce-by-key RDD %d: parent does not exist", r.id)
	}

	id, err := r.ctx.graph.AddNode(plan.RDDNode{
		Name: "ReduceByKey",
		Operator: plan.OperatorSpec{
			Kind:       plan.OpReduceByKey,
			FunctionID: functionID,
		},
		Dependencies: []plan.Dependency{{
			Kind:     plan.DependencyShuffle,
			ParentID: r.id,
			Shuffle: &plan.ShuffleDependencySpec{
				ShuffleID:      r.ctx.graph.NewShuffleID(),
				Partitioner:    partitioner,
				AggregatorID:   functionID,
				MapSideCombine: true,
			},
		}},
		NumPartitions: partitioner.NumPartitions,
		Partitioner:   &partitioner,
	})
	if err != nil {
		return RDD{}, fmt.Errorf("reduce-by-key RDD %d: %w", r.id, err)
	}

	return RDD{id: id, ctx: r.ctx}, nil
}

func (r RDD) addNarrowTransformation(
	name string,
	kind plan.OperatorKind,
	functionID string,
	preservePartitioner bool,
) (RDD, error) {
	if r.ctx == nil {
		return RDD{}, fmt.Errorf("%s RDD %d: missing context", name, r.id)
	}

	parent, ok := r.ctx.graph.Node(r.id)
	if !ok {
		return RDD{}, fmt.Errorf("%s RDD %d: parent does not exist", name, r.id)
	}

	var partitioner *plan.PartitionerSpec
	if preservePartitioner {
		partitioner = parent.Partitioner
	}

	id, err := r.ctx.graph.AddNode(plan.RDDNode{
		Name: name,
		Operator: plan.OperatorSpec{
			Kind:       kind,
			FunctionID: functionID,
		},
		Dependencies: []plan.Dependency{{
			Kind:     plan.DependencyNarrow,
			ParentID: r.id,
			Narrow: &plan.NarrowDependencySpec{
				Mapping: plan.NarrowOneToOne,
			},
		}},
		NumPartitions: parent.NumPartitions,
		Partitioner:   partitioner,
	})
	if err != nil {
		return RDD{}, fmt.Errorf("%s RDD %d: %w", name, r.id, err)
	}

	return RDD{id: id, ctx: r.ctx}, nil
}
