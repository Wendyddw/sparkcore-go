package api

import (
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestMapAddsLazyNarrowNode(t *testing.T) {
	ctx := NewContext(nil, nil)
	parentID, err := ctx.graph.AddNode(plan.RDDNode{
		Name:          "PartitionedParent",
		Operator:      plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "missing.txt"},
		NumPartitions: 3,
		Partitioner:   partitionerPtr(plan.HashPartitioner(3)),
	})
	if err != nil {
		t.Fatalf("AddNode() error = %v", err)
	}
	parentBefore, _ := ctx.graph.Node(parentID)

	child, err := (RDD{id: parentID, ctx: ctx}).Map("unregistered-map")
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}
	if child.id == parentID {
		t.Fatalf("Map() RDD ID = %d, want a new ID", child.id)
	}
	if child.ctx != ctx {
		t.Fatal("Map() RDD does not reference its parent context")
	}

	parentAfter, _ := ctx.graph.Node(parentID)
	if !reflect.DeepEqual(parentAfter, parentBefore) {
		t.Errorf("Map() changed parent node:\ngot  %#v\nwant %#v", parentAfter, parentBefore)
	}

	node, ok := ctx.graph.Node(child.id)
	if !ok {
		t.Fatalf("graph does not contain RDD %d", child.id)
	}
	if node.Name != "Map" {
		t.Errorf("node name = %q, want %q", node.Name, "Map")
	}
	if node.Operator.Kind != plan.OpMap {
		t.Errorf("operator kind = %q, want %q", node.Operator.Kind, plan.OpMap)
	}
	if node.Operator.FunctionID != "unregistered-map" {
		t.Errorf("function ID = %q, want %q", node.Operator.FunctionID, "unregistered-map")
	}
	if node.NumPartitions != 3 {
		t.Errorf("partition count = %d, want 3", node.NumPartitions)
	}
	if node.Partitioner != nil {
		t.Errorf("partitioner = %#v, want nil", node.Partitioner)
	}
	if len(node.Dependencies) != 1 {
		t.Fatalf("dependency count = %d, want 1", len(node.Dependencies))
	}

	dependency := node.Dependencies[0]
	if dependency.Kind != plan.DependencyNarrow {
		t.Errorf("dependency kind = %q, want %q", dependency.Kind, plan.DependencyNarrow)
	}
	if dependency.ParentID != parentID {
		t.Errorf("parent ID = %d, want %d", dependency.ParentID, parentID)
	}
	if dependency.Narrow == nil || dependency.Narrow.Mapping != plan.NarrowOneToOne {
		t.Errorf("narrow mapping = %#v, want %q", dependency.Narrow, plan.NarrowOneToOne)
	}
	if dependency.Shuffle != nil {
		t.Errorf("shuffle specification = %#v, want nil", dependency.Shuffle)
	}
}

func TestMapRejectsRDDWithoutContext(t *testing.T) {
	if _, err := (RDD{}).Map("map"); err == nil {
		t.Fatal("Map() error = nil, want missing context error")
	}
}

func TestRemainingNarrowTransformations(t *testing.T) {
	tests := []struct {
		name                 string
		kind                 plan.OperatorKind
		preservesPartitioner bool
		transform            func(RDD) (RDD, error)
	}{
		{
			name:                 "Filter",
			kind:                 plan.OpFilter,
			preservesPartitioner: true,
			transform: func(rdd RDD) (RDD, error) {
				return rdd.Filter("unregistered-filter")
			},
		},
		{
			name: "MapToPair",
			kind: plan.OpMapToPair,
			transform: func(rdd RDD) (RDD, error) {
				return rdd.MapToPair("unregistered-pair-map")
			},
		},
		{
			name:                 "MapValues",
			kind:                 plan.OpMapValues,
			preservesPartitioner: true,
			transform: func(rdd RDD) (RDD, error) {
				return rdd.MapValues("unregistered-value-map")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := NewContext(nil, nil)
			parentID, err := ctx.graph.AddNode(plan.RDDNode{
				Name:          "PartitionedParent",
				Operator:      plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "missing.txt"},
				NumPartitions: 3,
				Partitioner:   partitionerPtr(plan.HashPartitioner(3)),
			})
			if err != nil {
				t.Fatalf("AddNode() error = %v", err)
			}
			parentBefore, _ := ctx.graph.Node(parentID)

			child, err := test.transform(RDD{id: parentID, ctx: ctx})
			if err != nil {
				t.Fatalf("%s() error = %v", test.name, err)
			}

			parentAfter, _ := ctx.graph.Node(parentID)
			if !reflect.DeepEqual(parentAfter, parentBefore) {
				t.Errorf("%s() changed parent node", test.name)
			}

			node, ok := ctx.graph.Node(child.id)
			if !ok {
				t.Fatalf("graph does not contain RDD %d", child.id)
			}
			if child.id == parentID {
				t.Fatalf("%s() reused parent ID %d", test.name, parentID)
			}
			if node.Name != test.name {
				t.Errorf("node name = %q, want %q", node.Name, test.name)
			}
			if node.Operator.Kind != test.kind {
				t.Errorf("operator kind = %q, want %q", node.Operator.Kind, test.kind)
			}
			if node.Operator.FunctionID == "" {
				t.Error("function ID is empty")
			}
			if node.NumPartitions != 3 {
				t.Errorf("partition count = %d, want 3", node.NumPartitions)
			}
			if test.preservesPartitioner {
				if !reflect.DeepEqual(node.Partitioner, parentBefore.Partitioner) {
					t.Errorf("partitioner = %#v, want %#v", node.Partitioner, parentBefore.Partitioner)
				}
			} else if node.Partitioner != nil {
				t.Errorf("partitioner = %#v, want nil", node.Partitioner)
			}
			assertNarrowDependency(t, node, parentID)
		})
	}
}

func TestReduceByKeyAddsShuffleNode(t *testing.T) {
	ctx := NewContext(nil, nil)
	parent, err := ctx.TextFile("missing.txt", 4)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	requested := plan.HashPartitioner(2)

	first, err := parent.ReduceByKey("unregistered-sum", requested)
	if err != nil {
		t.Fatalf("ReduceByKey() error = %v", err)
	}
	second, err := parent.ReduceByKey("unregistered-sum", requested)
	if err != nil {
		t.Fatalf("second ReduceByKey() error = %v", err)
	}

	firstNode, ok := ctx.graph.Node(first.id)
	if !ok {
		t.Fatalf("graph does not contain RDD %d", first.id)
	}
	secondNode, ok := ctx.graph.Node(second.id)
	if !ok {
		t.Fatalf("graph does not contain RDD %d", second.id)
	}
	if firstNode.Name != "ReduceByKey" {
		t.Errorf("node name = %q, want %q", firstNode.Name, "ReduceByKey")
	}
	if firstNode.Operator.Kind != plan.OpReduceByKey {
		t.Errorf("operator kind = %q, want %q", firstNode.Operator.Kind, plan.OpReduceByKey)
	}
	if firstNode.Operator.FunctionID != "unregistered-sum" {
		t.Errorf("function ID = %q, want %q", firstNode.Operator.FunctionID, "unregistered-sum")
	}
	if firstNode.NumPartitions != 2 {
		t.Errorf("partition count = %d, want 2", firstNode.NumPartitions)
	}
	if !reflect.DeepEqual(firstNode.Partitioner, &requested) {
		t.Errorf("partitioner = %#v, want %#v", firstNode.Partitioner, &requested)
	}
	if len(firstNode.Dependencies) != 1 {
		t.Fatalf("dependency count = %d, want 1", len(firstNode.Dependencies))
	}

	dependency := firstNode.Dependencies[0]
	if dependency.Kind != plan.DependencyShuffle {
		t.Errorf("dependency kind = %q, want %q", dependency.Kind, plan.DependencyShuffle)
	}
	if dependency.ParentID != parent.id {
		t.Errorf("parent ID = %d, want %d", dependency.ParentID, parent.id)
	}
	if dependency.Narrow != nil {
		t.Errorf("narrow specification = %#v, want nil", dependency.Narrow)
	}
	if dependency.Shuffle == nil {
		t.Fatal("shuffle specification = nil")
	}
	if dependency.Shuffle.Partitioner != requested {
		t.Errorf("shuffle partitioner = %#v, want %#v", dependency.Shuffle.Partitioner, requested)
	}
	if dependency.Shuffle.AggregatorID != "unregistered-sum" {
		t.Errorf("aggregator ID = %q, want %q", dependency.Shuffle.AggregatorID, "unregistered-sum")
	}
	if !dependency.Shuffle.MapSideCombine {
		t.Error("map-side combine = false, want true")
	}
	if dependency.Shuffle.ShuffleID == secondNode.Dependencies[0].Shuffle.ShuffleID {
		t.Errorf("separate shuffles share ID %d", dependency.Shuffle.ShuffleID)
	}
}

func TestReduceByKeyRejectsInvalidPartitioner(t *testing.T) {
	ctx := NewContext(nil, nil)
	parent, err := ctx.TextFile("missing.txt", 4)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}

	if _, err := parent.ReduceByKey("sum", plan.HashPartitioner(0)); err == nil {
		t.Fatal("ReduceByKey() error = nil, want invalid partitioner error")
	}
	if ctx.graph.Len() != 1 {
		t.Fatalf("graph length = %d, want 1", ctx.graph.Len())
	}
}

func assertNarrowDependency(t *testing.T, node plan.RDDNode, parentID plan.RDDID) {
	t.Helper()
	if len(node.Dependencies) != 1 {
		t.Fatalf("dependency count = %d, want 1", len(node.Dependencies))
	}
	dependency := node.Dependencies[0]
	if dependency.Kind != plan.DependencyNarrow {
		t.Errorf("dependency kind = %q, want %q", dependency.Kind, plan.DependencyNarrow)
	}
	if dependency.ParentID != parentID {
		t.Errorf("parent ID = %d, want %d", dependency.ParentID, parentID)
	}
	if dependency.Narrow == nil || dependency.Narrow.Mapping != plan.NarrowOneToOne {
		t.Errorf("narrow mapping = %#v, want %q", dependency.Narrow, plan.NarrowOneToOne)
	}
	if dependency.Shuffle != nil {
		t.Errorf("shuffle specification = %#v, want nil", dependency.Shuffle)
	}
}

func partitionerPtr(partitioner plan.PartitionerSpec) *plan.PartitionerSpec {
	return &partitioner
}
