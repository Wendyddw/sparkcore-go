package plan

import (
	"strings"
	"testing"
)

func TestValidateAcceptsExistingTarget(t *testing.T) {
	graph := NewRDDGraph()
	target, err := graph.AddNode(RDDNode{
		Name:          "TextFile",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "events.txt"},
		NumPartitions: 4,
	})
	if err != nil {
		t.Fatalf("AddNode() error = %v", err)
	}

	if err := graph.Validate(target); err != nil {
		t.Fatalf("Validate(%d) error = %v", target, err)
	}
}

func TestValidateRejectsMissingTarget(t *testing.T) {
	graph := NewRDDGraph()
	target := RDDID(7)

	err := graph.Validate(target)
	if err == nil {
		t.Fatalf("Validate(%d) error = nil, want missing target error", target)
	}
	if !strings.Contains(err.Error(), "RDD 7") || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("Validate(%d) error = %q, want RDD ID and missing-target reason", target, err)
	}
}

func TestValidateRejectsNilGraph(t *testing.T) {
	var graph *RDDGraph

	err := graph.Validate(3)
	if err == nil {
		t.Fatal("Validate() error = nil, want nil graph error")
	}
	if !strings.Contains(err.Error(), "RDD 3") || !strings.Contains(err.Error(), "graph is nil") {
		t.Fatalf("Validate() error = %q, want RDD ID and nil-graph reason", err)
	}
}

func TestValidateRejectsMissingParent(t *testing.T) {
	graph := NewRDDGraph()
	target, err := graph.AddNode(validNarrowNode("Map", OpMap, "map", 99))
	if err != nil {
		t.Fatalf("AddNode() error = %v", err)
	}

	assertValidationError(t, graph, target, "RDD 0", "Map", "missing parent RDD 99")
}

func TestValidateRejectsMalformedDependency(t *testing.T) {
	graph := NewRDDGraph()
	parent, err := graph.AddNode(validationSourceNode())
	if err != nil {
		t.Fatalf("AddNode(source) error = %v", err)
	}
	target, err := graph.AddNode(validNarrowNode("Map", OpMap, "map", parent))
	if err != nil {
		t.Fatalf("AddNode(map) error = %v", err)
	}

	node := graph.nodes[target]
	node.Dependencies[0].Narrow = nil
	graph.nodes[target] = node

	assertValidationError(t, graph, target, "RDD 1", "Map", "narrow dependency")
}

func TestValidateRejectsBadPartitionCount(t *testing.T) {
	graph := NewRDDGraph()
	target, err := graph.AddNode(validationSourceNode())
	if err != nil {
		t.Fatalf("AddNode() error = %v", err)
	}

	node := graph.nodes[target]
	node.NumPartitions = 0
	graph.nodes[target] = node

	assertValidationError(t, graph, target, "RDD 0", "TextFile", "invalid partition count 0")
}

func TestValidateRejectsCycle(t *testing.T) {
	graph := NewRDDGraph()
	first, err := graph.AddNode(validationSourceNode())
	if err != nil {
		t.Fatalf("AddNode(source) error = %v", err)
	}
	second, err := graph.AddNode(validNarrowNode("Map", OpMap, "map", first))
	if err != nil {
		t.Fatalf("AddNode(map) error = %v", err)
	}

	node := graph.nodes[first]
	node.Name = "Filter"
	node.Operator = OperatorSpec{Kind: OpFilter, FunctionID: "filter"}
	node.Dependencies = []Dependency{narrowDependency(second)}
	graph.nodes[first] = node

	assertValidationError(t, graph, second, "RDD 1", "Map", "cycle")
}

func TestValidateRejectsShufflePartitionMismatch(t *testing.T) {
	graph := NewRDDGraph()
	parent, err := graph.AddNode(validationSourceNode())
	if err != nil {
		t.Fatalf("AddNode(source) error = %v", err)
	}
	partitioner := HashPartitioner(2)
	target, err := graph.AddNode(RDDNode{
		Name:          "ReduceByKey",
		Operator:      OperatorSpec{Kind: OpReduceByKey, FunctionID: "sum"},
		NumPartitions: 2,
		Partitioner:   &partitioner,
		Dependencies: []Dependency{{
			Kind:     DependencyShuffle,
			ParentID: parent,
			Shuffle: &ShuffleDependencySpec{
				ShuffleID:    0,
				Partitioner:  HashPartitioner(3),
				AggregatorID: "sum",
			},
		}},
	})
	if err != nil {
		t.Fatalf("AddNode(reduce) error = %v", err)
	}

	assertValidationError(t, graph, target, "RDD 1", "ReduceByKey", "reduce count 2", "partitioner count 3")
}

func TestValidateRejectsInvalidOperatorSpecs(t *testing.T) {
	tests := []struct {
		name string
		node RDDNode
		want string
	}{
		{
			name: "source without path",
			node: RDDNode{Name: "TextFile", Operator: OperatorSpec{Kind: OpSource}, NumPartitions: 1},
			want: "source path",
		},
		{
			name: "map without function",
			node: validNarrowNode("Map", OpMap, "", 0),
			want: "function ID",
		},
		{
			name: "map with shuffle",
			node: RDDNode{
				Name:          "Map",
				Operator:      OperatorSpec{Kind: OpMap, FunctionID: "map"},
				NumPartitions: 1,
				Dependencies: []Dependency{{
					Kind:     DependencyShuffle,
					ParentID: 0,
					Shuffle:  &ShuffleDependencySpec{Partitioner: HashPartitioner(1)},
				}},
			},
			want: "one narrow dependency",
		},
		{
			name: "unsupported operator",
			node: RDDNode{Name: "Unknown", Operator: OperatorSpec{Kind: "unknown"}, NumPartitions: 1},
			want: "unsupported operator",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph := NewRDDGraph()
			if test.node.Operator.Kind != OpSource && len(test.node.Dependencies) > 0 {
				if _, err := graph.AddNode(validationSourceNode()); err != nil {
					t.Fatalf("AddNode(source) error = %v", err)
				}
			}
			target, err := graph.AddNode(test.node)
			if err != nil {
				t.Fatalf("AddNode(target) error = %v", err)
			}

			assertValidationError(t, graph, target, test.node.Name, test.want)
		})
	}
}

func TestValidateIgnoresUnreachableInvalidNode(t *testing.T) {
	graph := NewRDDGraph()
	target, err := graph.AddNode(validationSourceNode())
	if err != nil {
		t.Fatalf("AddNode(source) error = %v", err)
	}
	if _, err := graph.AddNode(validNarrowNode("Map", OpMap, "map", 99)); err != nil {
		t.Fatalf("AddNode(unreachable map) error = %v", err)
	}

	if err := graph.Validate(target); err != nil {
		t.Fatalf("Validate(%d) error = %v", target, err)
	}
}

func validationSourceNode() RDDNode {
	return RDDNode{
		Name:          "TextFile",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "events.txt"},
		NumPartitions: 1,
	}
}

func validNarrowNode(name string, kind OperatorKind, functionID string, parent RDDID) RDDNode {
	return RDDNode{
		Name:          name,
		Operator:      OperatorSpec{Kind: kind, FunctionID: functionID},
		NumPartitions: 1,
		Dependencies:  []Dependency{narrowDependency(parent)},
	}
}

func narrowDependency(parent RDDID) Dependency {
	return Dependency{
		Kind:     DependencyNarrow,
		ParentID: parent,
		Narrow:   &NarrowDependencySpec{Mapping: NarrowOneToOne},
	}
}

func assertValidationError(t *testing.T, graph *RDDGraph, target RDDID, parts ...string) {
	t.Helper()
	err := graph.Validate(target)
	if err == nil {
		t.Fatalf("Validate(%d) error = nil, want validation error", target)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("Validate(%d) error = %q, want it to contain %q", target, err, part)
		}
	}
}
