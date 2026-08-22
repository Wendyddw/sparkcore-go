package plan

import "testing"

func TestRDDGraphAssignsMonotonicIDs(t *testing.T) {
	t.Parallel()

	graph := NewRDDGraph()
	firstID, err := graph.AddNode(validSourceNode())
	if err != nil {
		t.Fatalf("add first node: %v", err)
	}
	secondNode := validSourceNode()
	secondNode.ID = 99
	secondID, err := graph.AddNode(secondNode)
	if err != nil {
		t.Fatalf("add second node: %v", err)
	}

	if firstID != 0 || secondID != 1 {
		t.Fatalf("assigned IDs = (%d, %d), want (0, 1)", firstID, secondID)
	}
	if graph.Len() != 2 {
		t.Fatalf("graph length = %d, want 2", graph.Len())
	}

	nodes := graph.Nodes()
	if len(nodes) != 2 || nodes[0].ID != firstID || nodes[1].ID != secondID {
		t.Fatalf("nodes are not returned in ID order: %#v", nodes)
	}
}

func TestRDDGraphCopiesNodesAtBoundaries(t *testing.T) {
	t.Parallel()

	graph := NewRDDGraph()
	parentID, err := graph.AddNode(validSourceNode())
	if err != nil {
		t.Fatalf("add parent: %v", err)
	}

	partitioner := HashPartitioner(2)
	narrow := &NarrowDependencySpec{Mapping: NarrowOneToOne}
	shuffle := &ShuffleDependencySpec{
		ShuffleID:      7,
		Partitioner:    partitioner,
		AggregatorID:   "sum_amount",
		MapSideCombine: true,
	}
	node := RDDNode{
		Name:          "child",
		Operator:      OperatorSpec{Kind: OpMapValues, FunctionID: "format_amount"},
		NumPartitions: 2,
		Partitioner:   &partitioner,
		Dependencies: []Dependency{
			{Kind: DependencyNarrow, ParentID: parentID, Narrow: narrow},
			{Kind: DependencyShuffle, ParentID: parentID, Shuffle: shuffle},
		},
	}

	childID, err := graph.AddNode(node)
	if err != nil {
		t.Fatalf("add child: %v", err)
	}

	node.Partitioner.NumPartitions = 99
	node.Dependencies[0].Narrow.Mapping = "changed"
	node.Dependencies[1].Shuffle.AggregatorID = "changed"

	stored, ok := graph.Node(childID)
	if !ok {
		t.Fatalf("node %d not found", childID)
	}
	assertOriginalChild(t, stored)

	stored.Partitioner.NumPartitions = 99
	stored.Dependencies[0].Narrow.Mapping = "changed"
	stored.Dependencies[1].Shuffle.AggregatorID = "changed"

	storedAgain, ok := graph.Node(childID)
	if !ok {
		t.Fatalf("node %d not found on second read", childID)
	}
	assertOriginalChild(t, storedAgain)
}

func TestRDDGraphRejectsInvalidPartitionCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "zero", count: 0},
		{name: "negative", count: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := validSourceNode()
			node.NumPartitions = tt.count
			if _, err := NewRDDGraph().AddNode(node); err == nil {
				t.Fatalf("AddNode accepted partition count %d", tt.count)
			}
		})
	}
}

func TestRDDGraphRejectsMalformedDependencies(t *testing.T) {
	t.Parallel()

	validNarrow := &NarrowDependencySpec{Mapping: NarrowOneToOne}
	validShuffle := &ShuffleDependencySpec{
		ShuffleID:    1,
		Partitioner:  HashPartitioner(2),
		AggregatorID: "sum",
	}
	tests := []struct {
		name       string
		dependency Dependency
	}{
		{name: "unknown kind", dependency: Dependency{Kind: "unknown"}},
		{name: "narrow without spec", dependency: Dependency{Kind: DependencyNarrow}},
		{
			name: "unsupported narrow mapping",
			dependency: Dependency{
				Kind:   DependencyNarrow,
				Narrow: &NarrowDependencySpec{Mapping: "unsupported"},
			},
		},
		{
			name: "narrow with both specs",
			dependency: Dependency{
				Kind:    DependencyNarrow,
				Narrow:  validNarrow,
				Shuffle: validShuffle,
			},
		},
		{name: "shuffle without spec", dependency: Dependency{Kind: DependencyShuffle}},
		{
			name: "shuffle with invalid partitioner",
			dependency: Dependency{
				Kind: DependencyShuffle,
				Shuffle: &ShuffleDependencySpec{
					ShuffleID:   1,
					Partitioner: HashPartitioner(0),
				},
			},
		},
		{
			name: "shuffle with both specs",
			dependency: Dependency{
				Kind:    DependencyShuffle,
				Narrow:  validNarrow,
				Shuffle: validShuffle,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node := validSourceNode()
			node.Dependencies = []Dependency{tt.dependency}
			if _, err := NewRDDGraph().AddNode(node); err == nil {
				t.Fatalf("AddNode accepted malformed dependency: %#v", tt.dependency)
			}
		})
	}
}

func TestRDDGraphRejectsMismatchedPartitioner(t *testing.T) {
	t.Parallel()

	node := validSourceNode()
	partitioner := HashPartitioner(node.NumPartitions + 1)
	node.Partitioner = &partitioner

	if _, err := NewRDDGraph().AddNode(node); err == nil {
		t.Fatal("AddNode accepted a partitioner with a mismatched partition count")
	}
}

func validSourceNode() RDDNode {
	return RDDNode{
		Name:          "source",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "events.txt"},
		NumPartitions: 2,
	}
}

func assertOriginalChild(t *testing.T, node RDDNode) {
	t.Helper()

	if node.Partitioner == nil || node.Partitioner.NumPartitions != 2 {
		t.Fatalf("stored partitioner was mutated: %#v", node.Partitioner)
	}
	if got := node.Dependencies[0].Narrow.Mapping; got != NarrowOneToOne {
		t.Fatalf("stored narrow dependency was mutated: %q", got)
	}
	if got := node.Dependencies[1].Shuffle.AggregatorID; got != "sum_amount" {
		t.Fatalf("stored shuffle dependency was mutated: %q", got)
	}
}
