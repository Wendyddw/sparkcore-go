package plan

import "testing"

func TestExplainLineageRendersNarrowLineage(t *testing.T) {
	graph := NewRDDGraph()
	source := addExplainNode(t, graph, RDDNode{
		Name:          "TextFile",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "missing-events.txt"},
		NumPartitions: 4,
	})
	mapped := addExplainNode(t, graph, RDDNode{
		Name:          "Map",
		Operator:      OperatorSpec{Kind: OpMap, FunctionID: "parse-event"},
		Dependencies:  []Dependency{explainNarrowDependency(source)},
		NumPartitions: 4,
	})
	filtered := addExplainNode(t, graph, RDDNode{
		Name:          "Filter",
		Operator:      OperatorSpec{Kind: OpFilter, FunctionID: "is-relevant"},
		Dependencies:  []Dependency{explainNarrowDependency(mapped)},
		NumPartitions: 4,
	})

	got, err := graph.ExplainLineage(filtered)
	if err != nil {
		t.Fatalf("ExplainLineage() error = %v", err)
	}
	want := "" +
		"== RDD Lineage ==\n" +
		"RDD 0 TextFile[4] operator=source source=\"missing-events.txt\"\n" +
		"RDD 1 Map[4] operator=map function=\"parse-event\" narrow <- RDD 0\n" +
		"RDD 2 Filter[4] operator=filter function=\"is-relevant\" narrow <- RDD 1\n"
	if got != want {
		t.Fatalf("ExplainLineage() =\n%s\nwant:\n%s", got, want)
	}
}

func TestExplainLineageRendersShuffleLineage(t *testing.T) {
	graph := NewRDDGraph()
	source := addExplainNode(t, graph, RDDNode{
		Name:          "TextFile",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "events.txt"},
		NumPartitions: 4,
	})
	paired := addExplainNode(t, graph, RDDNode{
		Name:          "MapToPair",
		Operator:      OperatorSpec{Kind: OpMapToPair, FunctionID: "to-pair"},
		Dependencies:  []Dependency{explainNarrowDependency(source)},
		NumPartitions: 4,
	})
	partitioner := HashPartitioner(2)
	reduced := addExplainNode(t, graph, RDDNode{
		Name:          "ReduceByKey",
		Operator:      OperatorSpec{Kind: OpReduceByKey, FunctionID: "sum"},
		NumPartitions: 2,
		Partitioner:   &partitioner,
		Dependencies: []Dependency{{
			Kind:     DependencyShuffle,
			ParentID: paired,
			Shuffle: &ShuffleDependencySpec{
				ShuffleID:      3,
				Partitioner:    partitioner,
				AggregatorID:   "sum",
				MapSideCombine: true,
			},
		}},
	})

	got, err := graph.ExplainLineage(reduced)
	if err != nil {
		t.Fatalf("ExplainLineage() error = %v", err)
	}
	want := "" +
		"== RDD Lineage ==\n" +
		"RDD 0 TextFile[4] operator=source source=\"events.txt\"\n" +
		"RDD 1 MapToPair[4] operator=map_to_pair function=\"to-pair\" narrow <- RDD 0\n" +
		"RDD 2 ReduceByKey[2] operator=reduce_by_key function=\"sum\" partitioner=hash[2] " +
		"shuffle <- RDD 1 shuffle=3 shuffle_partitioner=hash[2] aggregator=\"sum\" map_side_combine=true\n"
	if got != want {
		t.Fatalf("ExplainLineage() =\n%s\nwant:\n%s", got, want)
	}
}

func TestExplainLineageIncludesOnlyTargetAncestors(t *testing.T) {
	graph := NewRDDGraph()
	target := addExplainNode(t, graph, RDDNode{
		Name:          "TextFile",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "target.txt"},
		NumPartitions: 1,
	})
	addExplainNode(t, graph, RDDNode{
		Name:          "TextFile",
		Operator:      OperatorSpec{Kind: OpSource, SourcePath: "unrelated.txt"},
		NumPartitions: 1,
	})

	got, err := graph.ExplainLineage(target)
	if err != nil {
		t.Fatalf("ExplainLineage() error = %v", err)
	}
	want := "== RDD Lineage ==\nRDD 0 TextFile[1] operator=source source=\"target.txt\"\n"
	if got != want {
		t.Fatalf("ExplainLineage() =\n%s\nwant:\n%s", got, want)
	}
}

func addExplainNode(t *testing.T, graph *RDDGraph, node RDDNode) RDDID {
	t.Helper()
	id, err := graph.AddNode(node)
	if err != nil {
		t.Fatalf("AddNode(%s) error = %v", node.Name, err)
	}
	return id
}

func explainNarrowDependency(parent RDDID) Dependency {
	return Dependency{
		Kind:     DependencyNarrow,
		ParentID: parent,
		Narrow:   &NarrowDependencySpec{Mapping: NarrowOneToOne},
	}
}
