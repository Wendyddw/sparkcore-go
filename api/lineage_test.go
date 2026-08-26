package api

import (
	"path/filepath"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestFullLineageConstructionIsLazy(t *testing.T) {
	runner := &recordingActionRunner{}
	driver := NewContext(nil, runner)
	missingPath := filepath.Join(t.TempDir(), "missing-events.txt")

	source, err := driver.TextFile(missingPath, 4)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	mapped, err := source.Map("parse-event")
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}
	filtered, err := mapped.Filter("is-relevant")
	if err != nil {
		t.Fatalf("Filter() error = %v", err)
	}
	paired, err := filtered.MapToPair("event-to-pair")
	if err != nil {
		t.Fatalf("MapToPair() error = %v", err)
	}
	values, err := paired.MapValues("normalize-value")
	if err != nil {
		t.Fatalf("MapValues() error = %v", err)
	}
	reduced, err := values.ReduceByKey("sum-values", plan.HashPartitioner(2))
	if err != nil {
		t.Fatalf("ReduceByKey() error = %v", err)
	}

	if runner.collectCalls != 0 || runner.countCalls != 0 {
		t.Fatalf(
			"lineage construction invoked runner: Collect=%d Count=%d",
			runner.collectCalls,
			runner.countCalls,
		)
	}

	nodes := driver.graph.Nodes()
	if len(nodes) != 6 {
		t.Fatalf("lineage node count = %d, want 6", len(nodes))
	}

	wantRDDs := []RDD{source, mapped, filtered, paired, values, reduced}
	wantKinds := []plan.OperatorKind{
		plan.OpSource,
		plan.OpMap,
		plan.OpFilter,
		plan.OpMapToPair,
		plan.OpMapValues,
		plan.OpReduceByKey,
	}
	wantPartitions := []int{4, 4, 4, 4, 4, 2}

	for i, node := range nodes {
		if node.ID != wantRDDs[i].id {
			t.Errorf("node %d ID = %d, want %d", i, node.ID, wantRDDs[i].id)
		}
		if node.Operator.Kind != wantKinds[i] {
			t.Errorf("node %d kind = %q, want %q", i, node.Operator.Kind, wantKinds[i])
		}
		if node.NumPartitions != wantPartitions[i] {
			t.Errorf("node %d partitions = %d, want %d", i, node.NumPartitions, wantPartitions[i])
		}

		if i == 0 {
			if len(node.Dependencies) != 0 {
				t.Errorf("source dependency count = %d, want 0", len(node.Dependencies))
			}
			if node.Operator.SourcePath != missingPath {
				t.Errorf("source path = %q, want %q", node.Operator.SourcePath, missingPath)
			}
			continue
		}

		if len(node.Dependencies) != 1 {
			t.Fatalf("node %d dependency count = %d, want 1", i, len(node.Dependencies))
		}
		if node.Dependencies[0].ParentID != nodes[i-1].ID {
			t.Errorf(
				"node %d parent = %d, want %d",
				i,
				node.Dependencies[0].ParentID,
				nodes[i-1].ID,
			)
		}
	}

	for i := 1; i < len(nodes)-1; i++ {
		if nodes[i].Dependencies[0].Kind != plan.DependencyNarrow {
			t.Errorf("node %d dependency = %q, want narrow", i, nodes[i].Dependencies[0].Kind)
		}
	}

	reduceNode := nodes[len(nodes)-1]
	if reduceNode.Dependencies[0].Kind != plan.DependencyShuffle {
		t.Errorf("reduce dependency = %q, want shuffle", reduceNode.Dependencies[0].Kind)
	}
	wantPartitioner := plan.HashPartitioner(2)
	if reduceNode.Partitioner == nil || *reduceNode.Partitioner != wantPartitioner {
		t.Errorf("reduce partitioner = %#v, want %#v", reduceNode.Partitioner, wantPartitioner)
	}
}
