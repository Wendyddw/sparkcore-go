package api

import (
	"path/filepath"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestTextFileAddsLazySourceNode(t *testing.T) {
	ctx := NewContext(nil, nil)
	missingPath := filepath.Join(t.TempDir(), "missing.txt")

	rdd, err := ctx.TextFile(missingPath, 4)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	if rdd.ctx != ctx {
		t.Fatal("TextFile() RDD does not reference its creating context")
	}
	if ctx.graph.Len() != 1 {
		t.Fatalf("graph length = %d, want 1", ctx.graph.Len())
	}

	node, ok := ctx.graph.Node(rdd.id)
	if !ok {
		t.Fatalf("graph does not contain RDD %d", rdd.id)
	}
	if node.Name != "TextFile" {
		t.Errorf("node name = %q, want %q", node.Name, "TextFile")
	}
	if node.Operator.Kind != plan.OpSource {
		t.Errorf("operator kind = %q, want %q", node.Operator.Kind, plan.OpSource)
	}
	if node.Operator.SourcePath != missingPath {
		t.Errorf("source path = %q, want %q", node.Operator.SourcePath, missingPath)
	}
	if node.NumPartitions != 4 {
		t.Errorf("partition count = %d, want 4", node.NumPartitions)
	}
	if len(node.Dependencies) != 0 {
		t.Errorf("dependency count = %d, want 0", len(node.Dependencies))
	}
	if node.Partitioner != nil {
		t.Errorf("partitioner = %#v, want nil", node.Partitioner)
	}
}

func TestTextFileRejectsInvalidPartitionCount(t *testing.T) {
	ctx := NewContext(nil, nil)

	if _, err := ctx.TextFile("input.txt", 0); err == nil {
		t.Fatal("TextFile() error = nil, want invalid partition count error")
	}
	if ctx.graph.Len() != 0 {
		t.Fatalf("graph length = %d, want 0", ctx.graph.Len())
	}
}
