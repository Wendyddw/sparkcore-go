package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunExplainsShuffleJobWithoutReadingSource(t *testing.T) {
	jobPath := filepath.Join("..", "..", "examples", "reduce_by_key.json")
	var output bytes.Buffer
	if err := run([]string{"-job", jobPath}, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}

	explanation := output.String()
	for _, want := range []string{
		"== RDD Lineage ==",
		"source=\"examples/missing-input.txt\"",
		"== Stage DAG ==",
		"Stage 0 kind=shuffle_map partitions=4 parents=[] tasks=4",
		"Stage 1 kind=result partitions=2 parents=[0] tasks=2",
		"Action: collect",
	} {
		if !strings.Contains(explanation, want) {
			t.Fatalf("explanation =\n%s\nwant %q", explanation, want)
		}
	}
}

func TestRunRequiresJobFlag(t *testing.T) {
	if err := run(nil, &bytes.Buffer{}); err == nil {
		t.Fatal("run() error = nil, want missing job flag")
	}
}
