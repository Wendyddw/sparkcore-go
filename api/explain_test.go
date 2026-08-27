package api

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestExplainLineageDoesNotExecute(t *testing.T) {
	runner := &recordingActionRunner{}
	driver := NewContext(nil, runner)
	missingPath := filepath.Join(t.TempDir(), "missing-events.txt")
	source, err := driver.TextFile(missingPath, 2)
	if err != nil {
		t.Fatalf("TextFile() error = %v", err)
	}
	mapped, err := source.Map("unregistered-map")
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}

	explanation, err := mapped.ExplainLineage()
	if err != nil {
		t.Fatalf("ExplainLineage() error = %v", err)
	}
	if !strings.Contains(explanation, missingPath) || !strings.Contains(explanation, "unregistered-map") {
		t.Fatalf("ExplainLineage() = %q, want source path and function ID", explanation)
	}
	if runner.collectCalls != 0 || runner.countCalls != 0 {
		t.Fatalf(
			"ExplainLineage() invoked runner: Collect=%d Count=%d",
			runner.collectCalls,
			runner.countCalls,
		)
	}
}

func TestExplainLineageRejectsRDDWithoutContext(t *testing.T) {
	if _, err := (RDD{}).ExplainLineage(); err == nil {
		t.Fatal("ExplainLineage() error = nil, want missing context error")
	}
}
