package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRunExecutesCountJob(t *testing.T) {
	inputPath, err := filepath.Abs(filepath.Join("..", "..", "examples", "input.txt"))
	if err != nil {
		t.Fatalf("Abs() error = %v", err)
	}
	jobPath := filepath.Join(t.TempDir(), "count.json")
	job := fmt.Sprintf(`{
		"source":{"path":%q,"num_partitions":4},
		"transformations":[
			{"kind":"map","function_id":"normalize"},
			{"kind":"filter","function_id":"non_empty"}
		],
		"action":"count"
	}`, inputPath)
	if err := os.WriteFile(jobPath, []byte(job), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	var output bytes.Buffer
	if err := run(context.Background(), []string{"-job", jobPath}, &output); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if output.String() != "5\n" {
		t.Fatalf("output = %q, want %q", output.String(), "5\n")
	}
}

func TestRunRequiresJobFlag(t *testing.T) {
	if err := run(context.Background(), nil, &bytes.Buffer{}); err == nil {
		t.Fatal("run() error = nil, want missing job flag")
	}
}
