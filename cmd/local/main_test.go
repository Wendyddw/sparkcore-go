package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
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
	if err := run(context.Background(), []string{"-job", jobPath}, &output, io.Discard); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if output.String() != "5\n" {
		t.Fatalf("output = %q, want %q", output.String(), "5\n")
	}
}

func TestRunRequiresJobFlag(t *testing.T) {
	if err := run(context.Background(), nil, &bytes.Buffer{}, io.Discard); err == nil {
		t.Fatal("run() error = nil, want missing job flag")
	}
}

func TestRunShuffleExample(t *testing.T) {
	data, err := os.ReadFile("../../examples/reduce_by_key.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec jobspec.Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatal(err)
	}
	spec.Source.Path, err = filepath.Abs("../../examples/input.txt")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for _, action := range []scheduler.ActionKind{scheduler.ActionCollect, scheduler.ActionCount} {
		spec.Action = action
		data, err = json.Marshal(spec)
		if err != nil {
			t.Fatal(err)
		}
		job := filepath.Join(t.TempDir(), "job.json")
		if err := os.WriteFile(job, data, 0600); err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		if err := run(context.Background(), []string{"--job", job, "--shuffle-root", root, "--max-stage-attempts", "1"}, &output, io.Discard); err != nil {
			t.Fatal(err)
		}
		if action == scheduler.ActionCount {
			if output.String() != "3\n" {
				t.Fatal(output.String())
			}
		} else {
			var records []json.RawMessage
			if err := json.Unmarshal(output.Bytes(), &records); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, r := range records {
				got = append(got, string(r))
			}
			sort.Strings(got)
			want := []string{`{"key":"alpha","value":2}`, `{"key":"beta","value":2}`, `{"key":"gamma","value":1}`}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("collect = %v", got)
			}
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 2 {
		t.Fatalf("run namespaces = %v, %v", entries, err)
	}
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"--job", "../../examples/count.json", "--shuffle-root", "relative"},
		{"--job", "unused", "--max-stage-attempts", "0"},
		{"--job", "unused", "--max-stage-attempts", "-1"}, {"--job", "unused", "extra"},
	} {
		if err := run(context.Background(), args, io.Discard, io.Discard); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	var help bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, io.Discard, &help); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	for _, text := range []string{"shuffle-root", "max-stage-attempts", "1 disables shuffle recovery"} {
		if !strings.Contains(help.String(), text) {
			t.Fatalf("missing %q in help: %s", text, &help)
		}
	}
}
