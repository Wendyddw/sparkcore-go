package jobspec

import (
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestDecodeCountJob(t *testing.T) {
	spec, err := Decode(strings.NewReader(`{
		"source": {"path": "examples/input.txt", "num_partitions": 4},
		"transformations": [
			{"kind": "map", "function_id": "normalize"},
			{"kind": "filter", "function_id": "non_empty"}
		],
		"action": "count"
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if spec.Source.Path != "examples/input.txt" || spec.Source.NumPartitions != 4 {
		t.Fatalf("source = %#v", spec.Source)
	}
	if len(spec.Transformations) != 2 || spec.Transformations[0].Kind != plan.OpMap {
		t.Fatalf("transformations = %#v", spec.Transformations)
	}
	if spec.Action != scheduler.ActionCount {
		t.Fatalf("action = %q, want count", spec.Action)
	}
}

func TestDecodeReduceByKeyJob(t *testing.T) {
	spec, err := Decode(strings.NewReader(`{
		"source": {"path": "missing.txt", "num_partitions": 4},
		"transformations": [
			{"kind": "map_to_pair", "function_id": "to_pair"},
			{"kind": "reduce_by_key", "function_id": "sum", "num_partitions": 2}
		],
		"action": "collect"
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	reduce := spec.Transformations[1]
	if reduce.Kind != plan.OpReduceByKey || reduce.FunctionID != "sum" || reduce.NumPartitions != 2 {
		t.Fatalf("reduce transformation = %#v", reduce)
	}
}

func TestDecodeRejectsInvalidJobs(t *testing.T) {
	tests := []struct {
		name string
		json string
		want string
	}{
		{name: "unknown field", json: `{"source":{"path":"x","num_partitions":1},"action":"count","code":"fn"}`, want: "unknown field"},
		{name: "empty source", json: `{"source":{"path":"","num_partitions":1},"action":"count"}`, want: "source path"},
		{name: "invalid source width", json: `{"source":{"path":"x","num_partitions":0},"action":"count"}`, want: "source partition"},
		{name: "unknown operator", json: `{"source":{"path":"x","num_partitions":1},"transformations":[{"kind":"flat_map","function_id":"f"}],"action":"count"}`, want: "unsupported operator"},
		{name: "missing function", json: `{"source":{"path":"x","num_partitions":1},"transformations":[{"kind":"map"}],"action":"count"}`, want: "function_id"},
		{name: "missing reduce width", json: `{"source":{"path":"x","num_partitions":1},"transformations":[{"kind":"reduce_by_key","function_id":"sum"}],"action":"collect"}`, want: "positive num_partitions"},
		{name: "width on map", json: `{"source":{"path":"x","num_partitions":1},"transformations":[{"kind":"map","function_id":"f","num_partitions":2}],"action":"count"}`, want: "does not accept"},
		{name: "unknown action", json: `{"source":{"path":"x","num_partitions":1},"action":"save"}`, want: "unsupported action"},
		{name: "multiple values", json: `{"source":{"path":"x","num_partitions":1},"action":"count"} {}`, want: "multiple JSON values"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Decode(strings.NewReader(test.json))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Decode() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestDecodeDoesNotReadSourceOrResolveFunctions(t *testing.T) {
	_, err := Decode(strings.NewReader(`{
		"source": {"path": "/definitely/missing/input.txt", "num_partitions": 4},
		"transformations": [{"kind": "map", "function_id": "not_registered"}],
		"action": "count"
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
}
