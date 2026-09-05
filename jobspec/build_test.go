package jobspec

import (
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/api"
)

func TestBuildRecordsOrderedLazyLineage(t *testing.T) {
	spec, err := Decode(strings.NewReader(`{
		"source":{"path":"missing.txt","num_partitions":4},
		"transformations":[
			{"kind":"map","function_id":"normalize"},
			{"kind":"filter","function_id":"non_empty"}
		],
		"action":"count"
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	rdd, err := Build(api.NewContext(nil, nil), spec)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	explanation, err := rdd.ExplainLineage()
	if err != nil {
		t.Fatalf("ExplainLineage() error = %v", err)
	}
	for _, want := range []string{"source=\"missing.txt\"", "function=\"normalize\"", "function=\"non_empty\""} {
		if !strings.Contains(explanation, want) {
			t.Fatalf("explanation =\n%s\nwant %q", explanation, want)
		}
	}
}
