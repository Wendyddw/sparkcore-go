package jobspec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestExampleJobFilesMatchSchema(t *testing.T) {
	tests := []struct {
		name              string
		file              string
		action            scheduler.ActionKind
		transformations   int
		lastOperator      plan.OperatorKind
		lastNumPartitions int
	}{
		{
			name:            "count",
			file:            "count.json",
			action:          scheduler.ActionCount,
			transformations: 2,
			lastOperator:    plan.OpFilter,
		},
		{
			name:              "reduce by key",
			file:              "reduce_by_key.json",
			action:            scheduler.ActionCollect,
			transformations:   4,
			lastOperator:      plan.OpReduceByKey,
			lastNumPartitions: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.Open(filepath.Join("..", "examples", test.file))
			if err != nil {
				t.Fatalf("open example: %v", err)
			}
			defer file.Close()

			spec, err := Decode(file)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if spec.Source.NumPartitions != 4 || spec.Action != test.action {
				t.Fatalf("example source/action = (%#v, %q)", spec.Source, spec.Action)
			}
			if len(spec.Transformations) != test.transformations {
				t.Fatalf("transformation count = %d, want %d", len(spec.Transformations), test.transformations)
			}
			last := spec.Transformations[len(spec.Transformations)-1]
			if last.Kind != test.lastOperator || last.NumPartitions != test.lastNumPartitions {
				t.Fatalf("last transformation = %#v", last)
			}
		})
	}
}

func TestCountExampleInputHasKnownNonEmptyLineCount(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "examples", "input.txt"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if got := countNormalizedNonEmptyLines(string(contents)); got != 5 {
		t.Fatalf("normalized non-empty lines = %d, want 5", got)
	}
}

func countNormalizedNonEmptyLines(contents string) int {
	count := 0
	for _, line := range strings.Split(contents, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}
