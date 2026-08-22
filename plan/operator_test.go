package plan

import (
	"encoding/json"
	"testing"
)

func TestOperatorSpecJSONRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		spec     OperatorSpec
		wantJSON string
	}{
		{
			name: "source",
			spec: OperatorSpec{
				Kind:       OpSource,
				SourcePath: "events.txt",
			},
			wantJSON: `{"kind":"source","source_path":"events.txt"}`,
		},
		{
			name: "registered function",
			spec: OperatorSpec{
				Kind:       OpMap,
				FunctionID: "parse_event",
			},
			wantJSON: `{"kind":"map","function_id":"parse_event"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded, err := json.Marshal(tt.spec)
			if err != nil {
				t.Fatalf("marshal OperatorSpec: %v", err)
			}
			if string(encoded) != tt.wantJSON {
				t.Fatalf("marshal OperatorSpec = %s, want %s", encoded, tt.wantJSON)
			}

			var decoded OperatorSpec
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("unmarshal OperatorSpec: %v", err)
			}
			if decoded != tt.spec {
				t.Fatalf("round trip OperatorSpec = %#v, want %#v", decoded, tt.spec)
			}
		})
	}
}
