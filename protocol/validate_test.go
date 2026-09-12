package protocol_test

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func validAssignment() protocol.TaskAssignment {
	return protocol.TaskAssignment{WorkerID: "a", Task: scheduler.Task{
		StageKind: scheduler.StageResult, NumPartitions: 4,
		Operations: []scheduler.StageOperation{
			{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 0, Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "missing-source.txt"}}},
			{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 1, Operator: plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "not-registered-here"}}},
		}, FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCount, TargetRDD: 1},
	}}
}

func TestMessageValidationRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		message protocol.Message
	}{
		{"empty worker", protocol.RegisterWorkerRequest{TotalSlots: 1}},
		{"blank worker", protocol.RegisterWorkerRequest{WorkerID: " \t", TotalSlots: 1}},
		{"zero capacity", protocol.RegisterWorkerRequest{WorkerID: "a"}},
		{"negative capacity", protocol.RegisterWorkerResponse{WorkerID: "a", TotalSlots: -1}},
		{"negative free", protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: -1, RunningAttemptIDs: []plan.TaskAttemptID{}}},
		{"null running", protocol.HeartbeatRequest{WorkerID: "a"}},
		{"duplicate running", protocol.HeartbeatRequest{WorkerID: "a", RunningAttemptIDs: []plan.TaskAttemptID{0, 0}}},
		{"null assignments", protocol.HeartbeatResponse{}},
		{"success negative partition", protocol.TaskSuccessRequest{WorkerID: "a", PartitionID: -1}},
		{"success empty worker", protocol.TaskSuccessRequest{}},
		{"failure negative partition", protocol.TaskFailureRequest{WorkerID: "a", PartitionID: -1, Error: "failed"}},
		{"failure empty worker", protocol.TaskFailureRequest{Error: "failed"}},
		{"blank failure", protocol.TaskFailureRequest{WorkerID: "a", Error: " \n"}},
		{"negative count", protocol.TaskOutput{Count: -1}},
		{"mixed output", protocol.TaskOutput{Count: 1, Records: []json.RawMessage{json.RawMessage(`1`)}}},
		{"malformed record", protocol.TaskOutput{Records: []json.RawMessage{json.RawMessage(`{`)}}},
		{"multiple record values", protocol.TaskOutput{Records: []json.RawMessage{json.RawMessage(`1 2`)}}},
		{"negative acknowledgment", protocol.TaskReportResponse{}},
		{"unknown result action", protocol.JobResultResponse{Action: "sum"}},
		{"count contains array", protocol.JobResultResponse{Action: scheduler.ActionCount, Records: []json.RawMessage{}}},
		{"collect null records", protocol.JobResultResponse{Action: scheduler.ActionCollect}},
		{"collect nonzero count", protocol.JobResultResponse{Action: scheduler.ActionCollect, Records: []json.RawMessage{}, Count: 1}},
		{"unknown error code", protocol.ErrorResponse{Code: "unknown", Message: "failed"}},
		{"blank error message", protocol.ErrorResponse{Code: protocol.CodeInternal, Message: " "}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.message.Validate(); err == nil {
				t.Fatal("invalid message accepted")
			}
		})
	}
}

func TestRegistrationURLValidation(t *testing.T) {
	for _, baseURL := range []string{"", "http://localhost:9001", "https://workers.example/base"} {
		if err := (protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 2, BaseURL: baseURL}).Validate(); err != nil {
			t.Fatalf("URL %q: %v", baseURL, err)
		}
	}
	for _, baseURL := range []string{"localhost:9001", "/worker", "ftp://worker", "http:///path", "http://user:secret@worker", "http://worker?x=1", "http://worker#fragment", "://"} {
		if err := (protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 2, BaseURL: baseURL}).Validate(); err == nil {
			t.Fatalf("invalid URL %q accepted", baseURL)
		}
	}
}

func TestHeartbeatCapacityValidation(t *testing.T) {
	for _, test := range []struct {
		free, total int
		running     []plan.TaskAttemptID
		valid       bool
	}{
		{2, 2, []plan.TaskAttemptID{}, true}, {0, 2, []plan.TaskAttemptID{0, 1}, true}, {1, 2, []plan.TaskAttemptID{}, true},
		{-1, 2, []plan.TaskAttemptID{}, false}, {3, 2, []plan.TaskAttemptID{}, false}, {2, 2, []plan.TaskAttemptID{0}, false},
		{0, 0, []plan.TaskAttemptID{}, false}, {0, -1, []plan.TaskAttemptID{}, false}, {0, 2, []plan.TaskAttemptID{0, 0}, false},
		{int(^uint(0) >> 1), int(^uint(0) >> 1), []plan.TaskAttemptID{0}, false},
	} {
		request := protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: test.free, RunningAttemptIDs: test.running}
		err := request.ValidateCapacity(test.total)
		if (err == nil) != test.valid {
			t.Fatalf("request %#v total %d: %v", request, test.total, err)
		}
	}
}

func TestAssignmentValidationRejectsInvalidPipelines(t *testing.T) {
	tests := []struct {
		name   string
		change func(*protocol.TaskAssignment)
	}{
		{"worker", func(a *protocol.TaskAssignment) { a.WorkerID = "" }},
		{"task identity", func(a *protocol.TaskAssignment) { a.Attempt.TaskID++ }},
		{"stage identity", func(a *protocol.TaskAssignment) { a.StageID++ }},
		{"negative partition", func(a *protocol.TaskAssignment) { a.Task.PartitionID = -1 }},
		{"partition out of range", func(a *protocol.TaskAssignment) { a.Task.PartitionID = 4 }},
		{"zero width", func(a *protocol.TaskAssignment) { a.Task.NumPartitions = 0 }},
		{"shuffle stage", func(a *protocol.TaskAssignment) { a.Task.StageKind = scheduler.StageShuffleMap }},
		{"shuffle write", func(a *protocol.TaskAssignment) { a.Task.ShuffleWrite = &scheduler.ShuffleWriteSpec{} }},
		{"no action", func(a *protocol.TaskAssignment) { a.Task.FinalAction = nil }},
		{"unknown action", func(a *protocol.TaskAssignment) { a.Task.FinalAction.Kind = "sum" }},
		{"wrong target", func(a *protocol.TaskAssignment) { a.Task.FinalAction.TargetRDD = 99 }},
		{"empty pipeline", func(a *protocol.TaskAssignment) { a.Task.Operations = nil }},
		{"shuffle read", func(a *protocol.TaskAssignment) { a.Task.Operations[0].Kind = scheduler.StageOperationShuffleRead }},
		{"ambiguous operation", func(a *protocol.TaskAssignment) { a.Task.Operations[0].ShuffleRead = &scheduler.ShuffleReadSpec{} }},
		{"missing RDD", func(a *protocol.TaskAssignment) { a.Task.Operations[0].RDD = nil }},
		{"duplicate RDD", func(a *protocol.TaskAssignment) { a.Task.Operations[1].RDD.RDDID = 0 }},
		{"missing source", func(a *protocol.TaskAssignment) { a.Task.Operations[0].RDD.Operator.Kind = plan.OpMap }},
		{"empty path", func(a *protocol.TaskAssignment) { a.Task.Operations[0].RDD.Operator.SourcePath = "" }},
		{"source function", func(a *protocol.TaskAssignment) { a.Task.Operations[0].RDD.Operator.FunctionID = "unexpected" }},
		{"missing function", func(a *protocol.TaskAssignment) { a.Task.Operations[1].RDD.Operator.FunctionID = "" }},
		{"transform path", func(a *protocol.TaskAssignment) { a.Task.Operations[1].RDD.Operator.SourcePath = "unexpected" }},
		{"second source", func(a *protocol.TaskAssignment) { a.Task.Operations[1].RDD.Operator.Kind = plan.OpSource }},
		{"shuffle operator", func(a *protocol.TaskAssignment) { a.Task.Operations[1].RDD.Operator.Kind = plan.OpReduceByKey }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := validAssignment()
			test.change(&a)
			if err := a.Validate(); err == nil {
				t.Fatal("invalid assignment accepted")
			}
		})
	}
	// Schema validation must not open files or resolve named functions.
	for _, kind := range []plan.OperatorKind{plan.OpMap, plan.OpFilter, plan.OpMapToPair, plan.OpMapValues} {
		a := validAssignment()
		a.Task.Operations[1].RDD.Operator.Kind = kind
		if err := a.Validate(); err != nil {
			t.Fatalf("narrow kind %q: %v", kind, err)
		}
	}
}

func TestHeartbeatResponseRejectsDuplicateAndMixedAssignments(t *testing.T) {
	first := validAssignment()
	for _, change := range []func(*protocol.TaskAssignment){
		func(a *protocol.TaskAssignment) { a.Task.PartitionID = 1; a.Task.ID = 1; a.Attempt.TaskID = 1 }, // Same physical attempt ID.
		func(a *protocol.TaskAssignment) { a.Attempt.ID = 1 },                                            // Same logical task.
		func(a *protocol.TaskAssignment) {
			a.WorkerID = "b"
			a.Attempt.ID = 1
			a.Task.ID = 1
			a.Attempt.TaskID = 1
		},
	} {
		second := validAssignment()
		change(&second)
		if err := (protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{first, second}}).Validate(); err == nil {
			t.Fatal("invalid assignment batch accepted")
		}
	}
}

func TestDecodeAndValidateSupportsEveryMessageAndExistingJobFiles(t *testing.T) {
	assertDecodes(t, protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 2})
	assertDecodes(t, protocol.RegisterWorkerResponse{WorkerID: "a", TotalSlots: 2})
	assertDecodes(t, protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 2, RunningAttemptIDs: []plan.TaskAttemptID{}})
	assertDecodes(t, protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}})
	assertDecodes(t, protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{validAssignment()}})
	assertDecodes(t, validAssignment())
	assertDecodes(t, protocol.TaskSuccessRequest{WorkerID: "a", Output: protocol.TaskOutput{Count: 5}})
	assertDecodes(t, protocol.TaskFailureRequest{WorkerID: "a", Error: "failed"})
	assertDecodes(t, protocol.TaskReportResponse{Acknowledged: true})
	assertDecodes(t, protocol.JobResultResponse{Action: scheduler.ActionCount, Count: 0})
	assertDecodes(t, protocol.JobResultResponse{Action: scheduler.ActionCollect, Records: []json.RawMessage{}})
	assertDecodes(t, protocol.TaskOutput{Records: []json.RawMessage{json.RawMessage(`null`)}})
	assertDecodes(t, protocol.ErrorResponse{Code: protocol.CodeInternal, Message: "failed"})
	assertDecodes(t, protocol.SubmitJobRequest{Source: jobspec.SourceSpec{Path: "missing-input.txt", NumPartitions: 4}, Action: scheduler.ActionCount})
	for _, path := range []string{"../examples/count.json", "../examples/reduce_by_key.json"} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := protocol.DecodeAndValidate[protocol.SubmitJobRequest](bytes.NewReader(data), 1<<20); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	// Reusing jobspec.Spec also reuses its semantic validation.
	if _, err := protocol.DecodeAndValidate[protocol.SubmitJobRequest](strings.NewReader(`{"source":{"path":"input","num_partitions":0},"action":"count"}`), 1024); err == nil {
		t.Fatal("invalid job accepted")
	}
}
func assertDecodes[T protocol.Message](t *testing.T, value T) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	got, err := protocol.DecodeAndValidate[T](bytes.NewReader(data), 1<<20)
	if err != nil {
		t.Fatalf("decode %T: %v", value, err)
	}
	if !reflect.DeepEqual(got, value) {
		t.Fatalf("decode %T changed value: %#v", value, got)
	}
}
