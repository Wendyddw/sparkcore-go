package protocol_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/jobspec"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestResultMessagesPreserveV1JSONContract(t *testing.T) {
	identity := scheduler.TaskAttemptIdentity{ID: 12, TaskID: 8, StageAttemptID: 3}
	tests := []struct {
		name  string
		value any
		json  string
	}{
		{"task success", protocol.TaskSuccessRequest{JobID: 5, StageID: 2, Attempt: identity, PartitionID: 1, WorkerID: "worker-a", Output: protocol.TaskOutput{Count: 4}}, `{"job_id":5,"stage_id":2,"attempt":{"id":12,"task_id":8,"stage_attempt_id":3},"partition_id":1,"worker_id":"worker-a","output":{"records":null,"count":4}}`},
		{"task failure", protocol.TaskFailureRequest{JobID: 5, StageID: 2, Attempt: identity, PartitionID: 1, WorkerID: "worker-a", Error: "unknown function: normalize"}, `{"job_id":5,"stage_id":2,"attempt":{"id":12,"task_id":8,"stage_attempt_id":3},"partition_id":1,"worker_id":"worker-a","error":"unknown function: normalize"}`},
		{"acknowledgment", protocol.TaskReportResponse{Acknowledged: true}, `{"acknowledged":true}`},
		{"zero count", protocol.JobResultResponse{Action: scheduler.ActionCount, Count: 0}, `{"action":"count","records":null,"count":0}`},
		{"empty collect", protocol.JobResultResponse{Action: scheduler.ActionCollect, Records: []json.RawMessage{}}, `{"action":"collect","records":[],"count":0}`},
		{"structured error", protocol.ErrorResponse{Code: protocol.CodeInvalidRequest, Message: "free_slots must not be negative"}, `{"code":"invalid_request","message":"free_slots must not be negative"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := json.Marshal(test.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != test.json {
				t.Fatalf("JSON = %s, want %s", data, test.json)
			}
			decoded := reflect.New(reflect.TypeOf(test.value))
			if err := json.Unmarshal(data, decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded.Elem().Interface(), test.value) {
				t.Fatalf("round trip = %#v, want %#v", decoded.Elem().Interface(), test.value)
			}
		})
	}
}

func TestResultRecordsPreserveJSONValuesAndIntegerPrecision(t *testing.T) {
	records := []json.RawMessage{
		json.RawMessage(`"alpha"`), json.RawMessage(`9007199254740993`), json.RawMessage(`true`), json.RawMessage(`null`),
		json.RawMessage(`{"key":"total","value":9223372036854775807}`), json.RawMessage(`[1,"nested",{"enabled":false}]`),
	}
	const count int64 = 9223372036854775807
	task := protocol.TaskSuccessRequest{WorkerID: "worker-a", Output: protocol.TaskOutput{Records: records, Count: count}}
	taskJSON, err := json.Marshal(task)
	if err != nil {
		t.Fatal(err)
	}
	var decodedTask protocol.TaskSuccessRequest
	if err := json.Unmarshal(taskJSON, &decodedTask); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(task, decodedTask) {
		t.Fatalf("task report changed during round trip: %#v", decodedTask)
	}
	job := protocol.JobResultResponse{Action: scheduler.ActionCollect, Records: decodedTask.Output.Records}
	jobJSON, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var decodedJob protocol.JobResultResponse
	if err := json.Unmarshal(jobJSON, &decodedJob); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decodedJob.Records, records) {
		t.Fatalf("job records = %s, want %s", decodedJob.Records, records)
	}
	// Count results must also avoid decoding through untyped floating-point numbers.
	countJSON, err := json.Marshal(protocol.JobResultResponse{Action: scheduler.ActionCount, Count: count})
	if err != nil {
		t.Fatal(err)
	}
	var decodedCount protocol.JobResultResponse
	if err := json.Unmarshal(countJSON, &decodedCount); err != nil {
		t.Fatal(err)
	}
	if decodedCount.Count != count {
		t.Fatalf("count = %d, want %d", decodedCount.Count, count)
	}
}

func TestSubmitJobRequestReusesJobSpecWithoutEnvelope(t *testing.T) {
	spec := jobspec.Spec{Source: jobspec.SourceSpec{Path: "input.txt", NumPartitions: 4},
		Transformations: []jobspec.TransformationSpec{{Kind: "map", FunctionID: "normalize"}, {Kind: "filter", FunctionID: "non_empty"}}, Action: scheduler.ActionCount}
	var request protocol.SubmitJobRequest = spec
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"source":{"path":"input.txt","num_partitions":4},"transformations":[{"kind":"map","function_id":"normalize"},{"kind":"filter","function_id":"non_empty"}],"action":"count"}`
	if string(data) != want {
		t.Fatalf("request = %s, want %s", data, want)
	}
	var decoded protocol.SubmitJobRequest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, spec) {
		t.Fatalf("request changed during round trip: %#v", decoded)
	}
}
