package protocol_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestWorkerMessagesPreserveV1JSONContract(t *testing.T) {
	tests := []struct {
		name  string
		value any
		json  string
	}{
		{"registration", protocol.RegisterWorkerRequest{WorkerID: "worker-a", TotalSlots: 2}, `{"worker_id":"worker-a","total_slots":2}`},
		{"registration with optional URL", protocol.RegisterWorkerRequest{WorkerID: "worker-a", TotalSlots: 2, BaseURL: "http://localhost:9001"}, `{"worker_id":"worker-a","total_slots":2,"base_url":"http://localhost:9001"}`},
		{"registration response", protocol.RegisterWorkerResponse{WorkerID: "worker-a", TotalSlots: 2}, `{"worker_id":"worker-a","total_slots":2}`},
		{"heartbeat", protocol.HeartbeatRequest{WorkerID: "worker-a", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{7}}, `{"worker_id":"worker-a","free_slots":1,"running_attempt_ids":[7]}`},
		{"worker at capacity", protocol.HeartbeatRequest{WorkerID: "worker-a", FreeSlots: 0, RunningAttemptIDs: []plan.TaskAttemptID{7, 8}}, `{"worker_id":"worker-a","free_slots":0,"running_attempt_ids":[7,8]}`},
		{"idle worker", protocol.HeartbeatRequest{WorkerID: "worker-a", FreeSlots: 2, RunningAttemptIDs: []plan.TaskAttemptID{}}, `{"worker_id":"worker-a","free_slots":2,"running_attempt_ids":[]}`},
		{"no assignments", protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{}}, `{"assignments":[]}`},
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

func TestHeartbeatAssignmentPreservesIdentityAndNarrowPipeline(t *testing.T) {
	const largeAttemptID plan.TaskAttemptID = 9007199254740993
	assignment := protocol.TaskAssignment{
		JobID: 10, StageID: 2, WorkerID: "worker-a",
		Attempt: scheduler.TaskAttemptIdentity{ID: largeAttemptID, TaskID: 8, StageAttemptID: 3},
		Task: scheduler.Task{ID: 8, StageID: 2, StageKind: scheduler.StageResult, PartitionID: 1, NumPartitions: 4,
			Operations: []scheduler.StageOperation{
				{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 0, Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "/data/input.txt"}}},
				{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 1, Operator: plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "normalize"}}},
				{Kind: scheduler.StageOperationRDD, RDD: &scheduler.RDDOperationSpec{RDDID: 2, Operator: plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "non_empty"}}},
			}, FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCount, TargetRDD: 2}},
	}
	response := protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{assignment}}
	data, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"assignments":[{"job_id":10,"stage_id":2,"worker_id":"worker-a","attempt":{"id":9007199254740993,"task_id":8,"stage_attempt_id":3},"task":{"id":8,"stage_id":2,"stage_kind":"result","partition_id":1,"num_partitions":4,"operations":[{"kind":"rdd","rdd":{"rdd_id":0,"operator":{"kind":"source","source_path":"/data/input.txt"}}},{"kind":"rdd","rdd":{"rdd_id":1,"operator":{"kind":"map","function_id":"normalize"}}},{"kind":"rdd","rdd":{"rdd_id":2,"operator":{"kind":"filter","function_id":"non_empty"}}}],"final_action":{"kind":"count","target_rdd":2}}}]}`
	if string(data) != want {
		t.Fatalf("assignment JSON = %s, want %s", data, want)
	}
	var decoded protocol.HeartbeatResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, response) {
		t.Fatalf("round trip = %#v, want %#v", decoded, response)
	}
}

func TestAssignmentPreservesZeroBasedIDs(t *testing.T) {
	assignment := protocol.TaskAssignment{WorkerID: "worker-a", Task: scheduler.Task{StageKind: scheduler.StageResult, NumPartitions: 1}}
	data, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"job_id", "stage_id"} {
		if string(fields[key]) != "0" {
			t.Fatalf("%s = %s, want explicit zero", key, fields[key])
		}
	}
	var identity map[string]json.RawMessage
	if err := json.Unmarshal(fields["attempt"], &identity); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"id", "task_id", "stage_attempt_id"} {
		if string(identity[key]) != "0" {
			t.Fatalf("%s = %s, want explicit zero", key, identity[key])
		}
	}
}
