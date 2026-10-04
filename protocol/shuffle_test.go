package protocol_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func shuffleAssignment() (protocol.TaskAssignment, shuffle.MapOutput) {
	a := validAssignment()
	a.Task.StageKind = scheduler.StageShuffleMap
	a.Task.NumPartitions = 1
	a.Task.FinalAction = nil
	a.Task.ShuffleWrite = &scheduler.ShuffleWriteSpec{Partitioner: plan.HashPartitioner(1), AggregatorID: "sum", MapSideCombine: true}
	a.Task.Operations[1].RDD.Operator.Kind = plan.OpMapToPair
	m := shuffle.MapOutput{Version: 1, Attempt: shuffle.AttemptIdentity{RunID: a.RunID}, NumMapPartitions: 1, NumReducePartitions: 1,
		Buckets: []shuffle.BucketMetadata{{SHA256: fmt.Sprintf("%x", sha256.Sum256(nil))}}}
	return a, m
}

func shuffleReduceAssignment() protocol.TaskAssignment {
	a, m := shuffleAssignment()
	a.StageID = 1
	a.Task.StageID = 1
	a.Task.StageKind = scheduler.StageResult
	a.Task.ShuffleWrite = nil
	a.Task.FinalAction = &scheduler.ActionSpec{Kind: scheduler.ActionCollect, TargetRDD: 1}
	a.Task.Operations[0] = scheduler.StageOperation{Kind: scheduler.StageOperationShuffleRead, ShuffleRead: &scheduler.ShuffleReadSpec{Partitioner: plan.HashPartitioner(1)}}
	a.Task.Operations[1].RDD.Operator = plan.OperatorSpec{Kind: plan.OpReduceByKey, FunctionID: "sum"}
	a.ShuffleInputs = &shuffle.InputSnapshot{RunID: a.RunID, NumMapPartitions: 1, NumReducePartitions: 1, Outputs: []shuffle.MapOutput{m}}
	return a
}

func TestShuffleWireRoundTripsAndStrictFields(t *testing.T) {
	a, m := shuffleAssignment()
	assertDecodes(t, a)
	r := shuffleReduceAssignment()
	assertDecodes(t, r)
	assertDecodes(t, protocol.TaskSuccessRequest{WorkerID: "a", Output: protocol.TaskOutput{ShuffleOutput: &m}})
	failure := protocol.TaskFailureRequest{Kind: scheduler.FailureShuffleInput, WorkerID: "a", Error: "missing bucket", ShuffleInput: &shuffle.InputReference{Attempt: m.Attempt}}
	assertDecodes(t, failure)
	data, _ := json.Marshal(r)
	for _, bad := range []string{
		strings.Replace(string(data), `,"map_partition_id":0`, "", 1),
		strings.Replace(string(data), `"map_partition_id":0`, `"map_partition_id":null`, 1),
		strings.Replace(string(data), `"num_map_partitions":1`, `"num_map_partitions":1,"num_map_partitions":1`, 1),
		strings.Replace(string(data), `"sha256":`, `"unexpected":0,"sha256":`, 1),
	} {
		if _, err := protocol.DecodeAndValidate[protocol.TaskAssignment](strings.NewReader(bad), 1<<20); !errors.Is(err, protocol.ErrInvalidMessage) {
			t.Fatalf("accepted malformed snapshot: %s: %v", bad, err)
		}
	}
	if _, err := protocol.DecodeAndValidate[protocol.TaskAssignment](bytes.NewReader(data), int64(len(data)-1)); !errors.Is(err, protocol.ErrMessageTooLarge) {
		t.Fatalf("size bound: %v", err)
	}
	failureJSON, _ := json.Marshal(failure)
	for _, bad := range []string{
		strings.Replace(string(failureJSON), `"kind":"shuffle_input",`, "", 1),
		strings.Replace(string(failureJSON), `"kind":"shuffle_input"`, `"kind":"worker_lost"`, 1),
	} {
		if _, err := protocol.DecodeAndValidate[protocol.TaskFailureRequest](strings.NewReader(bad), 1<<20); err == nil {
			t.Fatal("accepted missing/scheduler-only failure kind")
		}
	}
	copy := r.Execution()
	copy.ShuffleInputs.Outputs[0].Buckets[0].SHA256 = "mutated"
	if err := r.Validate(); err != nil {
		t.Fatalf("execution aliases wire snapshot: %v", err)
	}
}

func TestShuffleWireRejectsContradictoryForms(t *testing.T) {
	for name, change := range map[string]func(*protocol.TaskAssignment){
		"missing snapshot":  func(a *protocol.TaskAssignment) { a.ShuffleInputs = nil },
		"wrong job":         func(a *protocol.TaskAssignment) { a.JobID++ },
		"wrong shuffle":     func(a *protocol.TaskAssignment) { a.Task.Operations[0].ShuffleRead.ShuffleID++ },
		"missing reduction": func(a *protocol.TaskAssignment) { a.Task.Operations[1].RDD.Operator.Kind = plan.OpMap },
		"map reads shuffle": func(a *protocol.TaskAssignment) { a.Task.StageKind = scheduler.StageShuffleMap },
		"mixed write":       func(a *protocol.TaskAssignment) { a.Task.ShuffleWrite = &scheduler.ShuffleWriteSpec{} },
		"wrong width":       func(a *protocol.TaskAssignment) { a.Task.NumPartitions++ },
	} {
		t.Run(name, func(t *testing.T) {
			a := shuffleReduceAssignment()
			change(&a)
			if err := a.Validate(); err == nil {
				t.Fatal("invalid assignment accepted")
			}
		})
	}
	_, m := shuffleAssignment()
	for _, output := range []protocol.TaskOutput{{ShuffleOutput: &m, Count: 1}, {ShuffleOutput: &m, Records: []json.RawMessage{}}, {Count: 1, Records: []json.RawMessage{}}} {
		if err := output.Validate(); err == nil {
			t.Fatal("mixed output accepted")
		}
	}
	for _, failure := range []protocol.TaskFailureRequest{
		{Kind: scheduler.FailureShuffleInput, WorkerID: "a", Error: "failed"},
		{Kind: scheduler.FailureExecution, WorkerID: "a", Error: "failed", ShuffleInput: &shuffle.InputReference{Attempt: m.Attempt}},
	} {
		if err := failure.Validate(); err == nil {
			t.Fatal("contradictory failure accepted")
		}
	}
}
