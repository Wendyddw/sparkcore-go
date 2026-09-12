package protocol_test

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/protocol"
)

func TestDecodeAndValidateRejectsInvalidJSONEnvelopes(t *testing.T) {
	const valid = `{"worker_id":"a","total_slots":2}`
	tests := []struct{ name, body string }{
		{"empty", ""}, {"malformed", `{"worker_id":`}, {"unknown", `{"worker_id":"a","total_slots":2,"extra":1}`},
		{"multiple values", valid + ` {}`}, {"trailing junk", valid + ` nope`}, {"null root", `null`}, {"array root", `[]`},
		{"missing capacity", `{"worker_id":"a"}`}, {"null capacity", `{"worker_id":"a","total_slots":null}`},
		{"wrong type", `{"worker_id":1,"total_slots":2}`}, {"duplicate field", `{"worker_id":"a","total_slots":2,"total_slots":3}`},
		{"wrong casing", `{"Worker_ID":"a","total_slots":2}`}, {"invalid capacity", `{"worker_id":"a","total_slots":0}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](strings.NewReader(test.body), 1024)
			if !errors.Is(err, protocol.ErrInvalidMessage) {
				t.Fatalf("DecodeAndValidate() = %#v, %v; want invalid message", got, err)
			}
			if got != (protocol.RegisterWorkerRequest{}) {
				t.Fatalf("partial result escaped: %#v", got)
			}
		})
	}
	got, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](strings.NewReader(valid+" \n"), 1024)
	if err != nil || got.WorkerID != "a" || got.TotalSlots != 2 {
		t.Fatalf("valid registration = %#v, %v", got, err)
	}
}

func TestDecodeAndValidateRequiresExplicitAttemptIdentityButAllowsZero(t *testing.T) {
	const valid = `{"job_id":0,"stage_id":0,"attempt":{"id":0,"task_id":0,"stage_attempt_id":0},"partition_id":0,"worker_id":"a","error":"failed"}`
	if _, err := protocol.DecodeAndValidate[protocol.TaskFailureRequest](strings.NewReader(valid), 1024); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		strings.Replace(valid, `"job_id":0,`, "", 1), strings.Replace(valid, `"id":0,`, "", 1),
		strings.Replace(valid, `"attempt":{"id":0,"task_id":0,"stage_attempt_id":0}`, `"attempt":null`, 1),
		strings.Replace(valid, `"id":0`, `"id":null`, 1), strings.Replace(valid, `"id":0`, `"id":-1`, 1),
		strings.Replace(valid, `"id":0`, `"id":1.5`, 1), strings.Replace(valid, `"id":0`, `"id":18446744073709551616`, 1),
		strings.Replace(valid, `"id":0`, `"id":"0"`, 1), strings.Replace(valid, `"id":0`, `"id":0,"unexpected":1`, 1),
		strings.Replace(valid, `"id":0`, `"id":0,"id":1`, 1), strings.Replace(valid, `"partition_id":0`, `"partition_id":-1`, 1),
	} {
		if _, err := protocol.DecodeAndValidate[protocol.TaskFailureRequest](strings.NewReader(body), 1024); !errors.Is(err, protocol.ErrInvalidMessage) {
			t.Fatalf("body %s: error = %v", body, err)
		}
	}
	large := strings.Replace(valid, `"id":0`, `"id":18446744073709551615`, 1)
	got, err := protocol.DecodeAndValidate[protocol.TaskFailureRequest](strings.NewReader(large), 1024)
	if err != nil || uint64(got.Attempt.ID) != ^uint64(0) {
		t.Fatalf("max ID = %d, %v", got.Attempt.ID, err)
	}
}

func TestDecodeAndValidateEnforcesWholeBodyLimit(t *testing.T) {
	const valid = `{"worker_id":"a","total_slots":2}`
	for _, test := range []struct {
		body     string
		limit    int64
		tooLarge bool
	}{
		{valid, int64(len(valid)), false}, {valid, int64(len(valid) - 1), true},
		{valid + strings.Repeat(" ", 10), int64(len(valid)), true},
	} {
		_, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](strings.NewReader(test.body), test.limit)
		if test.tooLarge && !errors.Is(err, protocol.ErrMessageTooLarge) {
			t.Fatalf("error = %v, want too large", err)
		}
		if !test.tooLarge && err != nil {
			t.Fatal(err)
		}
	}
	reader := &countingReader{Reader: strings.NewReader(strings.Repeat(" ", 4096))}
	_, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](reader, 32)
	if !errors.Is(err, protocol.ErrMessageTooLarge) || reader.read != 33 {
		t.Fatalf("read %d bytes, error %v; want 33 and too large", reader.read, err)
	}
	for _, limit := range []int64{0, -1, 1<<63 - 1} {
		if _, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](strings.NewReader(valid), limit); err == nil {
			t.Fatal("invalid limit accepted")
		}
	}
	if _, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](nil, 32); err == nil {
		t.Fatal("nil reader accepted")
	}
	sentinel := errors.New("read failed")
	if _, err := protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](errorReader{sentinel}, 32); !errors.Is(err, sentinel) {
		t.Fatalf("error = %v", err)
	}
}

type countingReader struct {
	io.Reader
	read int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.read += n
	return n, err
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestDecodeAndValidatePreservesOpaqueRecordJSON(t *testing.T) {
	const body = `{"action":"collect","records":[{"unknown_field":9007199254740993,"nested":[null,true]},null,9223372036854775807],"count":0}`
	got, err := protocol.DecodeAndValidate[protocol.JobResultResponse](strings.NewReader(body), 1024)
	if err != nil {
		t.Fatal(err)
	}
	want := []json.RawMessage{json.RawMessage(`{"unknown_field":9007199254740993,"nested":[null,true]}`), json.RawMessage(`null`), json.RawMessage(`9223372036854775807`)}
	if !reflect.DeepEqual(got.Records, want) {
		t.Fatalf("records = %s, want %s", got.Records, want)
	}
}

func TestDecodeAndValidateChecksNestedMetadataAndArrayElements(t *testing.T) {
	assignment := validAssignment()
	data, err := json.Marshal(protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{assignment}})
	if err != nil {
		t.Fatal(err)
	}
	valid := string(data)
	for _, body := range []string{
		strings.Replace(valid, `"num_partitions":4,`, "", 1),
		strings.Replace(valid, `"num_partitions":4`, `"num_partitions":null`, 1),
		strings.Replace(valid, `"target_rdd":1`, `"target_rdd":null`, 1),
		strings.Replace(valid, `"source_path":"missing-source.txt"`, `"source_path":"missing-source.txt","extra":1`, 1),
		strings.Replace(valid, `"source_path":"missing-source.txt"`, `"source_path":"missing-source.txt","source_path":"other"`, 1),
		`{"assignments":[null]}`,
	} {
		if _, err := protocol.DecodeAndValidate[protocol.HeartbeatResponse](strings.NewReader(body), 4096); !errors.Is(err, protocol.ErrInvalidMessage) {
			t.Fatalf("body %s: error = %v", body, err)
		}
	}
	for _, body := range []string{
		`{"worker_id":"a","free_slots":1,"running_attempt_ids":[null]}`,
		`{"worker_id":"a","free_slots":1,"running_attempt_ids":[-1]}`,
		`{"worker_id":"a","free_slots":1,"running_attempt_ids":null}`,
	} {
		if _, err := protocol.DecodeAndValidate[protocol.HeartbeatRequest](strings.NewReader(body), 1024); !errors.Is(err, protocol.ErrInvalidMessage) {
			t.Fatalf("body %s: error = %v", body, err)
		}
	}
	const success = `{"job_id":0,"stage_id":0,"attempt":{"id":0,"task_id":0,"stage_attempt_id":0},"partition_id":0,"worker_id":"a","output":{"records":null,"count":9223372036854775807}}`
	result, err := protocol.DecodeAndValidate[protocol.TaskSuccessRequest](strings.NewReader(success), 1024)
	if err != nil || result.Output.Count != 1<<63-1 {
		t.Fatalf("large count = %d, error = %v", result.Output.Count, err)
	}
}
