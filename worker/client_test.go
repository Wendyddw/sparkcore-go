package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/worker"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newClient(t *testing.T, url string, config worker.ClientConfig) *worker.Client {
	t.Helper()
	client, err := worker.NewClient(url, config)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func assignment() protocol.TaskAssignment {
	return protocol.TaskAssignment{
		RunID: "0123456789abcdef0123456789abcdef",
		JobID: 7, StageID: 3, WorkerID: "a",
		Attempt: scheduler.TaskAttemptIdentity{ID: 9007199254740993, TaskID: 11, StageAttemptID: 2},
		Task: scheduler.Task{ID: 11, StageID: 3, StageKind: scheduler.StageResult, PartitionID: 0, NumPartitions: 1,
			Operations: []scheduler.StageOperation{{Kind: scheduler.StageOperationRDD,
				RDD: &scheduler.RDDOperationSpec{RDDID: 0, Operator: plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "/missing/source"}}}},
			FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCollect, TargetRDD: 0}},
	}
}

func successRequest(a protocol.TaskAssignment) protocol.TaskSuccessRequest {
	return protocol.TaskSuccessRequest{JobID: a.JobID, StageID: a.StageID, WorkerID: a.WorkerID,
		Attempt: a.Attempt, PartitionID: a.Task.PartitionID,
		Output: protocol.TaskOutput{Records: []json.RawMessage{json.RawMessage(`{"id":9007199254740993,"values":[null,true]}`)}}}
}

func failureRequest(a protocol.TaskAssignment) protocol.TaskFailureRequest {
	return protocol.TaskFailureRequest{JobID: a.JobID, StageID: a.StageID, WorkerID: a.WorkerID,
		Attempt: a.Attempt, PartitionID: a.Task.PartitionID, Error: "source read failed"}
}

func TestClientExchangesAllWorkerMessages(t *testing.T) {
	wantAssignment := assignment()
	registration := protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 1}
	heartbeat := protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{}}
	success := successRequest(wantAssignment)
	failure := failureRequest(wantAssignment)
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("request = %s %v", r.Method, r.Header)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		var got, want, reply any
		var err error
		switch r.URL.Path {
		case protocol.RegisterWorkerPath:
			got, err = protocol.DecodeAndValidate[protocol.RegisterWorkerRequest](r.Body, 4096)
			want, reply = registration, protocol.RegisterWorkerResponse{WorkerID: "a", TotalSlots: 1}
		case protocol.HeartbeatPath:
			got, err = protocol.DecodeAndValidate[protocol.HeartbeatRequest](r.Body, 4096)
			want, reply = heartbeat, protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{wantAssignment}}
		case protocol.TaskSuccessPath:
			got, err = protocol.DecodeAndValidate[protocol.TaskSuccessRequest](r.Body, 4096)
			want, reply = success, protocol.TaskReportResponse{Acknowledged: true}
		case protocol.TaskFailurePath:
			got, err = protocol.DecodeAndValidate[protocol.TaskFailureRequest](r.Body, 4096)
			want, reply = failure, protocol.TaskReportResponse{Acknowledged: true}
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("request %s = %#v, want %#v, err=%v", r.URL.Path, got, want, err)
		}
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	}))
	defer ts.Close()
	client := newClient(t, ts.URL+"/", worker.ClientConfig{})
	ctx := context.Background()
	if _, err := client.RegisterWorker(ctx, registration); err != nil {
		t.Fatal(err)
	}
	got, err := client.Heartbeat(ctx, heartbeat)
	if err != nil || !reflect.DeepEqual(got.Assignments, []protocol.TaskAssignment{wantAssignment}) {
		t.Fatalf("assignments = %#v, err=%v", got, err)
	}
	if ack, err := client.ReportSuccess(ctx, success); err != nil || !ack.Acknowledged {
		t.Fatalf("success acknowledgment = %#v, %v", ack, err)
	}
	if ack, err := client.ReportFailure(ctx, failure); err != nil || !ack.Acknowledged {
		t.Fatalf("failure acknowledgment = %#v, %v", ack, err)
	}
	if requests.Load() != 4 {
		t.Fatalf("request count = %d, want 4", requests.Load())
	}
}

func TestClientRejectsInvalidRequestsBeforeSending(t *testing.T) {
	var calls atomic.Int32
	client := newClient(t, "http://coordinator.test", worker.ClientConfig{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected request")
	})})
	ctx := context.Background()
	_, registerErr := client.RegisterWorker(ctx, protocol.RegisterWorkerRequest{WorkerID: "a"})
	_, heartbeatErr := client.Heartbeat(ctx, protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: -1})
	success := successRequest(assignment())
	success.Output.Count = -1
	_, successErr := client.ReportSuccess(ctx, success)
	failure := failureRequest(assignment())
	failure.Error = ""
	_, failureErr := client.ReportFailure(ctx, failure)
	for _, err := range []error{registerErr, heartbeatErr, successErr, failureErr} {
		if err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request reached transport")
	}
}

func TestClientRejectsMalformedResponses(t *testing.T) {
	for _, test := range []struct {
		name, body, contentType string
		status                  int
		limit                   int64
	}{
		{"malformed", "{", "application/json", 200, 0},
		{"unknown field", `{"acknowledged":true,"extra":0}`, "application/json", 200, 0},
		{"trailing value", `{"acknowledged":true}{}`, "application/json", 200, 0},
		{"negative acknowledgment", `{"acknowledged":false}`, "application/json", 200, 0},
		{"missing field", `{}`, "application/json", 200, 0},
		{"non-JSON content type", `{"acknowledged":true}`, "text/plain", 200, 0},
		{"unexpected success status", `{"acknowledged":true}`, "application/json", 201, 0},
		{"invalid error body", `{"code":"conflict"}`, "application/json", 409, 0},
		{"oversized success", `{"acknowledged":true} `, "application/json", 200, 21},
		{"oversized error", `{"code":"conflict","message":"too big"}`, "application/json", 409, 21},
	} {
		t.Run(test.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(test.status)
				io.WriteString(w, test.body)
			}))
			defer ts.Close()
			client := newClient(t, ts.URL, worker.ClientConfig{MaxResponseBytes: test.limit})
			got, err := client.ReportFailure(context.Background(), failureRequest(assignment()))
			if !errors.Is(err, worker.ErrInvalidResponse) || got.Acknowledged {
				t.Fatalf("response = %#v, err=%v", got, err)
			}
			if strings.HasPrefix(test.name, "oversized") && !errors.Is(err, protocol.ErrMessageTooLarge) {
				t.Fatalf("size error not preserved: %v", err)
			}
		})
	}
}

func TestClientRejectsResponsesInconsistentWithRequest(t *testing.T) {
	for _, test := range []string{"registration worker", "registration slots", "assignment worker", "assignment capacity", "running attempt"} {
		t.Run(test, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var response any
				if strings.HasPrefix(test, "registration") {
					registration := protocol.RegisterWorkerResponse{WorkerID: "a", TotalSlots: 1}
					if test == "registration worker" {
						registration.WorkerID = "b"
					} else {
						registration.TotalSlots = 2
					}
					response = registration
				} else {
					a := assignment()
					if test == "assignment worker" {
						a.WorkerID = "b"
					}
					response = protocol.HeartbeatResponse{Assignments: []protocol.TaskAssignment{a}}
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer ts.Close()
			client := newClient(t, ts.URL, worker.ClientConfig{})
			var err error
			if strings.HasPrefix(test, "registration") {
				var got protocol.RegisterWorkerResponse
				got, err = client.RegisterWorker(context.Background(), protocol.RegisterWorkerRequest{WorkerID: "a", TotalSlots: 1})
				if got != (protocol.RegisterWorkerResponse{}) {
					t.Fatal("invalid response returned partial registration")
				}
			} else {
				r := protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{}}
				if test == "assignment capacity" {
					r.FreeSlots = 0
				} else if test == "running attempt" {
					r.RunningAttemptIDs = []plan.TaskAttemptID{assignment().Attempt.ID}
				}
				var got protocol.HeartbeatResponse
				got, err = client.Heartbeat(context.Background(), r)
				if len(got.Assignments) != 0 {
					t.Fatal("invalid response returned executable assignments")
				}
			}
			if !errors.Is(err, worker.ErrInvalidResponse) {
				t.Fatalf("inconsistent response error = %v", err)
			}
		})
	}
}

type countedBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}
func (b *countedBody) Close() error { b.closed = true; return nil }

func TestClientBoundsAndClosesResponseBodies(t *testing.T) {
	const ack = `{"acknowledged":true}`
	for _, text := range []string{ack, ack + " ", "{"} {
		body := &countedBody{Reader: strings.NewReader(text)}
		client := newClient(t, "http://coordinator.test", worker.ClientConfig{MaxResponseBytes: int64(len(ack)), Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: body}, nil
		})})
		_, err := client.ReportFailure(context.Background(), failureRequest(assignment()))
		if (err == nil) != (text == ack) || !body.closed || body.read > len(ack)+1 {
			t.Fatalf("body %q: err=%v closed=%v read=%d", text, err, body.closed, body.read)
		}
	}
}

func TestClientHonorsCancellationAndBodyReadTimeout(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "cancellation"
		if timeout {
			name = "timeout"
		}
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				close(entered)
				<-r.Context().Done()
			}))
			defer ts.Close()
			config := worker.ClientConfig{}
			if timeout {
				config.RequestTimeout = 200 * time.Millisecond
			}
			client := newClient(t, ts.URL, config)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := client.ReportFailure(ctx, failureRequest(assignment())); done <- err }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("request did not reach server")
			}
			want := context.DeadlineExceeded
			if !timeout {
				cancel()
				want = context.Canceled
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatalf("request error = %v, want %v", err, want)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("request did not stop")
			}
		})
	}
}

func TestClientDoesNotFollowRedirectsOrRetryTransportFailures(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer ts.Close()
	client := newClient(t, ts.URL, worker.ClientConfig{})
	if _, err := client.ReportFailure(context.Background(), failureRequest(assignment())); err == nil || calls.Load() != 1 {
		t.Fatalf("redirect handling: calls=%d err=%v", calls.Load(), err)
	}
	calls.Store(0)
	want := errors.New("transport unavailable")
	client = newClient(t, ts.URL, worker.ClientConfig{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, want
	})})
	if _, err := client.ReportFailure(context.Background(), failureRequest(assignment())); !errors.Is(err, want) || calls.Load() != 1 {
		t.Fatalf("transport handling: calls=%d err=%v", calls.Load(), err)
	}
}

func TestClientSupportsConcurrentHeartbeatsAndReports(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == protocol.HeartbeatPath {
			io.WriteString(w, `{"assignments":[]}`)
		} else {
			io.WriteString(w, `{"acknowledged":true}`)
		}
	}))
	defer ts.Close()
	client := newClient(t, ts.URL, worker.ClientConfig{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := client.Heartbeat(context.Background(), protocol.HeartbeatRequest{WorkerID: "a", FreeSlots: 1, RunningAttemptIDs: []plan.TaskAttemptID{}})
			if err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := client.ReportFailure(context.Background(), failureRequest(assignment())); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestClientRejectsInvalidConfiguration(t *testing.T) {
	for _, url := range []string{"", "/relative", "ftp://host", "http://", "http://user:pass@host", "http://host/path", "http://host/%2f", "http://host?", "http://host?q=1", "http://host#fragment"} {
		if _, err := worker.NewClient(url, worker.ClientConfig{}); err == nil {
			t.Errorf("accepted invalid coordinator URL %q", url)
		}
	}
	for _, config := range []worker.ClientConfig{{RequestTimeout: -1}, {MaxResponseBytes: -1}, {MaxResponseBytes: 1<<63 - 1}} {
		if _, err := worker.NewClient("http://localhost", config); err == nil {
			t.Errorf("accepted invalid config %#v", config)
		}
	}
}
