package coordinator_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

type submitFunc func(context.Context, protocol.SubmitJobRequest) (protocol.JobResultResponse, error)

func (f submitFunc) Submit(ctx context.Context, spec protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
	return f(ctx, spec)
}

const countJobJSON = `{"source":{"path":"/worker/input.txt","num_partitions":4},"transformations":[{"kind":"map","function_id":"normalize"}],"action":"count"}`

func jobHandler(t *testing.T, jobs coordinator.JobSubmitter, config coordinator.Config) http.Handler {
	t.Helper()
	tasks, _ := newService(t, coordinator.Config{})
	server, err := coordinator.NewServer(tasks, jobs, config)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler
}

func TestSubmitHTTPDecodesJobAndSetsDeadline(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Minute} {
		called := false
		h := jobHandler(t, submitFunc(func(ctx context.Context, spec protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
			called = true
			if spec.Source.Path != "/worker/input.txt" || spec.Source.NumPartitions != 4 || len(spec.Transformations) != 1 ||
				spec.Transformations[0].FunctionID != "normalize" || spec.Action != scheduler.ActionCount {
				t.Fatalf("decoded spec = %+v", spec)
			}
			want := timeout
			if want == 0 {
				want = 5 * time.Minute
			}
			deadline, ok := ctx.Deadline()
			if left := time.Until(deadline); !ok || left > want || left < want-time.Second {
				t.Fatalf("job deadline = %v, want %v", deadline, want)
			}
			return protocol.JobResultResponse{Action: scheduler.ActionCount, Count: 9007199254740993}, nil
		}), coordinator.Config{JobTimeout: timeout, MaxRequestBytes: int64(len(countJobJSON))})
		got := response[protocol.JobResultResponse](t, request(h, "POST", protocol.SubmitJobPath, countJobJSON), 200)
		if !called || got.Count != 9007199254740993 || got.Records != nil {
			t.Fatalf("result = %+v, called = %v", got, called)
		}
	}
}

func TestSubmitHTTPRejectsRequestsBeforeSubmission(t *testing.T) {
	h := jobHandler(t, submitFunc(func(context.Context, protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
		t.Fatal("invalid request reached job service")
		return protocol.JobResultResponse{}, nil
	}), coordinator.Config{MaxRequestBytes: int64(len(countJobJSON))})
	for _, test := range []struct {
		method, path, body string
		status             int
		code               protocol.ErrorCode
	}{
		{"GET", protocol.SubmitJobPath, "", 405, protocol.CodeMethodNotAllowed},
		{"POST", protocol.SubmitJobPath + "/", "{}", 404, protocol.CodeNotFound},
		{"POST", protocol.SubmitJobPath, "{", 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, `{"source":{"path":"x"},"action":"count"}`, 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, `{"source":{"path":"x","num_partitions":null},"action":"count"}`, 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, `{"source":{"path":"x","num_partitions":0},"action":"count"}`, 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, `{"source":{"path":"x","num_partitions":1,"extra":1},"action":"count"}`, 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, `{"source":{"path":"x","num_partitions":1},"action":"count","action":"collect"}`, 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, `{"source":{"path":"x","num_partitions":1},"action":"count"}{}`, 400, protocol.CodeInvalidRequest},
		{"POST", protocol.SubmitJobPath, countJobJSON + " ", 413, protocol.CodeRequestTooLarge},
	} {
		w := request(h, test.method, test.path, test.body)
		checkError(t, w, test.status, test.code)
		if test.status == 405 && w.Header().Get("Allow") != "POST" {
			t.Fatal("missing Allow header")
		}
	}
}

func TestSubmitHTTPErrorCategories(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
		code   protocol.ErrorCode
	}{
		{fmt.Errorf("wrapped: %w", coordinator.ErrJobFailed), 422, protocol.CodeJobFailed},
		{fmt.Errorf("wrapped: %w", scheduler.ErrSchedulerClosed), 503, protocol.CodeUnavailable},
		{fmt.Errorf("wrapped: %w", scheduler.ErrTaskSchedulerClosed), 503, protocol.CodeUnavailable},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), 504, protocol.CodeJobFailed},
		{errors.New("private implementation detail"), 500, protocol.CodeInternal},
	} {
		h := jobHandler(t, submitFunc(func(context.Context, protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
			return protocol.JobResultResponse{}, test.err
		}), coordinator.Config{})
		w := request(h, "POST", protocol.SubmitJobPath, countJobJSON)
		if strings.Contains(w.Body.String(), "private implementation detail") {
			t.Fatal("internal error detail exposed")
		}
		checkError(t, w, test.status, test.code)
	}
	checkError(t, request(jobHandler(t, nil, coordinator.Config{}), "POST", protocol.SubmitJobPath, countJobJSON), 503, protocol.CodeUnavailable)
}

func TestSubmitHTTPRejectsInvalidResults(t *testing.T) {
	for _, result := range []protocol.JobResultResponse{
		{Action: scheduler.ActionCollect},
		{Action: scheduler.ActionCount, Count: -1},
	} {
		h := jobHandler(t, submitFunc(func(context.Context, protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
			return result, nil
		}), coordinator.Config{})
		checkError(t, request(h, "POST", protocol.SubmitJobPath, countJobJSON), 500, protocol.CodeInternal)
	}
}

// A wrapped ResponseWriter may fail either deadline update; never return success.
type failingDeadlineWriter struct {
	*httptest.ResponseRecorder
	calls, failOn int
}

func (w *failingDeadlineWriter) SetWriteDeadline(time.Time) error {
	w.calls++
	if w.calls == w.failOn {
		return errors.New("deadline unavailable")
	}
	return nil
}

func TestSubmitHTTPHandlesDeadlineConfigurationFailure(t *testing.T) {
	for _, failOn := range []int{1, 2} {
		calls := 0
		h := jobHandler(t, submitFunc(func(context.Context, protocol.SubmitJobRequest) (protocol.JobResultResponse, error) {
			calls++
			return protocol.JobResultResponse{Action: scheduler.ActionCount, Count: 5}, nil
		}), coordinator.Config{})
		w := &failingDeadlineWriter{ResponseRecorder: httptest.NewRecorder(), failOn: failOn}
		h.ServeHTTP(w, httptest.NewRequest("POST", protocol.SubmitJobPath, strings.NewReader(countJobJSON)))
		checkError(t, w.ResponseRecorder, 500, protocol.CodeInternal)
		if calls != failOn-1 {
			t.Fatalf("deadline update %d: job submitted %d times", failOn, calls)
		}
	}
}
