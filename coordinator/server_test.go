package coordinator_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/coordinator"
	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func newService(t *testing.T, config coordinator.Config) (*scheduler.FIFOTaskScheduler, *http.Server) {
	t.Helper()
	tasks := scheduler.NewFIFOTaskScheduler()
	t.Cleanup(tasks.Close)
	server, err := coordinator.NewServer(tasks, config)
	if err != nil {
		t.Fatal(err)
	}
	return tasks, server
}

func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func response[T protocol.Message](t *testing.T, w *httptest.ResponseRecorder, status int) T {
	t.Helper()
	if w.Code != status || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("response = %d %v %s", w.Code, w.Header(), w.Body.String())
	}
	value, err := protocol.DecodeAndValidate[T](w.Body, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func checkError(t *testing.T, w *httptest.ResponseRecorder, status int, code protocol.ErrorCode) {
	t.Helper()
	got := response[protocol.ErrorResponse](t, w, status)
	if got.Code != code {
		t.Fatalf("error = %#v, want code %q", got, code)
	}
}

func register(t *testing.T, h http.Handler, id plan.WorkerID, slots int) {
	t.Helper()
	w := request(h, http.MethodPost, protocol.RegisterWorkerPath,
		fmt.Sprintf(`{"worker_id":%q,"total_slots":%d}`, id, slots))
	got := response[protocol.RegisterWorkerResponse](t, w, http.StatusOK)
	if got.WorkerID != id || got.TotalSlots != slots {
		t.Fatalf("registration = %#v", got)
	}
}

func TestRegistrationIsIdempotentAndRejectsCapacityConflict(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 2)
	before, err := tasks.Worker("a")
	if err != nil {
		t.Fatal(err)
	}
	register(t, server.Handler, "a", 2)
	w := request(server.Handler, http.MethodPost, protocol.RegisterWorkerPath,
		`{"worker_id":"a","total_slots":3}`)
	checkError(t, w, http.StatusConflict, protocol.CodeConflict)
	after, err := tasks.Worker("a")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("registration changed worker: before=%#v after=%#v err=%v", before, after, err)
	}
}

func TestHTTPRejectsInvalidRequests(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 2)
	before, _ := tasks.Worker("a")
	tests := []struct {
		name, method, path, body string
		status                   int
		code                     protocol.ErrorCode
	}{
		{"register method", "GET", protocol.RegisterWorkerPath, "", 405, protocol.CodeMethodNotAllowed},
		{"heartbeat method", "PUT", protocol.HeartbeatPath, "", 405, protocol.CodeMethodNotAllowed},
		{"unknown path", "POST", "/v1/missing", "{}", 404, protocol.CodeNotFound},
		{"trailing slash", "POST", protocol.HeartbeatPath + "/", "{}", 404, protocol.CodeNotFound},
		{"empty body", "POST", protocol.RegisterWorkerPath, "", 400, protocol.CodeInvalidRequest},
		{"malformed JSON", "POST", protocol.RegisterWorkerPath, "{", 400, protocol.CodeInvalidRequest},
		{"unknown field", "POST", protocol.RegisterWorkerPath, `{"worker_id":"b","total_slots":2,"extra":1}`, 400, protocol.CodeInvalidRequest},
		{"trailing value", "POST", protocol.RegisterWorkerPath, `{"worker_id":"b","total_slots":2}{}`, 400, protocol.CodeInvalidRequest},
		{"missing capacity", "POST", protocol.RegisterWorkerPath, `{"worker_id":"b"}`, 400, protocol.CodeInvalidRequest},
		{"invalid registration", "POST", protocol.RegisterWorkerPath, `{"worker_id":"b","total_slots":0}`, 400, protocol.CodeInvalidRequest},
		{"invalid base URL", "POST", protocol.RegisterWorkerPath, `{"worker_id":"b","total_slots":2,"base_url":"invalid"}`, 400, protocol.CodeInvalidRequest},
		{"unknown worker", "POST", protocol.HeartbeatPath, `{"worker_id":"b","free_slots":1,"running_attempt_ids":[]}`, 404, protocol.CodeNotFound},
		{"negative slots", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":-1,"running_attempt_ids":[]}`, 400, protocol.CodeInvalidRequest},
		{"excess capacity", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":3,"running_attempt_ids":[]}`, 400, protocol.CodeInvalidRequest},
		{"inconsistent capacity", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":2,"running_attempt_ids":[0]}`, 400, protocol.CodeInvalidRequest},
		{"unknown attempt", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":1,"running_attempt_ids":[99]}`, 400, protocol.CodeInvalidRequest},
		{"missing running IDs", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":2}`, 400, protocol.CodeInvalidRequest},
		{"null running IDs", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":2,"running_attempt_ids":null}`, 400, protocol.CodeInvalidRequest},
		{"duplicate running IDs", "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":0,"running_attempt_ids":[0,0]}`, 400, protocol.CodeInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := request(server.Handler, test.method, test.path, test.body)
			checkError(t, w, test.status, test.code)
			if test.status == http.StatusMethodNotAllowed && w.Header().Get("Allow") != http.MethodPost {
				t.Fatal("missing Allow: POST")
			}
		})
	}
	after, _ := tasks.Worker("a")
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected request changed worker: %#v", after)
	}
	if _, err := tasks.Worker("b"); !errors.Is(err, scheduler.ErrUnknownWorker) {
		t.Fatalf("invalid request registered worker: %v", err)
	}
}

func TestRequestLimitIncludesTrailingWhitespace(t *testing.T) {
	for _, path := range []string{protocol.RegisterWorkerPath, protocol.HeartbeatPath} {
		t.Run(path, func(t *testing.T) {
			body := `{"worker_id":"a","total_slots":1}`
			if path == protocol.HeartbeatPath {
				body = `{"worker_id":"a","free_slots":1,"running_attempt_ids":[]}`
			}
			tasks, server := newService(t, coordinator.Config{MaxRequestBytes: int64(len(body))})
			if path == protocol.HeartbeatPath {
				register(t, server.Handler, "a", 1)
			}
			w := request(server.Handler, http.MethodPost, path, body+" ")
			checkError(t, w, http.StatusRequestEntityTooLarge, protocol.CodeRequestTooLarge)
			if path == protocol.RegisterWorkerPath {
				if _, err := tasks.Worker("a"); !errors.Is(err, scheduler.ErrUnknownWorker) {
					t.Fatalf("oversized registration changed state: %v", err)
				}
			} else if worker, _ := tasks.Worker("a"); !worker.LastHeartbeat.IsZero() {
				t.Fatal("oversized heartbeat changed state")
			}
			w = request(server.Handler, http.MethodPost, path, body)
			if w.Code != http.StatusOK {
				t.Fatalf("exact size limit rejected: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

type failingRegistration struct {
	coordinator.TaskScheduler
	err error
}

func (s failingRegistration) RegisterWorker(plan.WorkerID, int) error { return s.err }

func TestSchedulerErrorsHaveStableHTTPResponses(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
		code   protocol.ErrorCode
	}{
		{fmt.Errorf("wrapped: %w", scheduler.ErrWorkerConflict), 409, protocol.CodeConflict},
		{errors.New("private scheduler detail"), 500, protocol.CodeInternal},
	} {
		server, err := coordinator.NewServer(failingRegistration{err: test.err}, coordinator.Config{})
		if err != nil {
			t.Fatal(err)
		}
		w := request(server.Handler, http.MethodPost, protocol.RegisterWorkerPath, `{"worker_id":"a","total_slots":1}`)
		if test.status == 500 && strings.Contains(w.Body.String(), "private scheduler detail") {
			t.Fatal("internal error exposed implementation details")
		}
		checkError(t, w, test.status, test.code)
	}

	tasks, server := newService(t, coordinator.Config{})
	register(t, server.Handler, "a", 1)
	tasks.Close()
	checkError(t, request(server.Handler, "POST", protocol.RegisterWorkerPath, `{"worker_id":"a","total_slots":1}`),
		http.StatusServiceUnavailable, protocol.CodeUnavailable)
	checkError(t, request(server.Handler, "POST", protocol.HeartbeatPath, `{"worker_id":"a","free_slots":1,"running_attempt_ids":[]}`),
		http.StatusServiceUnavailable, protocol.CodeUnavailable)
}

func TestServerConfiguration(t *testing.T) {
	tasks, server := newService(t, coordinator.Config{})
	if server.Addr != "127.0.0.1:8080" || server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("missing bounded defaults: %#v", server)
	}
	config := coordinator.Config{Addr: "127.0.0.1:0", MaxRequestBytes: 512, ReadHeaderTimeout: time.Second,
		ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 4 * time.Second}
	server, err := coordinator.NewServer(tasks, config)
	if err != nil {
		t.Fatal(err)
	}
	if server.Addr != config.Addr || server.ReadHeaderTimeout != config.ReadHeaderTimeout || server.ReadTimeout != config.ReadTimeout || server.WriteTimeout != config.WriteTimeout || server.IdleTimeout != config.IdleTimeout {
		t.Fatalf("HTTP configuration was not applied: %#v", server)
	}
	if _, err := coordinator.NewServer(nil, config); err == nil {
		t.Fatal("nil scheduler accepted")
	}
	for _, invalid := range []coordinator.Config{
		{MaxRequestBytes: -1}, {MaxRequestBytes: 1<<63 - 1},
		{ReadHeaderTimeout: -1}, {ReadTimeout: -1}, {WriteTimeout: -1}, {IdleTimeout: -1},
	} {
		if _, err := coordinator.NewServer(tasks, invalid); err == nil {
			t.Fatalf("invalid config accepted: %#v", invalid)
		}
	}
}

type blockedRegistration struct {
	coordinator.TaskScheduler
	entered chan struct{}
	release chan struct{}
}

func (s blockedRegistration) RegisterWorker(id plan.WorkerID, slots int) error {
	close(s.entered)
	<-s.release
	return s.TaskScheduler.RegisterWorker(id, slots)
}

func TestShutdownDrainsActiveRequestAndLeavesSchedulerOwnedByCaller(t *testing.T) {
	tasks := scheduler.NewFIFOTaskScheduler()
	defer tasks.Close()
	blocked := blockedRegistration{TaskScheduler: tasks, entered: make(chan struct{}), release: make(chan struct{})}
	server, err := coordinator.NewServer(blocked, coordinator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewUnstartedServer(nil)
	ts.Config = server
	ts.Start()
	defer ts.Close()
	defer close(blocked.release)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requestDone := make(chan error, 1)
	go func() {
		r, err := http.NewRequestWithContext(ctx, "POST", ts.URL+protocol.RegisterWorkerPath,
			strings.NewReader(`{"worker_id":"a","total_slots":1}`))
		if err != nil {
			requestDone <- err
			return
		}
		res, err := ts.Client().Do(r)
		if err == nil {
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				err = fmt.Errorf("registration status = %d", res.StatusCode)
			} else {
				_, err = protocol.DecodeAndValidate[protocol.RegisterWorkerResponse](res.Body, 1024)
			}
		}
		requestDone <- err
	}()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		t.Fatal("registration did not start")
	}
	stopping := make(chan struct{})
	server.RegisterOnShutdown(func() { close(stopping) })
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- server.Shutdown(ctx) }()
	select {
	case <-stopping:
	case <-ctx.Done():
		t.Fatal("shutdown did not start")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before active request completed: %v", err)
	default:
	}
	blocked.release <- struct{}{}
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if err := tasks.RegisterWorker("b", 1); err != nil {
		t.Fatalf("HTTP shutdown closed caller's scheduler: %v", err)
	}
}
