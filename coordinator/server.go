// Package coordinator adapts HTTP requests to scheduler operations.
package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// TaskScheduler owns worker state and placement. Methods must be concurrency-safe.
type TaskScheduler interface {
	RegisterWorker(plan.WorkerID, int) error
	Worker(plan.WorkerID) (scheduler.WorkerSnapshot, error)
	OfferResources(plan.WorkerID, int, []plan.TaskAttemptID) ([]scheduler.TaskAssignment, error)
}

var _ TaskScheduler = (*scheduler.FIFOTaskScheduler)(nil)

// Config sets HTTP limits. Zero values select the defaults in NewServer.
type Config struct {
	Addr              string
	MaxRequestBytes   int64
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// NewServer creates an HTTP server without starting it. Defaults are localhost:8080,
// 1 MiB bodies, 5s header reads, 10s reads/writes, and 60s idle connections.
// The caller owns serving and Shutdown; shutting down HTTP does not close tasks.
func NewServer(tasks TaskScheduler, config Config) (*http.Server, error) {
	if tasks == nil {
		return nil, fmt.Errorf("task scheduler is nil")
	}
	if config.MaxRequestBytes < 0 || config.MaxRequestBytes == 1<<63-1 {
		return nil, fmt.Errorf("max request bytes must be positive and bounded")
	}
	if config.ReadHeaderTimeout < 0 || config.ReadTimeout < 0 || config.WriteTimeout < 0 || config.IdleTimeout < 0 {
		return nil, fmt.Errorf("HTTP timeouts must not be negative")
	}
	if config.Addr == "" {
		config.Addr = "127.0.0.1:8080"
	}
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = 1 << 20
	}
	if config.ReadHeaderTimeout == 0 {
		config.ReadHeaderTimeout = 5 * time.Second
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = 10 * time.Second
	}
	if config.WriteTimeout == 0 {
		config.WriteTimeout = 10 * time.Second
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = time.Minute
	}
	return &http.Server{
		Addr:              config.Addr,
		Handler:           &handler{tasks: tasks, maxRequestBytes: config.MaxRequestBytes},
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       config.IdleTimeout,
	}, nil
}

type handler struct {
	tasks           TaskScheduler
	maxRequestBytes int64
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var serve http.HandlerFunc
	switch r.URL.Path {
	case protocol.RegisterWorkerPath:
		serve = h.registerWorker
	case protocol.HeartbeatPath:
		serve = h.heartbeat
	default:
		writeError(w, http.StatusNotFound, protocol.CodeNotFound, "unknown endpoint")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, protocol.CodeMethodNotAllowed, "endpoint requires POST")
		return
	}
	serve(w, r)
}

func decodeRequest[T protocol.Message](w http.ResponseWriter, r *http.Request, maxBytes int64) (T, bool) {
	message, err := protocol.DecodeAndValidate[T](r.Body, maxBytes)
	if err == nil {
		return message, true
	}
	if errors.Is(err, protocol.ErrMessageTooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, protocol.CodeRequestTooLarge, err.Error())
	} else {
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, err.Error())
	}
	return message, false
}

func writeSchedulerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrUnknownWorker):
		writeError(w, http.StatusNotFound, protocol.CodeNotFound, err.Error())
	case errors.Is(err, scheduler.ErrWorkerConflict):
		writeError(w, http.StatusConflict, protocol.CodeConflict, err.Error())
	case errors.Is(err, scheduler.ErrInvalidWorker), errors.Is(err, scheduler.ErrInvalidResourceOffer):
		writeError(w, http.StatusBadRequest, protocol.CodeInvalidRequest, err.Error())
	case errors.Is(err, scheduler.ErrTaskSchedulerClosed):
		writeError(w, http.StatusServiceUnavailable, protocol.CodeUnavailable, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, protocol.CodeInternal, "scheduler operation failed")
	}
}

func writeError(w http.ResponseWriter, status int, code protocol.ErrorCode, message string) {
	writeJSON(w, status, protocol.ErrorResponse{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, message any) {
	data, err := json.Marshal(message)
	if err != nil {
		status = http.StatusInternalServerError
		data = []byte(`{"code":"internal_error","message":"response encoding failed"}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(data, '\n'))
}
