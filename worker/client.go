// Package worker provides the coordinator client for remote task execution.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wendyddw/sparkcore-go/protocol"
)

// ErrInvalidResponse indicates a malformed or inconsistent coordinator response.
var ErrInvalidResponse = errors.New("invalid coordinator response")

// HTTPError preserves a coordinator rejection for errors.As callers.
type HTTPError struct {
	StatusCode int
	Code       protocol.ErrorCode
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("coordinator HTTP %d (%s): %s", e.StatusCode, e.Code, e.Message)
}

// ClientConfig uses a 10s request timeout and 1 MiB response limit by default.
// Transport defaults to http.DefaultTransport and remains caller-owned.
type ClientConfig struct {
	RequestTimeout   time.Duration
	MaxResponseBytes int64
	Transport        http.RoundTripper
}

// Client makes individual HTTP calls. It is safe for concurrent use and starts no loops.
type Client struct {
	baseURL          string
	http             *http.Client
	maxResponseBytes int64
}

// NewClient accepts an absolute HTTP(S) origin with an optional trailing slash.
// Calls do not follow redirects or retry requests.
func NewClient(baseURL string, config ClientConfig) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return nil, fmt.Errorf("coordinator URL must be an absolute HTTP(S) origin without credentials, query, or fragment")
	}
	if config.RequestTimeout < 0 || config.MaxResponseBytes < 0 || config.MaxResponseBytes == 1<<63-1 {
		return nil, fmt.Errorf("client timeout and response limit must be positive and bounded")
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 10 * time.Second
	}
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = 1 << 20
	}
	return &Client{
		baseURL: strings.TrimSuffix(u.String(), "/"), maxResponseBytes: config.MaxResponseBytes,
		http: &http.Client{
			Transport: config.Transport, Timeout: config.RequestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func (c *Client) RegisterWorker(ctx context.Context, request protocol.RegisterWorkerRequest) (protocol.RegisterWorkerResponse, error) {
	response, err := post[protocol.RegisterWorkerResponse](ctx, c, protocol.RegisterWorkerPath, request)
	if err != nil {
		return protocol.RegisterWorkerResponse{}, err
	}
	if response.WorkerID != request.WorkerID || response.TotalSlots != request.TotalSlots {
		return protocol.RegisterWorkerResponse{}, fmt.Errorf("%w: registration identity or capacity differs from request", ErrInvalidResponse)
	}
	return response, nil
}

func (c *Client) Heartbeat(ctx context.Context, request protocol.HeartbeatRequest) (protocol.HeartbeatResponse, error) {
	response, err := post[protocol.HeartbeatResponse](ctx, c, protocol.HeartbeatPath, request)
	if err != nil {
		return protocol.HeartbeatResponse{}, err
	}
	if len(response.Assignments) > request.FreeSlots {
		return protocol.HeartbeatResponse{}, fmt.Errorf("%w: assignments exceed offered slots", ErrInvalidResponse)
	}
	for _, assignment := range response.Assignments {
		if assignment.WorkerID != request.WorkerID {
			return protocol.HeartbeatResponse{}, fmt.Errorf("%w: assignment targets another worker", ErrInvalidResponse)
		}
		for _, running := range request.RunningAttemptIDs {
			if assignment.Attempt.ID == running {
				return protocol.HeartbeatResponse{}, fmt.Errorf("%w: assignment repeats running attempt %d", ErrInvalidResponse, running)
			}
		}
	}
	return response, nil
}

func (c *Client) ReportSuccess(ctx context.Context, request protocol.TaskSuccessRequest) (protocol.TaskReportResponse, error) {
	return post[protocol.TaskReportResponse](ctx, c, protocol.TaskSuccessPath, request)
}

func (c *Client) ReportFailure(ctx context.Context, request protocol.TaskFailureRequest) (protocol.TaskReportResponse, error) {
	return post[protocol.TaskReportResponse](ctx, c, protocol.TaskFailurePath, request)
}

func post[T protocol.Message](ctx context.Context, c *Client, path string, message protocol.Message) (T, error) {
	var zero T
	if err := message.Validate(); err != nil {
		return zero, fmt.Errorf("%s request: %w", path, err)
	}
	data, err := json.Marshal(message)
	if err != nil {
		return zero, fmt.Errorf("encode %s request: %w", path, err)
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(data))
	if err != nil {
		return zero, err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json")
	response, err := c.http.Do(r)
	if err != nil {
		return zero, fmt.Errorf("POST %s: %w", path, err)
	}
	defer response.Body.Close()
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return zero, fmt.Errorf("%w: HTTP %d requires application/json", ErrInvalidResponse, response.StatusCode)
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode < 400 || response.StatusCode > 599 {
			return zero, fmt.Errorf("%w: unexpected HTTP status %d", ErrInvalidResponse, response.StatusCode)
		}
		rejection, err := protocol.DecodeAndValidate[protocol.ErrorResponse](response.Body, c.maxResponseBytes)
		if err != nil {
			return zero, fmt.Errorf("%w: HTTP %d: %w", ErrInvalidResponse, response.StatusCode, err)
		}
		return zero, &HTTPError{StatusCode: response.StatusCode, Code: rejection.Code, Message: rejection.Message}
	}
	value, err := protocol.DecodeAndValidate[T](response.Body, c.maxResponseBytes)
	if err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidResponse, err)
	}
	return value, nil
}
