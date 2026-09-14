package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/Wendyddw/sparkcore-go/protocol"
)

func submissionURL(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawPath != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return "", fmt.Errorf("coordinator URL must be an absolute HTTP(S) origin without credentials, query, or fragment")
	}
	return strings.TrimSuffix(u.String(), "/") + protocol.SubmitJobPath, nil
}

func submitJob(ctx context.Context, endpoint string, spec protocol.SubmitJobRequest, maxBytes int64) (protocol.JobResultResponse, error) {
	var zero protocol.JobResultResponse
	data, err := json.Marshal(spec)
	if err != nil {
		return zero, fmt.Errorf("encode job: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return zero, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	response, err := client.Do(req)
	if err != nil {
		return zero, fmt.Errorf("submit job: %w", err)
	}
	defer response.Body.Close()
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return zero, fmt.Errorf("invalid coordinator response: HTTP %d requires application/json", response.StatusCode)
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode < 400 || response.StatusCode > 599 {
			return zero, fmt.Errorf("unexpected coordinator HTTP status %d", response.StatusCode)
		}
		rejection, err := protocol.DecodeAndValidate[protocol.ErrorResponse](response.Body, maxBytes)
		if err != nil {
			return zero, fmt.Errorf("invalid coordinator HTTP %d error: %w", response.StatusCode, err)
		}
		return zero, fmt.Errorf("coordinator HTTP %d (%s): %s", response.StatusCode, rejection.Code, rejection.Message)
	}
	result, err := protocol.DecodeAndValidate[protocol.JobResultResponse](response.Body, maxBytes)
	if err != nil {
		return zero, fmt.Errorf("invalid job result: %w", err)
	}
	if result.Action != spec.Action {
		return zero, fmt.Errorf("job result action %q differs from submitted action %q", result.Action, spec.Action)
	}
	return result, nil
}
