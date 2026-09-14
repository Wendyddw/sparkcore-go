package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func jobFile(t *testing.T, action string) string {
	t.Helper()
	return writeJob(t, fmt.Sprintf(`{"source":{"path":"/worker-only/input.txt","num_partitions":4},"transformations":[{"kind":"map","function_id":"normalize"}],"action":%q}`, action))
}

func writeJob(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "job.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunPrintsOnlyFinalResult(t *testing.T) {
	for _, test := range []struct{ action, response, want string }{
		{"count", `{"action":"count","records":null,"count":9007199254740993}`, "9007199254740993\n"},
		{"collect", `{"action":"collect","records":[9007199254740993,{"key":"x","value":9223372036854775807},null],"count":0}`, "[9007199254740993,{\"key\":\"x\",\"value\":9223372036854775807},null]\n"},
		{"collect", `{"action":"collect","records":[],"count":0}`, "[]\n"},
	} {
		var calls atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if r.Method != "POST" || r.URL.Path != protocol.SubmitJobPath || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json" {
				t.Errorf("unexpected request: %s %s %v", r.Method, r.URL, r.Header)
			}
			spec, err := protocol.DecodeAndValidate[protocol.SubmitJobRequest](r.Body, 1<<20)
			if err != nil || spec.Source.Path != "/worker-only/input.txt" || spec.Source.NumPartitions != 4 || spec.Action != scheduler.ActionKind(test.action) {
				t.Errorf("job = %+v, %v", spec, err)
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			fmt.Fprint(w, test.response)
		}))
		var stdout, stderr bytes.Buffer
		err := run(context.Background(), []string{"--coordinator", ts.URL + "/", "--job", jobFile(t, test.action)}, &stdout, &stderr)
		ts.Close()
		if err != nil || stdout.String() != test.want || stderr.Len() != 0 || calls.Load() != 1 {
			t.Fatalf("stdout=%q stderr=%q error=%v calls=%d", stdout.String(), stderr.String(), err, calls.Load())
		}
	}
}

func TestRunRejectsConfigurationAndJobBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer ts.Close()
	valid := jobFile(t, "count")
	for _, args := range [][]string{
		nil, {"--job", valid, "extra"}, {"--unknown"}, {"--job", valid, "--request-timeout", "0"},
		{"--job", valid, "--max-response-bytes", "0"}, {"--job", valid, "--max-response-bytes", "9223372036854775807"},
		{"--job", valid + ".missing"}, {"--job", writeJob(t, "{")},
		{"--job", writeJob(t, `{"source":{"path":"x"},"action":"count"}`)},
		{"--job", writeJob(t, `{"source":{"path":"x","num_partitions":1},"action":"count","action":"collect"}`)},
		{"--job", writeJob(t, strings.Repeat(" ", 1<<20+1))},
	} {
		var output bytes.Buffer
		err := run(context.Background(), append([]string{"--coordinator", ts.URL}, args...), &output, io.Discard)
		if err == nil || output.Len() != 0 {
			t.Fatalf("args=%v output=%q error=%v", args, output.String(), err)
		}
	}
	for _, origin := range []string{"", "ftp://example.com", "http://user:pass@example.com", ts.URL + "/jobs", ts.URL + "?x=y", ts.URL + "#fragment"} {
		if err := run(context.Background(), []string{"--coordinator", origin, "--job", valid}, io.Discard, io.Discard); err == nil {
			t.Fatalf("invalid URL accepted: %q", origin)
		}
	}
	var output, help bytes.Buffer
	if err := run(context.Background(), []string{"--help"}, &output, &help); !errors.Is(err, flag.ErrHelp) || output.Len() != 0 || !strings.Contains(help.String(), "-job") {
		t.Fatalf("help = %q, stdout = %q, err = %v", help.String(), output.String(), err)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached coordinator")
	}
}

func TestRunRejectsResponsesWithoutPartialOutput(t *testing.T) {
	for _, test := range []struct {
		status                int
		body, mediaType, want string
	}{
		{422, `{"code":"job_failed","message":"partition failed"}`, "application/json", "HTTP 422 (job_failed): partition failed"},
		{503, `{"code":"unavailable","message":"stopping"}`, "application/json", "HTTP 503 (unavailable)"},
		{504, `{"code":"job_failed","message":"deadline"}`, "application/json", "HTTP 504 (job_failed)"},
		{500, "oops", "application/json", "invalid coordinator"},
		{200, "{}", "text/plain", "application/json"},
		{200, `{"action":"count","records":null}`, "application/json", "invalid job result"},
		{200, `{"action":"count","records":null,"count":-1}`, "application/json", "invalid job result"},
		{200, `{"action":"count","records":null,"count":5}{}`, "application/json", "invalid job result"},
		{200, `{"action":"collect","records":[],"count":0}`, "application/json", "differs from submitted action"},
		{201, `{"action":"count","records":null,"count":5}`, "application/json", "unexpected coordinator HTTP status"},
	} {
		var calls atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", test.mediaType)
			w.WriteHeader(test.status)
			fmt.Fprint(w, test.body)
		}))
		var output bytes.Buffer
		err := run(context.Background(), []string{"--coordinator", ts.URL, "--job", jobFile(t, "count")}, &output, io.Discard)
		ts.Close()
		if err == nil || !strings.Contains(err.Error(), test.want) || output.Len() != 0 || calls.Load() != 1 {
			t.Fatalf("status=%d err=%v output=%q calls=%d", test.status, err, output.String(), calls.Load())
		}
	}
}

func TestRunEnforcesResponseLimitAndDoesNotFollowRedirect(t *testing.T) {
	body := `{"action":"count","records":null,"count":5}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer ts.Close()
	path := jobFile(t, "count")
	for _, size := range []int{len(body), len(body) - 1} {
		var output bytes.Buffer
		err := run(context.Background(), []string{"--coordinator", ts.URL, "--job", path, "--max-response-bytes", fmt.Sprint(size)}, &output, io.Discard)
		if size == len(body) {
			if err != nil || output.String() != "5\n" {
				t.Fatalf("boundary response: %q %v", output.String(), err)
			}
		} else if !errors.Is(err, protocol.ErrMessageTooLarge) || output.Len() != 0 {
			t.Fatalf("oversized response: %q %v", output.String(), err)
		}
	}
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Store(true) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	if err := run(context.Background(), []string{"--coordinator", redirect.URL, "--job", path}, io.Discard, io.Discard); err == nil || redirected.Load() {
		t.Fatalf("redirect followed or accepted: %v", err)
	}
}

func TestRunTimeoutCoversResponseBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"action":"count",`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer ts.Close()
	var output bytes.Buffer
	err := run(context.Background(), []string{"--coordinator", ts.URL, "--job", jobFile(t, "count"), "--request-timeout", "50ms"}, &output, io.Discard)
	if !errors.Is(err, context.DeadlineExceeded) || output.Len() != 0 {
		t.Fatalf("partial output=%q timeout error=%v", output.String(), err)
	}
}
