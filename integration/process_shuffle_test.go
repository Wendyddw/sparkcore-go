package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/protocol"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// processLog retains diagnostics and provides observable readiness without sleeps.
type processLog struct {
	mu      sync.Mutex
	pending []byte
	events  []map[string]any
	wake    chan struct{}
}

func (l *processLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pending = append(l.pending, p...)
	for {
		i := bytes.IndexByte(l.pending, '\n')
		if i < 0 {
			break
		}
		var event map[string]any
		if err := json.Unmarshal(l.pending[:i], &event); err == nil {
			l.events = append(l.events, event)
		}
		l.pending = l.pending[i+1:]
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return len(p), nil
}
func (l *processLog) snapshot() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]map[string]any(nil), l.events...)
}
func (l *processLog) wait(t *testing.T, ctx context.Context, match func(map[string]any) bool) map[string]any {
	t.Helper()
	for {
		for _, e := range l.snapshot() {
			if match(e) {
				return e
			}
		}
		select {
		case <-l.wake:
		case <-ctx.Done():
			t.Fatalf("waiting for process event: %v; logs=%v", ctx.Err(), l.snapshot())
		}
	}
}

type commandProcess struct {
	cmd     *exec.Cmd
	logs    *processLog
	done    chan struct{}
	err     error // Read after done closes.
	stdout  bytes.Buffer
	stopped bool
}

func startCommand(t *testing.T, ctx context.Context, binary, dir string, args ...string) *commandProcess {
	t.Helper()
	p := &commandProcess{cmd: exec.CommandContext(ctx, binary, args...), logs: &processLog{wake: make(chan struct{}, 1)}, done: make(chan struct{})}
	p.cmd.Dir = dir
	p.cmd.Stderr, p.cmd.Stdout = p.logs, &p.stdout
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.stop(t, false) })
	return p
}
func (p *commandProcess) stop(t *testing.T, kill bool) {
	t.Helper()
	if p.stopped {
		return
	}
	p.stopped = true
	if kill {
		_ = p.cmd.Process.Kill()
	} else {
		_ = p.cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		t.Errorf("process did not stop on signal: %v", p.logs.snapshot())
	}
	if !kill && p.err != nil {
		t.Errorf("process failed: %v; logs=%v", p.err, p.logs.snapshot())
	}
	if p.stdout.Len() != 0 {
		t.Errorf("daemon wrote stdout: %s", &p.stdout)
	}
}

func submitProcessJob(ctx context.Context, client *http.Client, origin, path string, action scheduler.ActionKind) (protocol.JobResultResponse, error) {
	body, err := json.Marshal(shuffleSpec(path, action))
	if err != nil {
		return protocol.JobResultResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, origin+protocol.SubmitJobPath, bytes.NewReader(body))
	if err != nil {
		return protocol.JobResultResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		return protocol.JobResultResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return protocol.JobResultResponse{}, fmt.Errorf("submit: %s: %s", response.Status, data)
	}
	return protocol.DecodeAndValidate[protocol.JobResultResponse](response.Body, 1<<20)
}

// Build and run the real commands. A test-only HTTP proxy holds the first map
// success before it reaches the coordinator; no production failure hook is needed.
func TestCommandProcessesShuffleAndWorkerLoss(t *testing.T) {
	project, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	binaries := t.TempDir()
	buildCtx, cancelBuild := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-race", "-o", binaries, "./cmd/coordinator", "./cmd/worker")
	build.Dir = project
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build commands: %v\n%s", err, output)
	}
	for _, loseWorker := range []bool{false, true} {
		t.Run(fmt.Sprintf("worker_loss=%t", loseWorker), func(t *testing.T) {
			root := t.TempDir() // Removed only after process cleanups registered below.
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			t.Cleanup(cancel)
			coordinator := startCommand(t, ctx, filepath.Join(binaries, "coordinator"), project,
				"--listen", "127.0.0.1:0", "--worker-timeout", "1s", "--worker-check-interval", "50ms",
				"--job-timeout", "15s", "--max-task-attempts", "2", "--max-stage-attempts", "2")
			listening := coordinator.logs.wait(t, ctx, func(e map[string]any) bool { return e["msg"] == "coordinator_listening" })
			origin := "http://" + listening["address"].(string)
			upstream, err := url.Parse(origin)
			if err != nil {
				t.Fatal(err)
			}
			proxy := httputil.NewSingleHostReverseProxy(upstream)
			proxyTransport := http.DefaultTransport.(*http.Transport).Clone()
			proxy.Transport = proxyTransport
			t.Cleanup(proxyTransport.CloseIdleConnections)
			held, release, abort := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var abortOnce sync.Once
			stopGate := func() { abortOnce.Do(func() { close(abort) }) }
			var gated atomic.Bool
			gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == protocol.TaskSuccessPath && gated.CompareAndSwap(false, true) {
					close(held)
					select {
					case <-release:
					case <-abort:
						return
					case <-ctx.Done():
						return
					case <-r.Context().Done():
						return
					}
					if r.Context().Err() != nil {
						return
					}
				}
				proxy.ServeHTTP(w, r)
			}))
			t.Cleanup(func() { stopGate(); gate.Close() })
			workerArgs := func(origin, id string, slots string) []string {
				return []string{"--coordinator", origin, "--id", id, "--slots", slots, "--shuffle-root", root,
					"--heartbeat-interval", "10ms", "--request-timeout", "10s"}
			}
			first := startCommand(t, ctx, filepath.Join(binaries, "worker"), project, workerArgs(gate.URL, "first", "1")...)
			first.logs.wait(t, ctx, func(e map[string]any) bool { return e["msg"] == "worker_registered" })
			client := &http.Client{Timeout: 20 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			type outcome struct {
				result protocol.JobResultResponse
				err    error
			}
			completed := make(chan outcome, 1)
			go func() {
				r, err := submitProcessJob(ctx, client, origin, "examples/input.txt", scheduler.ActionCollect)
				completed <- outcome{r, err}
			}()
			assigned := coordinator.logs.wait(t, ctx, func(e map[string]any) bool { return e["msg"] == "task_assigned" && e["worker_id"] == "first" })
			select {
			case <-held:
			case <-ctx.Done():
				t.Fatal("first map never published")
			}
			survivor := startCommand(t, ctx, filepath.Join(binaries, "worker"), project, workerArgs(origin, "survivor", "2")...)
			coordinator.logs.wait(t, ctx, func(e map[string]any) bool { return e["msg"] == "task_assigned" && e["worker_id"] == "survivor" })
			if loseWorker {
				first.stop(t, true)
				stopGate()
				coordinator.logs.wait(t, ctx, func(e map[string]any) bool { return e["msg"] == "worker_lost" && e["worker_id"] == "first" })
			} else {
				close(release)
			}
			var got outcome
			select {
			case got = <-completed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if got.err != nil {
				t.Fatal(got.err)
			}
			var records []string
			for _, record := range got.result.Records {
				records = append(records, string(record))
			}
			sort.Strings(records)
			want := []string{`{"key":"alpha","value":2}`, `{"key":"beta","value":2}`, `{"key":"gamma","value":1}`}
			if !reflect.DeepEqual(records, want) {
				t.Fatalf("Collect=%v", records)
			}
			accepted, retries, mapStage, resultStage := 0, 0, 0, 0
			for _, event := range coordinator.logs.snapshot() {
				switch event["msg"] {
				case "shuffle_output_accepted":
					accepted++
					if loseWorker && event["attempt_id"] == assigned["attempt_id"] {
						t.Fatal("lost map output was accepted")
					}
				case "task_requeued":
					retries++
				case "stage_started":
					if event["stage_id"] == assigned["stage_id"] {
						mapStage++
					} else {
						if accepted != 4 {
							t.Fatal("reducers started before four map outputs were accepted")
						}
						resultStage++
					}
				}
			}
			wantRetries := 0
			if loseWorker {
				wantRetries = 1
			}
			if accepted != 4 || retries != wantRetries || mapStage != 1 || resultStage != 1 {
				t.Fatalf("accepted=%d retries=%d map/result stages=%d/%d", accepted, retries, mapStage, resultStage)
			}
			// Both successful and unaccepted attempts retain distinct publication paths.
			manifests := 0
			if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.Name() == "manifest.json" {
					manifests++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if manifests != 4+wantRetries {
				t.Fatalf("published map attempts=%d", manifests)
			}
			count, err := submitProcessJob(ctx, client, origin, "examples/input.txt", scheduler.ActionCount)
			if err != nil || count.Count != 3 {
				t.Fatalf("Count=%d, %v", count.Count, err)
			}
			empty := filepath.Join(t.TempDir(), "empty.txt")
			if err := os.WriteFile(empty, nil, 0600); err != nil {
				t.Fatal(err)
			}
			for _, action := range []scheduler.ActionKind{scheduler.ActionCollect, scheduler.ActionCount} {
				result, err := submitProcessJob(ctx, client, origin, empty, action)
				if err != nil || result.Count != 0 || len(result.Records) != 0 {
					t.Fatalf("empty %s = %+v, %v", action, result, err)
				}
			}
			survivor.stop(t, false)
			if !loseWorker {
				first.stop(t, false)
			}
			coordinator.stop(t, false)
		})
	}
}
