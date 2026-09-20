package shuffle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func testStore(t *testing.T, path string) *Filesystem {
	t.Helper()
	s, err := NewFilesystem(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return s
}

func testIdentity() AttemptIdentity { return AttemptIdentity{RunID: strings.Repeat("a", 32)} }

func publishRecords(t *testing.T, s *Filesystem, a AttemptIdentity, maps, reduces int, records ...Record) MapOutput {
	t.Helper()
	w, err := s.Begin(context.Background(), a, maps, reduces)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	for _, record := range records {
		if err := w.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	output, err := w.Publish()
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func readBucket(t *testing.T, s *Filesystem, output MapOutput, partition plan.PartitionID) []Record {
	t.Helper()
	r, err := s.OpenBucket(context.Background(), output, partition)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var records []Record
	for {
		record, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if _, err := r.(*fileBucketReader).file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("file not closed after EOF: %v", err)
	}
	return records
}

func TestFilesystemIndependentStoresRoundTrip(t *testing.T) {
	path := t.TempDir()
	writerStore, readerStore := testStore(t, path), testStore(t, path)
	records := []Record{{"apple", json.Number("9007199254740993")}, {"banana", nil}, {"", "empty key"}, {"世界", true}}
	output := publishRecords(t, writerStore, testIdentity(), 1, 7, records...)
	for partition := 0; partition < 7; partition++ {
		var want []Record
		for _, record := range records {
			bucket, _ := PartitionFor(record.Key, 7)
			if int(bucket) == partition {
				want = append(want, record)
			}
		}
		got := readBucket(t, readerStore, output, plan.PartitionID(partition))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("bucket %d: got %#v, want %#v", partition, got, want)
		}
		data, err := os.ReadFile(filepath.Join(path, attemptPath(output.Attempt), bucketName(plan.PartitionID(partition))))
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		bucket := output.Buckets[partition]
		if bucket.ByteCount != int64(len(data)) || bucket.RecordCount != int64(len(want)) || bucket.SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatalf("wrong bucket metadata: %+v", bucket)
		}
	}
}

func TestFilesystemUnpublishedAndEmptyOutput(t *testing.T) {
	s := testStore(t, t.TempDir())
	w, err := s.Begin(context.Background(), testIdentity(), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	expected, _ := initialOutput(testIdentity(), 1, 3)
	if _, err := s.OpenBucket(context.Background(), expected, 0); !errors.Is(err, os.ErrNotExist) || !errors.Is(err, ErrShuffleInput) {
		t.Fatalf("private attempt was readable: %v", err)
	}
	if _, err := s.root.Stat(attemptPath(testIdentity())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("final directory exposed before publication: %v", err)
	}
	output, err := w.Publish()
	if err != nil {
		t.Fatal(err)
	}
	for partition := 0; partition < 3; partition++ {
		if got := readBucket(t, s, output, plan.PartitionID(partition)); len(got) != 0 {
			t.Fatal("empty map produced records")
		}
	}
	if err := s.root.Remove(filepath.Join(attemptPath(output.Attempt), bucketName(0))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenBucket(context.Background(), output, 0); !errors.Is(err, os.ErrNotExist) || !errors.Is(err, ErrShuffleInput) {
		t.Fatalf("missing empty bucket treated as empty data: %v", err)
	}
}

func TestFilesystemAttemptIsolation(t *testing.T) {
	s := testStore(t, t.TempDir())
	identities := []AttemptIdentity{testIdentity(), testIdentity(), testIdentity(), testIdentity(), testIdentity()}
	identities[1].TaskAttemptID = 1
	identities[2].JobID = 1
	identities[3].RunID = strings.Repeat("b", 32)
	identities[4].StageAttemptID = 1
	var outputs []MapOutput
	for i, identity := range identities {
		outputs = append(outputs, publishRecords(t, s, identity, 1, 1, Record{Key: "a", Value: i}))
	}
	for i, output := range outputs {
		got := readBucket(t, s, output, 0)
		want, _ := json.Marshal(i)
		if len(got) != 1 || got[0].Value != json.Number(string(want)) {
			t.Fatalf("attempt output overwritten: %+v", got)
		}
	}
}

func TestFilesystemConcurrentPublicationDoesNotOverwrite(t *testing.T) {
	path := t.TempDir()
	a, b := testStore(t, path), testStore(t, path)
	writers := make([]AttemptWriter, 2)
	for i, s := range []*Filesystem{a, b} {
		w, err := s.Begin(context.Background(), testIdentity(), 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer w.Abort()
		if err := w.Write(Record{"key", i}); err != nil {
			t.Fatal(err)
		}
		writers[i] = w
	}
	type result struct {
		output MapOutput
		err    error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for _, w := range writers {
		go func() { <-start; out, err := w.Publish(); results <- result{out, err} }()
	}
	close(start)
	var winner MapOutput
	wins, conflicts := 0, 0
	for range writers {
		r := <-results
		if r.err == nil {
			wins++
			winner = r.output
		} else if errors.Is(r.err, ErrOutputExists) {
			conflicts++
		} else {
			t.Fatal(r.err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	if got := readBucket(t, a, winner, 0); len(got) != 1 {
		t.Fatalf("winner not intact: %+v", got)
	}
	if _, err := b.Begin(context.Background(), testIdentity(), 1, 1); !errors.Is(err, ErrOutputExists) {
		t.Fatalf("duplicate Begin: %v", err)
	}
	entries, err := a.root.ReadFile(filepath.Join(attemptPath(winner.Attempt), "manifest.json"))
	if err != nil || len(entries) == 0 {
		t.Fatalf("winner manifest missing: %v", err)
	}
	for _, w := range writers {
		if _, err := a.root.Stat(w.(*fileWriter).private); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("private directory retained: %v", err)
		}
	}
}

func TestFilesystemCorruptInput(t *testing.T) {
	for _, kind := range []string{"manifest", "missing zero field", "truncated", "checksum", "record count", "wrong partition", "malformed", "missing manifest"} {
		t.Run(kind, func(t *testing.T) {
			s := testStore(t, t.TempDir())
			output := publishRecords(t, s, testIdentity(), 1, 2, Record{"a", 1})
			partition, _ := PartitionFor("a", 2)
			dir := attemptPath(output.Attempt)
			bucketPath, manifestPath := filepath.Join(dir, bucketName(partition)), filepath.Join(dir, "manifest.json")
			data, err := s.root.ReadFile(bucketPath)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "manifest":
				mustWrite(t, s, manifestPath, []byte(`{}`))
			case "missing zero field":
				manifest, _ := s.root.ReadFile(manifestPath)
				mustWrite(t, s, manifestPath, []byte(strings.Replace(string(manifest), `"job_id":0,`, "", 1)))
			case "missing manifest":
				if err := s.root.Remove(manifestPath); err != nil {
					t.Fatal(err)
				}
			case "truncated":
				mustWrite(t, s, bucketPath, data[:len(data)-1])
			case "checksum":
				mustWrite(t, s, bucketPath, []byte(strings.Replace(string(data), `:1`, `:2`, 1)))
			case "record count":
				output.Buckets[partition].RecordCount++
				manifest, _ := manifestBytes(output)
				mustWrite(t, s, manifestPath, manifest)
			case "wrong partition", "malformed":
				if kind == "wrong partition" {
					data, err = EncodeRecord(Record{"b", 1}) // a and b hash into different buckets modulo 2.
					if err != nil {
						t.Fatal(err)
					}
				} else {
					data = []byte("not json\n")
				}
				mustWrite(t, s, bucketPath, data)
				digest := sha256.Sum256(data)
				output.Buckets[partition].ByteCount = int64(len(data))
				output.Buckets[partition].SHA256 = hex.EncodeToString(digest[:])
				manifest, _ := manifestBytes(output)
				mustWrite(t, s, manifestPath, manifest)
			}
			r, err := s.OpenBucket(context.Background(), output, partition)
			if err == nil {
				defer r.Close()
				for err == nil {
					_, err = r.Next()
				}
				if _, statErr := r.(*fileBucketReader).file.Stat(); !errors.Is(statErr, os.ErrClosed) {
					t.Fatalf("error left file open: %v", statErr)
				}
			}
			var input *InputError
			if !errors.Is(err, ErrShuffleInput) || !errors.As(err, &input) || input.Attempt != output.Attempt || input.PartitionID != partition {
				t.Fatalf("missing identified input error: %v", err)
			}
		})
	}
}

func mustWrite(t *testing.T, s *Filesystem, path string, data []byte) {
	t.Helper()
	if err := s.root.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestFilesystemCancellationAndAbort(t *testing.T) {
	s := testStore(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	w, err := s.Begin(ctx, testIdentity(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.Write(Record{"a", 1}); err != nil {
		t.Fatal(err)
	}
	cancel()
	awaitClosed(t, w.(*fileWriter).done)
	if _, err := w.Publish(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Publish: %v", err)
	}
	if _, err := s.root.Stat(w.(*fileWriter).private); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private files retained: %v", err)
	}
	output := publishRecords(t, s, testIdentity(), 1, 1, Record{"a", 1})
	ctx, cancel = context.WithCancel(context.Background())
	r, err := s.OpenBucket(ctx, output, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cancel()
	awaitClosed(t, r.(*fileBucketReader).done)
	if _, err := r.Next(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Next: %v", err)
	}
	if _, err := r.(*fileBucketReader).file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("cancellation left file open: %v", err)
	}
}

func awaitClosed(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handle did not close")
	}
}

func TestFilesystemWriteFailureAborts(t *testing.T) {
	s := testStore(t, t.TempDir())
	w, err := s.Begin(context.Background(), testIdentity(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := w.Write(Record{"a", 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(Record{"a", make(chan int)}); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("invalid write: %v", err)
	}
	if _, err := w.Publish(); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("failed writer published: %v", err)
	}
	if _, err := s.root.Stat(w.(*fileWriter).private); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed writer left private output: %v", err)
	}
}

func TestFilesystemCleanupAndLifetime(t *testing.T) {
	s := testStore(t, t.TempDir())
	a := testIdentity()
	w, err := s.Begin(context.Background(), a, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if err := s.Close(); !errors.Is(err, ErrStoreBusy) {
		t.Fatalf("closed active store: %v", err)
	}
	if err := s.CleanupRun(a.RunID); !errors.Is(err, ErrStoreBusy) {
		t.Fatalf("cleaned active run: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(); err != nil {
		t.Fatal(err)
	}
	output := publishRecords(t, s, a, 1, 1)
	b := a
	b.RunID = strings.Repeat("b", 32)
	other := publishRecords(t, s, b, 1, 1)
	r, err := s.OpenBucket(context.Background(), output, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := s.CleanupRun(a.RunID); !errors.Is(err, ErrStoreBusy) {
		t.Fatalf("cleaned active reader: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupRun(a.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.root.Stat(a.RunID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run remains: %v", err)
	}
	readBucket(t, s, other, 0)
	if err := s.CleanupRun("../"); err == nil {
		t.Fatal("accepted unsafe cleanup")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(context.Background(), a, 1, 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("Begin after Close: %v", err)
	}
}

func TestFilesystemConfinement(t *testing.T) {
	path, outside := t.TempDir(), t.TempDir()
	s := testStore(t, path)
	a := testIdentity()
	if err := os.Symlink(outside, filepath.Join(path, a.RunID)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(context.Background(), a, 1, 1); err == nil {
		t.Fatal("write escaped through symlink")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("outside changed: %v, %v", entries, err)
	}
	if err := s.CleanupRun(a.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("cleanup removed symlink target: %v", err)
	}
}

func TestFilesystemConcurrentCancellation(t *testing.T) {
	s := testStore(t, t.TempDir())
	for attempt := 0; attempt < 10; attempt++ {
		a := testIdentity()
		a.TaskAttemptID = plan.TaskAttemptID(attempt)
		ctx, cancel := context.WithCancel(context.Background())
		w, err := s.Begin(ctx, a, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		var tasks sync.WaitGroup
		tasks.Add(2)
		go func() { defer tasks.Done(); _ = w.Write(Record{"a", 1}); _, _ = w.Publish() }()
		go func() { defer tasks.Done(); cancel(); _ = w.Abort() }()
		tasks.Wait()
		awaitClosed(t, w.(*fileWriter).done)
	}
}

func TestFilesystemStorageFailureCannotPublish(t *testing.T) {
	for _, target := range []string{"bucket", "manifest"} {
		t.Run(target, func(t *testing.T) {
			s := testStore(t, t.TempDir())
			w, err := s.Begin(context.Background(), testIdentity(), 1, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Abort()
			name := bucketName(0)
			if target == "manifest" {
				name = "manifest.json"
			}
			// A directory at the file path produces a deterministic filesystem error.
			if err := s.root.Mkdir(filepath.Join(w.(*fileWriter).private, name), 0700); err != nil {
				t.Fatal(err)
			}
			if target == "bucket" {
				if err := w.Write(Record{"a", 1}); err == nil {
					t.Fatal("write succeeded despite storage failure")
				}
			}
			if _, err := w.Publish(); err == nil {
				t.Fatal("failed attempt published")
			}
			if _, err := s.root.Stat(w.(*fileWriter).private); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("temporary output survived failure: %v", err)
			}
			if _, err := s.root.Stat(attemptPath(testIdentity())); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed output became visible: %v", err)
			}
		})
	}
}

func TestFilesystemRejectsInvalidRequestsBeforeWriting(t *testing.T) {
	if s, err := NewFilesystem("relative"); err == nil {
		s.Close()
		t.Fatal("accepted relative storage root")
	}
	s := testStore(t, t.TempDir())
	for _, counts := range [][2]int{{0, 1}, {1, 0}, {1, 1 << 30}} {
		if _, err := s.Begin(context.Background(), testIdentity(), counts[0], counts[1]); err == nil {
			t.Fatalf("accepted counts %v", counts)
		}
	}
	if _, err := s.root.Stat(testIdentity().RunID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid request created run files: %v", err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("job stopped")
	cancel(cause)
	if _, err := s.Begin(ctx, testIdentity(), 1, 1); !errors.Is(err, cause) {
		t.Fatalf("lost cancellation cause: %v", err)
	}
	output := publishRecords(t, s, testIdentity(), 1, 1)
	if _, err := s.OpenBucket(context.Background(), output, 1); err == nil {
		t.Fatal("accepted out-of-range reduce bucket")
	}
}

func TestFilesystemReaderCopiesAcceptedMetadata(t *testing.T) {
	s := testStore(t, t.TempDir())
	output := publishRecords(t, s, testIdentity(), 1, 1, Record{"a", 1})
	r, err := s.OpenBucket(context.Background(), output, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	output.Buckets[0].RecordCount = 99
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("reader aliased caller's metadata: %v", err)
	}
}
