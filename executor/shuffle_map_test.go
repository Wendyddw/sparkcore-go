package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func TestShuffleMapCombiningAndAttemptIsolation(t *testing.T) {
	for _, combine := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncombined", true: "combined"}[combine], func(t *testing.T) {
			path := t.TempDir()
			store, readerStore := mapTestStore(t, path), mapTestStore(t, path)
			registry := NewFunctionRegistry()
			if err := registry.RegisterReduce("sum", func(a, b Record) (Record, error) {
				return a.(int64) + b.(int64), nil
			}); err != nil {
				t.Fatal(err)
			}
			sources := memorySourceReader{partitions: map[plan.PartitionID][]Record{
				0: {KeyValue{"alpha", int64(9007199254740993)}, KeyValue{"beta", int64(1)}, KeyValue{"alpha", int64(1)}},
				1: {KeyValue{"alpha", int64(1)}, KeyValue{"beta", int64(1)}, KeyValue{"世界", int64(1)}},
				2: {},
			}}
			runner := NewLocalRunner(registry, sources, 2, WithShuffleStore(store))
			sums := make(map[string]int64)
			var written int64
			for partition := 0; partition < 3; partition++ {
				execution := mapExecution(combine)
				execution.Task.PartitionID = plan.PartitionID(partition)
				execution.Task.ID = plan.TaskID(partition)
				execution.Attempt.TaskID = execution.Task.ID
				output, err := runner.RunTask(context.Background(), execution)
				if err != nil {
					t.Fatal(err)
				}
				if output.ShuffleOutput == nil || output.Count != 0 || output.Records != nil {
					t.Fatalf("expected only shuffle output: %+v", output)
				}
				manifest := *output.ShuffleOutput
				wantIdentity := shuffle.AttemptIdentity{RunID: execution.RunID, JobID: 7, ShuffleID: 4,
					StageID: 3, StageAttemptID: 5, TaskID: plan.TaskID(partition), TaskAttemptID: 9,
					MapPartitionID: plan.PartitionID(partition)}
				if manifest.Attempt != wantIdentity || manifest.NumMapPartitions != 3 || manifest.NumReducePartitions != 4 {
					t.Fatalf("unexpected descriptor: %+v", manifest)
				}
				if err := manifest.Validate(); err != nil {
					t.Fatal(err)
				}
				for _, bucket := range manifest.Buckets {
					written += bucket.RecordCount
					if partition == 2 && (bucket.RecordCount != 0 || bucket.ByteCount != 0) {
						t.Fatalf("empty map produced data: %+v", bucket)
					}
					reader, err := readerStore.OpenBucket(context.Background(), manifest, bucket.PartitionID)
					if err != nil {
						t.Fatal(err)
					}
					var keys []string
					for {
						record, err := reader.Next()
						if err == io.EOF {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						value, err := record.Value.(json.Number).Int64()
						if err != nil {
							t.Fatal(err)
						}
						sums[record.Key] += value
						keys = append(keys, record.Key)
					}
					if err := reader.Close(); err != nil {
						t.Fatal(err)
					}
					if combine && !sort.StringsAreSorted(keys) {
						t.Fatalf("combined keys are not sorted: %v", keys)
					}
				}
				// A duplicate cannot overwrite output; a new attempt has its own namespace.
				if _, err := runner.RunTask(context.Background(), execution); !errors.Is(err, shuffle.ErrOutputExists) {
					t.Fatalf("duplicate publication: %v", err)
				}
				execution.Attempt.ID++
				retry, err := runner.RunTask(context.Background(), execution)
				if err != nil || !reflect.DeepEqual(retry.ShuffleOutput.Buckets, manifest.Buckets) {
					t.Fatalf("retry changed bucket contents: %+v, %v", retry, err)
				}
				if retry.ShuffleOutput.Attempt.TaskAttemptID != execution.Attempt.ID {
					t.Fatal("retry reused the previous attempt identity")
				}
			}
			if want := map[string]int64{"alpha": 9007199254740995, "beta": 2, "世界": 1}; !reflect.DeepEqual(sums, want) {
				t.Fatalf("combined totals = %v, want %v", sums, want)
			}
			wantWritten := int64(6)
			if combine {
				wantWritten = 5
			}
			if written != wantWritten {
				t.Fatalf("wrote %d records, want %d", written, wantWritten)
			}
		})
	}
}

func TestShuffleMapRejectsInvalidConfigurationBeforeSourceAccess(t *testing.T) {
	for name, change := range map[string]func(*scheduler.TaskExecution){
		"missing write": func(e *scheduler.TaskExecution) { e.Task.ShuffleWrite = nil },
		"final action": func(e *scheduler.TaskExecution) {
			e.Task.FinalAction = &scheduler.ActionSpec{Kind: scheduler.ActionCount}
		},
		"partition":       func(e *scheduler.TaskExecution) { e.Task.PartitionID = 3 },
		"map count":       func(e *scheduler.TaskExecution) { e.Task.NumPartitions = 0 },
		"partitioner":     func(e *scheduler.TaskExecution) { e.Task.ShuffleWrite.Partitioner.Kind = "unknown" },
		"reduce count":    func(e *scheduler.TaskExecution) { e.Task.ShuffleWrite.Partitioner.NumPartitions = 0 },
		"missing reducer": func(e *scheduler.TaskExecution) { e.Task.ShuffleWrite.MapSideCombine = true },
	} {
		t.Run(name, func(t *testing.T) {
			source := &identityGuardSource{}
			execution := mapExecution(false)
			change(&execution)
			runner := NewLocalRunner(NewFunctionRegistry(), source, 1, WithShuffleStore(mapTestStore(t, t.TempDir())))
			if output, err := runner.RunTask(context.Background(), execution); err == nil || source.reads != 0 || output.ShuffleOutput != nil {
				t.Fatalf("invalid task reached execution: output=%+v err=%v reads=%d", output, err, source.reads)
			}
		})
	}
	for _, canceled := range []bool{false, true} {
		source := &identityGuardSource{}
		ctx, cancel := context.WithCancel(context.Background())
		if canceled {
			cancel()
		}
		_, err := NewLocalRunner(NewFunctionRegistry(), source, 1).RunTask(ctx, mapExecution(false))
		cancel()
		if err == nil || source.reads != 0 {
			t.Fatalf("missing store/cancellation reached source: %v, reads=%d", err, source.reads)
		}
	}
}

func TestShuffleMapRunsTextPipelineWithoutCombiner(t *testing.T) {
	store := mapTestStore(t, t.TempDir())
	registry := NewFunctionRegistry()
	if err := registry.RegisterPairMap("pair", func(r Record) (KeyValue, error) {
		return KeyValue{r.(string), 1}, nil
	}); err != nil {
		t.Fatal(err)
	}
	execution := mapExecution(false)
	execution.Task.NumPartitions = 1
	execution.Task.Operations[0].RDD.Operator.SourcePath = writeTextFixture(t, "alpha\nbeta\nalpha\n")
	execution.Task.Operations = append(execution.Task.Operations, runnerOperation(1,
		plan.OperatorSpec{Kind: plan.OpMapToPair, FunctionID: "pair"}))
	// No reducer is registered: uncombined map work must not invoke one.
	output, err := NewLocalRunner(registry, nil, 1, WithShuffleStore(store)).RunTask(context.Background(), execution)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, bucket := range output.ShuffleOutput.Buckets {
		count += bucket.RecordCount
	}
	if count != 3 {
		t.Fatalf("text pipeline wrote %d records, want 3", count)
	}
}

func TestShuffleMapCombinesNullValues(t *testing.T) {
	registry := NewFunctionRegistry()
	var calls int
	if err := registry.RegisterReduce("sum", func(a, b Record) (Record, error) {
		calls++
		if a != nil || b != nil {
			t.Fatalf("unexpected values: %v, %v", a, b)
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	store := mapTestStore(t, t.TempDir())
	source := memorySourceReader{partitions: map[plan.PartitionID][]Record{0: {KeyValue{"", nil}, KeyValue{"", nil}}}}
	output, err := NewLocalRunner(registry, source, 1, WithShuffleStore(store)).RunTask(context.Background(), mapExecution(true))
	if err != nil || calls != 1 {
		t.Fatalf("null values were not combined: calls=%d err=%v", calls, err)
	}
	partition, _ := shuffle.PartitionFor("", 4)
	reader, err := store.OpenBucket(context.Background(), *output.ShuffleOutput, partition)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	record, err := reader.Next()
	if err != nil || record.Key != "" || record.Value != nil {
		t.Fatalf("unexpected combined null record: %+v, %v", record, err)
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("expected verified EOF: %v", err)
	}
}

func TestShuffleMapCancellationAfterPublicationReturnsNoOutput(t *testing.T) {
	store := mapTestStore(t, t.TempDir())
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("stopped after publication")
	var published shuffle.MapOutput
	injected := mapFailureStore{Store: store, afterPublish: func(output shuffle.MapOutput) {
		published = output
		cancel(cause)
	}}
	source := memorySourceReader{partitions: map[plan.PartitionID][]Record{}}
	output, err := NewLocalRunner(NewFunctionRegistry(), source, 1, WithShuffleStore(injected)).RunTask(ctx, mapExecution(false))
	if !errors.Is(err, cause) || !reflect.DeepEqual(output, scheduler.TaskOutput{}) {
		t.Fatalf("canceled map returned success: %+v, %v", output, err)
	}
	// Published orphan files remain readable until explicit inactive-run cleanup.
	reader, err := store.OpenBucket(context.Background(), published, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err != io.EOF {
		t.Fatalf("orphan empty bucket: %v", err)
	}
}

func TestShuffleMapFailuresAbortOutputAndCloseSource(t *testing.T) {
	sentinel := errors.New("injected failure")
	for _, failure := range []string{"source", "pipeline", "function", "reducer", "record", "encoding", "write", "publish", "close", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			path := t.TempDir()
			store := mapTestStore(t, path)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			source := &mapSource{iterator: &mapIteratorProbe{Iterator: &sliceIterator{records: []Record{"a", "a"}}}}
			registry := NewFunctionRegistry()
			mustRegisterRunnerMap(t, registry, "map", func(r Record) (Record, error) { return r, nil })
			mustRegisterRunnerFilter(t, registry, "filter", func(Record) (bool, error) { return true, nil })
			if err := registry.RegisterPairMap("pair", func(r Record) (KeyValue, error) {
				if failure == "function" {
					return KeyValue{}, sentinel
				}
				return KeyValue{r.(string), 1}, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := registry.RegisterValueMap("value", func(r Record) (Record, error) {
				if failure == "encoding" {
					return make(chan int), nil
				}
				if failure == "cancel" {
					cancel(sentinel)
				}
				return r, nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := registry.RegisterReduce("sum", func(a, b Record) (Record, error) { return nil, sentinel }); err != nil {
				t.Fatal(err)
			}
			execution := mapExecution(failure == "reducer")
			execution.Task.Operations = append(execution.Task.Operations,
				runnerOperation(1, plan.OperatorSpec{Kind: plan.OpMap, FunctionID: "map"}),
				runnerOperation(2, plan.OperatorSpec{Kind: plan.OpFilter, FunctionID: "filter"}),
				runnerOperation(3, plan.OperatorSpec{Kind: plan.OpMapToPair, FunctionID: "pair"}),
				runnerOperation(4, plan.OperatorSpec{Kind: plan.OpMapValues, FunctionID: "value"}))
			switch failure {
			case "source":
				source.err = sentinel
			case "pipeline":
				execution.Task.Operations[4].RDD.Operator.FunctionID = "unknown"
			case "record":
				execution.Task.Operations = execution.Task.Operations[:3]
			case "close":
				source.iterator.err = sentinel
			}
			injected := mapFailureStore{Store: store, failure: failure, err: sentinel}
			output, err := NewLocalRunner(registry, source, 1, WithShuffleStore(injected)).RunTask(ctx, execution)
			if err == nil || !reflect.DeepEqual(output, scheduler.TaskOutput{}) {
				t.Fatalf("failed map returned success: %+v, %v", output, err)
			}
			if failure != "pipeline" && failure != "record" && failure != "encoding" && !errors.Is(err, sentinel) {
				t.Fatalf("lost original error: %v", err)
			}
			if failure != "source" && source.iterator.closes != 1 {
				t.Fatalf("source closed %d times, want 1", source.iterator.closes)
			}
			if err := filepath.WalkDir(path, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if !entry.IsDir() {
					t.Errorf("failed task left a file: %s", path)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func mapExecution(combine bool) scheduler.TaskExecution {
	e := runnerExecution(scheduler.Task{ID: 0, StageID: 3, StageKind: scheduler.StageShuffleMap,
		NumPartitions: 3, Operations: []scheduler.StageOperation{runnerOperation(0, plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "unused"})},
		ShuffleWrite: &scheduler.ShuffleWriteSpec{ShuffleID: 4, Partitioner: plan.HashPartitioner(4), AggregatorID: "sum", MapSideCombine: combine}})
	e.Attempt.ID = 9
	e.Attempt.StageAttemptID = 5
	return e
}

func mapTestStore(t *testing.T, path string) *shuffle.Filesystem {
	t.Helper()
	store, err := shuffle.NewFilesystem(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

type mapSource struct {
	iterator *mapIteratorProbe
	err      error
}

func (s *mapSource) Open(context.Context, string, plan.PartitionID, int) (Iterator, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.iterator, nil
}

type mapIteratorProbe struct {
	Iterator
	closes int
	err    error
}

func (i *mapIteratorProbe) Close() error { i.closes++; return i.err }

type mapFailureStore struct {
	shuffle.Store
	failure      string
	err          error
	afterPublish func(shuffle.MapOutput)
}

func (s mapFailureStore) Begin(ctx context.Context, a shuffle.AttemptIdentity, maps, reduces int) (shuffle.AttemptWriter, error) {
	w, err := s.Store.Begin(ctx, a, maps, reduces)
	if err != nil {
		return nil, err
	}
	return mapFailureWriter{AttemptWriter: w, failure: s.failure, err: s.err, afterPublish: s.afterPublish}, nil
}

type mapFailureWriter struct {
	shuffle.AttemptWriter
	failure      string
	err          error
	afterPublish func(shuffle.MapOutput)
}

func (w mapFailureWriter) Write(r shuffle.Record) error {
	if w.failure == "write" {
		return w.err
	}
	return w.AttemptWriter.Write(r)
}
func (w mapFailureWriter) Publish() (shuffle.MapOutput, error) {
	if w.failure == "publish" {
		return shuffle.MapOutput{}, w.err
	}
	output, err := w.AttemptWriter.Publish()
	if err == nil && w.afterPublish != nil {
		w.afterPublish(output)
	}
	return output, err
}
