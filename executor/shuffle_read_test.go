package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
	"github.com/Wendyddw/sparkcore-go/scheduler"
	"github.com/Wendyddw/sparkcore-go/shuffle"
)

func TestShuffleReadOrderOwnershipAndNoAggregation(t *testing.T) {
	store, inputs := readFixture(t, t.TempDir(), [][]Record{
		{KeyValue{"z", 1}, KeyValue{"a", 2}}, {}, {KeyValue{"z", 3}},
	})
	observed := &observedReadStore{Store: store}
	e := reduceExecution(inputs)
	registry := readRegistry(t, nil)
	iterator, err := buildTaskIterator(context.Background(), e, registry, TextSourceReader{}, observed)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIterator(iterator)
	// Building the pipeline must not open buckets, and the reader owns a deep copy.
	if len(observed.opened) != 0 {
		t.Fatal("pipeline construction read shuffle files")
	}
	e.ShuffleInputs.Outputs[0].Buckets[0].SHA256 = "changed"
	got := drainIterator(t, iterator)
	want := []Record{KeyValue{"a", json.Number("2")}, KeyValue{"z", int64(4)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("reduced output = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(observed.opened, []plan.PartitionID{0, 1, 2}) || observed.maxLive != 1 || observed.live != 0 {
		t.Fatalf("reader lifecycle: %+v", observed)
	}
	// ShuffleRead alone emits every input record, including repeated keys.
	reader := &shuffleIterator{store: store, inputs: inputs, partition: 0}
	defer reader.Close()
	if records := drainIterator(t, reader); !reflect.DeepEqual(records, []Record{
		KeyValue{"z", json.Number("1")}, KeyValue{"a", json.Number("2")}, KeyValue{"z", json.Number("3")},
	}) {
		t.Fatalf("shuffle read aggregated or reordered records: %#v", records)
	}
}

func TestShuffleReadRejectsInvalidBindingsBeforeIO(t *testing.T) {
	store, inputs := readFixture(t, t.TempDir(), [][]Record{{}})
	for name, change := range map[string]func(*scheduler.TaskExecution){
		"missing inputs":    func(e *scheduler.TaskExecution) { e.ShuffleInputs = nil },
		"wrong run":         func(e *scheduler.TaskExecution) { e.RunID = scheduler.NewRunID() },
		"wrong job":         func(e *scheduler.TaskExecution) { e.JobID++ },
		"wrong shuffle":     func(e *scheduler.TaskExecution) { e.Task.Operations[0].ShuffleRead.ShuffleID++ },
		"partition count":   func(e *scheduler.TaskExecution) { e.Task.NumPartitions++ },
		"partition index":   func(e *scheduler.TaskExecution) { e.Task.PartitionID = 1 },
		"partitioner":       func(e *scheduler.TaskExecution) { e.Task.Operations[0].ShuffleRead.Partitioner.Kind = "unknown" },
		"missing reduce":    func(e *scheduler.TaskExecution) { e.Task.Operations = e.Task.Operations[:1] },
		"unknown reducer":   func(e *scheduler.TaskExecution) { e.Task.Operations[1].RDD.Operator.FunctionID = "unknown" },
		"missing read spec": func(e *scheduler.TaskExecution) { e.Task.Operations[0].ShuffleRead = nil },
		"read after source": func(e *scheduler.TaskExecution) {
			e.Task.Operations = append([]scheduler.StageOperation{runnerOperation(9, plan.OperatorSpec{Kind: plan.OpSource})}, e.Task.Operations...)
		},
		"map stage": func(e *scheduler.TaskExecution) { e.Task.StageKind = scheduler.StageShuffleMap },
		"unexpected inputs": func(e *scheduler.TaskExecution) {
			e.Task.Operations = []scheduler.StageOperation{runnerOperation(9, plan.OperatorSpec{Kind: plan.OpSource})}
		},
	} {
		t.Run(name, func(t *testing.T) {
			observed := &observedReadStore{Store: store}
			source := &identityGuardSource{}
			e := reduceExecution(inputs)
			change(&e)
			output, err := NewLocalRunner(readRegistry(t, nil), source, 1, WithShuffleStore(observed)).RunTask(context.Background(), e)
			if err == nil || !reflect.DeepEqual(output, scheduler.TaskOutput{}) || source.reads != 0 || len(observed.opened) != 0 {
				t.Fatalf("invalid task reached IO: output=%+v err=%v source=%d buckets=%v", output, err, source.reads, observed.opened)
			}
		})
	}
}

func TestShuffleReadMissingAndCorruptFilesReturnNoPartialResult(t *testing.T) {
	for _, mode := range []string{"missing-empty", "checksum", "truncated"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			records := []Record{KeyValue{"a", 1}, KeyValue{"a", 1}}
			if mode == "missing-empty" {
				records = nil
			}
			store, inputs := readFixture(t, root, [][]Record{records})
			if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.Name() != "bucket-0.jsonl" {
					return nil
				}
				if mode == "missing-empty" {
					return os.Remove(path)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if mode == "truncated" {
					data = data[:len(data)-1]
				} else {
					for i, b := range data {
						if b == '1' {
							data[i] = '2'
							break
						}
					}
				}
				return os.WriteFile(path, data, 0600)
			}); err != nil {
				t.Fatal(err)
			}
			e := reduceExecution(inputs)
			output, err := NewLocalRunner(readRegistry(t, nil), nil, 1, WithShuffleStore(store)).RunTask(context.Background(), e)
			var inputErr *shuffle.InputError
			if !errors.As(err, &inputErr) || !errors.Is(err, shuffle.ErrShuffleInput) || !reflect.DeepEqual(output, scheduler.TaskOutput{}) {
				t.Fatalf("expected typed input failure and no result: %+v, %v", output, err)
			}
			if inputErr.Attempt != inputs.Outputs[0].Attempt || inputErr.PartitionID != 0 {
				t.Fatalf("wrong input identity: %+v", inputErr)
			}
		})
	}
}

func TestShuffleReadersCloseOnReducerFailureAndCancellation(t *testing.T) {
	for _, cancelRead := range []bool{false, true} {
		store, inputs := readFixture(t, t.TempDir(), [][]Record{{KeyValue{"a", 1}, KeyValue{"a", 1}}, {KeyValue{"a", 1}}})
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("stop execution")
		observed := &observedReadStore{Store: store}
		if cancelRead {
			observed.onOpen = func() { cancel(cause) }
		}
		registry := readRegistry(t, func(Record, Record) (Record, error) { return nil, cause })
		output, err := NewLocalRunner(registry, nil, 1, WithShuffleStore(observed)).RunTask(ctx, reduceExecution(inputs))
		cancel(nil)
		if !errors.Is(err, cause) || errors.Is(err, shuffle.ErrShuffleInput) || !reflect.DeepEqual(output, scheduler.TaskOutput{}) {
			t.Fatalf("unexpected error/output: %+v, %v", output, err)
		}
		if observed.live != 0 || len(observed.opened) != 1 {
			t.Fatalf("readers leaked or execution continued: %+v", observed)
		}
	}
}

func TestReduceIteratorNullsAndFailureAreSticky(t *testing.T) {
	var calls int
	iterator := &reduceIterator{input: &sliceIterator{records: []Record{KeyValue{"", nil}, KeyValue{"", nil}}}, fn: func(a, b Record) (Record, error) {
		calls++
		if a != nil || b != nil {
			t.Fatal("null input changed")
		}
		return nil, nil
	}}
	if got := drainIterator(t, iterator); !reflect.DeepEqual(got, []Record{KeyValue{"", nil}}) || calls != 1 {
		t.Fatalf("null aggregation: %v, calls %d", got, calls)
	}
	bad := &reduceIterator{input: &sliceIterator{records: []Record{"not a pair"}}}
	_, _, first := bad.Next(context.Background())
	_, _, second := bad.Next(context.Background())
	if first == nil || first != second {
		t.Fatalf("failure not preserved: %v / %v", first, second)
	}
}

func TestShuffleReducePreservesLargeIntegers(t *testing.T) {
	store, inputs := readFixture(t, t.TempDir(), [][]Record{
		{KeyValue{"a", int64(9007199254740993)}, KeyValue{"a", 1}}, {KeyValue{"a", 2}},
	})
	output, err := NewLocalRunner(readRegistry(t, nil), nil, 1, WithShuffleStore(store)).RunTask(context.Background(), reduceExecution(inputs))
	if err != nil {
		t.Fatal(err)
	}
	if want := []any{KeyValue{"a", int64(9007199254740996)}}; !reflect.DeepEqual(output.Records, want) {
		t.Fatalf("lost integer precision: %#v", output.Records)
	}
}

func TestShuffleReaderEarlyCloseAndCloseFailure(t *testing.T) {
	store, inputs := readFixture(t, t.TempDir(), [][]Record{{KeyValue{"a", 1}}, {}})
	observed := &observedReadStore{Store: store}
	reader := &shuffleIterator{store: observed, inputs: inputs, partition: 0}
	if _, ok, err := reader.Next(context.Background()); err != nil || !ok {
		t.Fatalf("first read: %v, %v", ok, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := reader.Next(context.Background()); err != nil || ok {
		t.Fatalf("read after close: %v, %v", ok, err)
	}
	if observed.live != 0 || len(observed.opened) != 1 {
		t.Fatalf("early close leaked or opened next bucket: %+v", observed)
	}

	cause := errors.New("close failed")
	observed.closeErr = cause
	output, err := NewLocalRunner(readRegistry(t, nil), nil, 1, WithShuffleStore(observed)).RunTask(context.Background(), reduceExecution(inputs))
	var inputErr *shuffle.InputError
	if !errors.Is(err, cause) || !errors.As(err, &inputErr) || inputErr.Attempt != inputs.Outputs[0].Attempt || !reflect.DeepEqual(output, scheduler.TaskOutput{}) {
		t.Fatalf("close error lost identity or produced output: %+v, %v", output, err)
	}
	if observed.live != 0 {
		t.Fatal("close failure leaked reader")
	}
}

func readFixture(t *testing.T, path string, partitions [][]Record) (*shuffle.Filesystem, shuffle.InputSnapshot) {
	t.Helper()
	store := mapTestStore(t, path)
	sources := memorySourceReader{partitions: make(map[plan.PartitionID][]Record)}
	for p, records := range partitions {
		sources.partitions[plan.PartitionID(p)] = records
	}
	runner := NewLocalRunner(NewFunctionRegistry(), sources, 1, WithShuffleStore(store))
	inputs := shuffle.InputSnapshot{RunID: mapExecution(false).RunID, JobID: 7, ShuffleID: 4, StageID: 3, StageAttemptID: 5, NumMapPartitions: len(partitions), NumReducePartitions: 1}
	for p := range partitions {
		e := mapExecution(false)
		e.Task.NumPartitions = len(partitions)
		e.Task.PartitionID = plan.PartitionID(p)
		e.Task.ID = plan.TaskID(p)
		e.Attempt.TaskID = e.Task.ID
		e.Attempt.ID += plan.TaskAttemptID(p)
		e.Task.ShuffleWrite.Partitioner.NumPartitions = 1
		output, err := runner.RunTask(context.Background(), e)
		if err != nil {
			t.Fatal(err)
		}
		inputs.Outputs = append(inputs.Outputs, *output.ShuffleOutput)
	}
	return store, inputs
}

func reduceExecution(inputs shuffle.InputSnapshot) scheduler.TaskExecution {
	inputs = inputs.Clone()
	e := runnerExecution(scheduler.Task{ID: 100, StageID: 4, StageKind: scheduler.StageResult, NumPartitions: inputs.NumReducePartitions,
		FinalAction: &scheduler.ActionSpec{Kind: scheduler.ActionCollect}, Operations: []scheduler.StageOperation{
			{Kind: scheduler.StageOperationShuffleRead, ShuffleRead: &scheduler.ShuffleReadSpec{ShuffleID: inputs.ShuffleID, Partitioner: plan.HashPartitioner(inputs.NumReducePartitions)}},
			runnerOperation(5, plan.OperatorSpec{Kind: plan.OpReduceByKey, FunctionID: "sum"}),
		}})
	e.ShuffleInputs = &inputs
	return e
}

func readRegistry(t *testing.T, reduce ReduceFunc) *FunctionRegistry {
	t.Helper()
	registry := NewFunctionRegistry()
	if reduce == nil {
		reduce = func(a, b Record) (Record, error) {
			toInt := func(v Record) int64 {
				if n, ok := v.(int64); ok {
					return n
				}
				n, err := v.(json.Number).Int64()
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			return toInt(a) + toInt(b), nil
		}
	}
	if err := registry.RegisterReduce("sum", reduce); err != nil {
		t.Fatal(err)
	}
	return registry
}

type observedReadStore struct {
	shuffle.Store
	opened        []plan.PartitionID
	live, maxLive int
	onOpen        func()
	closeErr      error
}

func (s *observedReadStore) OpenBucket(ctx context.Context, output shuffle.MapOutput, partition plan.PartitionID) (shuffle.BucketReader, error) {
	r, err := s.Store.OpenBucket(ctx, output, partition)
	if err != nil {
		return nil, err
	}
	s.opened = append(s.opened, output.Attempt.MapPartitionID)
	s.live++
	if s.live > s.maxLive {
		s.maxLive = s.live
	}
	if s.onOpen != nil {
		s.onOpen()
	}
	return &observedBucket{BucketReader: r, owner: s}, nil
}

type observedBucket struct {
	shuffle.BucketReader
	owner  *observedReadStore
	closed bool
}

func (r *observedBucket) Close() error {
	if !r.closed {
		r.closed = true
		r.owner.live--
	}
	return errors.Join(r.BucketReader.Close(), r.owner.closeErr)
}
