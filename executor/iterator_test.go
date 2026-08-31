package executor

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestMapAndFilterIteratorsComposeIncrementally(t *testing.T) {
	input := &sliceIterator{records: []Record{1, 2, 3, 4}}
	mapped := &mapIterator{input: input, fn: func(record Record) (Record, error) {
		return record.(int) * 2, nil
	}}
	filtered := &filterIterator{input: mapped, fn: func(record Record) (bool, error) {
		return record.(int) > 4, nil
	}}

	if got := drainIterator(t, filtered); !reflect.DeepEqual(got, []Record{6, 8}) {
		t.Fatalf("iterator output = %#v, want [6 8]", got)
	}
}

func TestPairMapAndValueMapPreserveKeys(t *testing.T) {
	input := &sliceIterator{records: []Record{"a", "bb"}}
	paired := &pairMapIterator{input: input, fn: func(record Record) (KeyValue, error) {
		value := record.(string)
		return KeyValue{Key: value, Value: len(value)}, nil
	}}
	values := &valueMapIterator{input: paired, fn: func(record Record) (Record, error) {
		return fmt.Sprintf("length=%d", record.(int)), nil
	}}

	want := []Record{
		KeyValue{Key: "a", Value: "length=1"},
		KeyValue{Key: "bb", Value: "length=2"},
	}
	if got := drainIterator(t, values); !reflect.DeepEqual(got, want) {
		t.Fatalf("iterator output = %#v, want %#v", got, want)
	}
}

func TestEmptyIterator(t *testing.T) {
	if got := drainIterator(t, &sliceIterator{}); len(got) != 0 {
		t.Fatalf("empty iterator output = %#v", got)
	}
}

func drainIterator(t *testing.T, iterator Iterator) []Record {
	t.Helper()
	var records []Record
	for {
		record, ok, err := iterator.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if !ok {
			return records
		}
		records = append(records, record)
	}
}
