package executor

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

// reduceIterator aggregates a bounded partition in memory before emitting any result.
type reduceIterator struct {
	input  Iterator
	fn     ReduceFunc
	result *sliceIterator
	err    error
	closed bool
}

func (i *reduceIterator) Next(ctx context.Context) (Record, bool, error) {
	if i.err != nil {
		return nil, false, i.err
	}
	if i.closed {
		return nil, false, nil
	}
	if i.result == nil {
		records, err := reduceRecords(ctx, i.input, i.fn)
		i.err = errors.Join(err, closeIterator(i.input))
		i.input = nil
		if i.err != nil {
			return nil, false, i.err
		}
		i.result = &sliceIterator{records: records}
	}
	return i.result.Next(ctx)
}

func (i *reduceIterator) Close() error {
	i.closed = true
	i.result = nil
	err := closeIterator(i.input)
	i.input = nil
	return err
}

func reduceRecords(ctx context.Context, input Iterator, fn ReduceFunc) ([]Record, error) {
	values := make(map[string]Record)
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		record, ok, err := input.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		pair, ok := record.(KeyValue)
		if !ok {
			return nil, permanentErrorf("reduce-by-key expected KeyValue, got %T", record)
		}
		value := pair.Value
		if previous, exists := values[pair.Key]; exists {
			value, err = fn(previous, value)
			if err != nil {
				return nil, fmt.Errorf("reduce key %q: %w", pair.Key, err)
			}
		}
		values[pair.Key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	records := make([]Record, 0, len(keys))
	for _, key := range keys {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		records = append(records, KeyValue{Key: key, Value: values[key]})
	}
	return records, nil
}
