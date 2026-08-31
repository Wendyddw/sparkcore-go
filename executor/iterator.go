package executor

import (
	"context"
	"fmt"
)

// Iterator produces records incrementally until ok is false.
type Iterator interface {
	Next(context.Context) (record Record, ok bool, err error)
}

type sliceIterator struct {
	records []Record
	next    int
}

func (i *sliceIterator) Next(ctx context.Context) (Record, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if i.next >= len(i.records) {
		return nil, false, nil
	}
	record := i.records[i.next]
	i.next++
	return record, true, nil
}

type mapIterator struct {
	input Iterator
	fn    MapFunc
}

func (i *mapIterator) Next(ctx context.Context) (Record, bool, error) {
	record, ok, err := i.input.Next(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	mapped, err := i.fn(record)
	return mapped, err == nil, err
}

type filterIterator struct {
	input Iterator
	fn    FilterFunc
}

func (i *filterIterator) Next(ctx context.Context) (Record, bool, error) {
	for {
		record, ok, err := i.input.Next(ctx)
		if err != nil || !ok {
			return nil, ok, err
		}
		keep, err := i.fn(record)
		if err != nil {
			return nil, false, err
		}
		if keep {
			return record, true, nil
		}
	}
}

type pairMapIterator struct {
	input Iterator
	fn    PairMapFunc
}

func (i *pairMapIterator) Next(ctx context.Context) (Record, bool, error) {
	record, ok, err := i.input.Next(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	pair, err := i.fn(record)
	if err != nil {
		return nil, false, err
	}
	return pair, true, nil
}

type valueMapIterator struct {
	input Iterator
	fn    ValueMapFunc
}

func (i *valueMapIterator) Next(ctx context.Context) (Record, bool, error) {
	record, ok, err := i.input.Next(ctx)
	if err != nil || !ok {
		return nil, ok, err
	}
	pair, ok := record.(KeyValue)
	if !ok {
		return nil, false, fmt.Errorf("map-values expected KeyValue, got %T", record)
	}
	value, err := i.fn(pair.Value)
	if err != nil {
		return nil, false, err
	}
	pair.Value = value
	return pair, true, nil
}
