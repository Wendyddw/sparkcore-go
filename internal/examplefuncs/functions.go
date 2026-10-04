// Package examplefuncs registers the named functions used by example jobs.
package examplefuncs

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

// Register adds every named function referenced by the example job files.
func Register(registry *executor.FunctionRegistry) error {
	if registry == nil {
		return fmt.Errorf("register example functions: registry is nil")
	}
	registrations := []struct {
		name string
		fn   func() error
	}{
		{name: "normalize", fn: func() error {
			return registry.RegisterMap("normalize", func(record executor.Record) (executor.Record, error) {
				value, ok := record.(string)
				if !ok {
					return nil, scheduler.PermanentFailure(fmt.Errorf("normalize expected string, got %T", record))
				}
				return strings.TrimSpace(value), nil
			})
		}},
		{name: "non_empty", fn: func() error {
			return registry.RegisterFilter("non_empty", func(record executor.Record) (bool, error) {
				value, ok := record.(string)
				if !ok {
					return false, scheduler.PermanentFailure(fmt.Errorf("non_empty expected string, got %T", record))
				}
				return value != "", nil
			})
		}},
		{name: "word_pair", fn: func() error {
			return registry.RegisterPairMap("word_pair", func(record executor.Record) (executor.KeyValue, error) {
				word, ok := record.(string)
				if !ok {
					return executor.KeyValue{}, scheduler.PermanentFailure(fmt.Errorf("word_pair expected string, got %T", record))
				}
				return executor.KeyValue{Key: word, Value: 1}, nil
			})
		}},
		{name: "sum_int", fn: func() error {
			return registry.RegisterReduce("sum_int", func(left, right executor.Record) (executor.Record, error) {
				leftValue, err := integerValue(left)
				if err != nil {
					return nil, err
				}
				rightValue, err := integerValue(right)
				if err != nil {
					return nil, err
				}
				if (rightValue > 0 && leftValue > math.MaxInt64-rightValue) || (rightValue < 0 && leftValue < math.MinInt64-rightValue) {
					return nil, scheduler.PermanentFailure(fmt.Errorf("sum_int overflow"))
				}
				return leftValue + rightValue, nil
			})
		}},
	}
	for _, registration := range registrations {
		if err := registration.fn(); err != nil {
			return fmt.Errorf("register example function %q: %w", registration.name, err)
		}
	}
	return nil
}

func integerValue(value executor.Record) (int64, error) {
	switch number := value.(type) {
	case int:
		return int64(number), nil
	case int64:
		return number, nil
	case json.Number:
		if !json.Valid([]byte(number)) {
			return 0, scheduler.PermanentFailure(fmt.Errorf("sum_int invalid JSON number %q", number))
		}
		integer, err := number.Int64()
		if err != nil {
			return 0, scheduler.PermanentFailure(fmt.Errorf("sum_int expected signed int64: %w", err))
		}
		return integer, nil
	default:
		return 0, scheduler.PermanentFailure(fmt.Errorf("sum_int expected int, int64 or integer json.Number, got %T", value))
	}
}
