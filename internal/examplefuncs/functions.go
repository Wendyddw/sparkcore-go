// Package examplefuncs registers the named functions used by example jobs.
package examplefuncs

import (
	"fmt"
	"strings"

	"github.com/Wendyddw/sparkcore-go/executor"
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
					return nil, fmt.Errorf("normalize expected string, got %T", record)
				}
				return strings.TrimSpace(value), nil
			})
		}},
		{name: "non_empty", fn: func() error {
			return registry.RegisterFilter("non_empty", func(record executor.Record) (bool, error) {
				value, ok := record.(string)
				if !ok {
					return false, fmt.Errorf("non_empty expected string, got %T", record)
				}
				return value != "", nil
			})
		}},
		{name: "word_pair", fn: func() error {
			return registry.RegisterPairMap("word_pair", func(record executor.Record) (executor.KeyValue, error) {
				word, ok := record.(string)
				if !ok {
					return executor.KeyValue{}, fmt.Errorf("word_pair expected string, got %T", record)
				}
				return executor.KeyValue{Key: word, Value: 1}, nil
			})
		}},
		{name: "sum_int", fn: func() error {
			return registry.RegisterReduce("sum_int", func(left, right executor.Record) (executor.Record, error) {
				leftValue, leftOK := left.(int)
				rightValue, rightOK := right.(int)
				if !leftOK || !rightOK {
					return nil, fmt.Errorf("sum_int expected ints, got %T and %T", left, right)
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
