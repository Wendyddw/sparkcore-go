package executor

import (
	"strings"
	"testing"
)

func TestFunctionRegistryRegistersAndLooksUpFunctions(t *testing.T) {
	t.Parallel()

	registry := NewFunctionRegistry()
	mapFn := MapFunc(func(record Record) (Record, error) { return record, nil })
	filterFn := FilterFunc(func(Record) (bool, error) { return true, nil })
	pairMapFn := PairMapFunc(func(Record) (KeyValue, error) {
		return KeyValue{Key: "key", Value: 1}, nil
	})
	valueMapFn := ValueMapFunc(func(record Record) (Record, error) { return record, nil })
	reduceFn := ReduceFunc(func(left, _ Record) (Record, error) { return left, nil })

	registrations := []struct {
		name     string
		register func() error
	}{
		{name: "map", register: func() error { return registry.RegisterMap("map", mapFn) }},
		{name: "filter", register: func() error { return registry.RegisterFilter("filter", filterFn) }},
		{name: "pair-map", register: func() error { return registry.RegisterPairMap("pair-map", pairMapFn) }},
		{name: "value-map", register: func() error { return registry.RegisterValueMap("value-map", valueMapFn) }},
		{name: "reduce", register: func() error { return registry.RegisterReduce("reduce", reduceFn) }},
	}
	for _, registration := range registrations {
		if err := registration.register(); err != nil {
			t.Fatalf("register %s function: %v", registration.name, err)
		}
	}

	if got, err := registry.Map("map"); err != nil || got == nil {
		t.Fatalf("lookup map function = (%v, %v), want registered function", got, err)
	}
	if got, err := registry.Filter("filter"); err != nil || got == nil {
		t.Fatalf("lookup filter function = (%v, %v), want registered function", got, err)
	}
	if got, err := registry.PairMap("pair-map"); err != nil || got == nil {
		t.Fatalf("lookup pair-map function = (%v, %v), want registered function", got, err)
	}
	if got, err := registry.ValueMap("value-map"); err != nil || got == nil {
		t.Fatalf("lookup value-map function = (%v, %v), want registered function", got, err)
	}
	if got, err := registry.Reduce("reduce"); err != nil || got == nil {
		t.Fatalf("lookup reduce function = (%v, %v), want registered function", got, err)
	}
}

func TestFunctionRegistryRejectsDuplicateIDs(t *testing.T) {
	t.Parallel()

	tests := registrationCases(NewFunctionRegistry())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.register("duplicate"); err != nil {
				t.Fatalf("first registration: %v", err)
			}
			if err := tt.register("duplicate"); err == nil {
				t.Fatal("second registration succeeded")
			} else if !strings.Contains(err.Error(), "duplicate") {
				t.Fatalf("duplicate error %q does not contain function ID", err)
			}
		})
	}
}

func TestFunctionRegistryReturnsDescriptiveUnknownErrors(t *testing.T) {
	t.Parallel()

	registry := NewFunctionRegistry()
	tests := []struct {
		name   string
		lookup func() error
	}{
		{name: "map", lookup: func() error { _, err := registry.Map("missing"); return err }},
		{name: "filter", lookup: func() error { _, err := registry.Filter("missing"); return err }},
		{name: "pair-map", lookup: func() error { _, err := registry.PairMap("missing"); return err }},
		{name: "value-map", lookup: func() error { _, err := registry.ValueMap("missing"); return err }},
		{name: "reduce", lookup: func() error { _, err := registry.Reduce("missing"); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.lookup()
			if err == nil {
				t.Fatal("lookup succeeded")
			}
			if !strings.Contains(err.Error(), tt.name) || !strings.Contains(err.Error(), "missing") {
				t.Fatalf("lookup error %q does not identify kind and function ID", err)
			}
		})
	}
}

func TestFunctionRegistryRejectsEmptyIDsAndNilFunctions(t *testing.T) {
	t.Parallel()

	registry := NewFunctionRegistry()
	if err := registry.RegisterMap("", func(record Record) (Record, error) { return record, nil }); err == nil {
		t.Fatal("empty function ID was accepted")
	}
	if err := registry.RegisterMap("nil", nil); err == nil {
		t.Fatal("nil function was accepted")
	}
}

type registrationCase struct {
	name     string
	register func(string) error
}

func registrationCases(registry *FunctionRegistry) []registrationCase {
	return []registrationCase{
		{
			name: "map",
			register: func(id string) error {
				return registry.RegisterMap(id, func(record Record) (Record, error) { return record, nil })
			},
		},
		{
			name: "filter",
			register: func(id string) error {
				return registry.RegisterFilter(id, func(Record) (bool, error) { return true, nil })
			},
		},
		{
			name: "pair-map",
			register: func(id string) error {
				return registry.RegisterPairMap(id, func(Record) (KeyValue, error) { return KeyValue{}, nil })
			},
		},
		{
			name: "value-map",
			register: func(id string) error {
				return registry.RegisterValueMap(id, func(record Record) (Record, error) { return record, nil })
			},
		},
		{
			name: "reduce",
			register: func(id string) error {
				return registry.RegisterReduce(id, func(left, _ Record) (Record, error) { return left, nil })
			},
		},
	}
}
