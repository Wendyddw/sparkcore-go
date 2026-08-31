package executor

import (
	"fmt"
	"sync"
)

// MapFunc transforms one record.
type MapFunc func(Record) (Record, error)

// FilterFunc decides whether to retain a record.
type FilterFunc func(Record) (bool, error)

// PairMapFunc transforms one record into a key-value record.
type PairMapFunc func(Record) (KeyValue, error)

// ValueMapFunc transforms a value without changing its key.
type ValueMapFunc func(Record) (Record, error)

// ReduceFunc combines two values for the same key.
type ReduceFunc func(Record, Record) (Record, error)

// FunctionRegistry resolves function IDs to executable Go functions.
// Register functions during startup; lookups may run concurrently.
type FunctionRegistry struct {
	mu        sync.RWMutex
	maps      map[string]MapFunc
	filters   map[string]FilterFunc
	pairMaps  map[string]PairMapFunc
	valueMaps map[string]ValueMapFunc
	reducers  map[string]ReduceFunc
}

// NewFunctionRegistry creates an empty registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		maps:      make(map[string]MapFunc),
		filters:   make(map[string]FilterFunc),
		pairMaps:  make(map[string]PairMapFunc),
		valueMaps: make(map[string]ValueMapFunc),
		reducers:  make(map[string]ReduceFunc),
	}
}

// RegisterMap registers a map function under id.
func (r *FunctionRegistry) RegisterMap(id string, fn MapFunc) error {
	if err := validateFunctionRegistration("map", id, fn == nil); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maps == nil {
		r.maps = make(map[string]MapFunc)
	}
	if _, exists := r.maps[id]; exists {
		return duplicateFunctionError("map", id)
	}
	r.maps[id] = fn
	return nil
}

// Map returns the map function registered under id.
func (r *FunctionRegistry) Map(id string) (MapFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.maps[id]
	if !ok {
		return nil, unknownFunctionError("map", id)
	}
	return fn, nil
}

// HasMap reports whether id names a registered map function.
func (r *FunctionRegistry) HasMap(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.maps[id]
	return ok
}

// RegisterFilter registers a filter function under id.
func (r *FunctionRegistry) RegisterFilter(id string, fn FilterFunc) error {
	if err := validateFunctionRegistration("filter", id, fn == nil); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.filters == nil {
		r.filters = make(map[string]FilterFunc)
	}
	if _, exists := r.filters[id]; exists {
		return duplicateFunctionError("filter", id)
	}
	r.filters[id] = fn
	return nil
}

// Filter returns the filter function registered under id.
func (r *FunctionRegistry) Filter(id string) (FilterFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.filters[id]
	if !ok {
		return nil, unknownFunctionError("filter", id)
	}
	return fn, nil
}

// HasFilter reports whether id names a registered filter function.
func (r *FunctionRegistry) HasFilter(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.filters[id]
	return ok
}

// RegisterPairMap registers a pair-map function under id.
func (r *FunctionRegistry) RegisterPairMap(id string, fn PairMapFunc) error {
	if err := validateFunctionRegistration("pair-map", id, fn == nil); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pairMaps == nil {
		r.pairMaps = make(map[string]PairMapFunc)
	}
	if _, exists := r.pairMaps[id]; exists {
		return duplicateFunctionError("pair-map", id)
	}
	r.pairMaps[id] = fn
	return nil
}

// PairMap returns the pair-map function registered under id.
func (r *FunctionRegistry) PairMap(id string) (PairMapFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.pairMaps[id]
	if !ok {
		return nil, unknownFunctionError("pair-map", id)
	}
	return fn, nil
}

// HasPairMap reports whether id names a registered pair-map function.
func (r *FunctionRegistry) HasPairMap(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.pairMaps[id]
	return ok
}

// RegisterValueMap registers a value-map function under id.
func (r *FunctionRegistry) RegisterValueMap(id string, fn ValueMapFunc) error {
	if err := validateFunctionRegistration("value-map", id, fn == nil); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.valueMaps == nil {
		r.valueMaps = make(map[string]ValueMapFunc)
	}
	if _, exists := r.valueMaps[id]; exists {
		return duplicateFunctionError("value-map", id)
	}
	r.valueMaps[id] = fn
	return nil
}

// ValueMap returns the value-map function registered under id.
func (r *FunctionRegistry) ValueMap(id string) (ValueMapFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.valueMaps[id]
	if !ok {
		return nil, unknownFunctionError("value-map", id)
	}
	return fn, nil
}

// HasValueMap reports whether id names a registered value-map function.
func (r *FunctionRegistry) HasValueMap(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.valueMaps[id]
	return ok
}

// RegisterReduce registers a reduce function under id.
func (r *FunctionRegistry) RegisterReduce(id string, fn ReduceFunc) error {
	if err := validateFunctionRegistration("reduce", id, fn == nil); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reducers == nil {
		r.reducers = make(map[string]ReduceFunc)
	}
	if _, exists := r.reducers[id]; exists {
		return duplicateFunctionError("reduce", id)
	}
	r.reducers[id] = fn
	return nil
}

// Reduce returns the reduce function registered under id.
func (r *FunctionRegistry) Reduce(id string) (ReduceFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.reducers[id]
	if !ok {
		return nil, unknownFunctionError("reduce", id)
	}
	return fn, nil
}

// HasReduce reports whether id names a registered reduce function.
func (r *FunctionRegistry) HasReduce(id string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.reducers[id]
	return ok
}

func validateFunctionRegistration(kind, id string, nilFunction bool) error {
	if id == "" {
		return fmt.Errorf("%s function ID must not be empty", kind)
	}
	if nilFunction {
		return fmt.Errorf("%s function %q must not be nil", kind, id)
	}
	return nil
}

func duplicateFunctionError(kind, id string) error {
	return fmt.Errorf("%s function %q is already registered", kind, id)
}

func unknownFunctionError(kind, id string) error {
	return fmt.Errorf("%s function %q is not registered", kind, id)
}
