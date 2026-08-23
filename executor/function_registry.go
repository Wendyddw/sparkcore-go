package executor

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
