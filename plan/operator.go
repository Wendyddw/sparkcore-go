package plan

// OperatorKind identifies an RDD operation.
type OperatorKind string

const (
	// OpSource reads input records.
	OpSource OperatorKind = "source"
	// OpMap transforms records.
	OpMap OperatorKind = "map"
	// OpFilter selects records.
	OpFilter OperatorKind = "filter"
	// OpMapToPair creates key-value records.
	OpMapToPair OperatorKind = "map_to_pair"
	// OpMapValues transforms values while preserving keys.
	OpMapValues OperatorKind = "map_values"
	// OpReduceByKey combines values by key.
	OpReduceByKey OperatorKind = "reduce_by_key"
)

// OperatorSpec describes a serializable RDD operation.
// FunctionID references registered code; SourcePath applies to sources.
type OperatorSpec struct {
	Kind       OperatorKind `json:"kind"`
	FunctionID string       `json:"function_id,omitempty"`
	SourcePath string       `json:"source_path,omitempty"`
}
