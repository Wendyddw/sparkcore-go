package plan

// RDDNode describes one immutable step in an RDD lineage graph.
type RDDNode struct {
	ID            RDDID            `json:"id"`
	Name          string           `json:"name"`
	Operator      OperatorSpec     `json:"operator"`
	Dependencies  []Dependency     `json:"dependencies,omitempty"`
	NumPartitions int              `json:"num_partitions"`
	Partitioner   *PartitionerSpec `json:"partitioner,omitempty"`
}
