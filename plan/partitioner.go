package plan

// PartitionerKind identifies a partitioning strategy.
type PartitionerKind string

const (
	// PartitionerHash assigns keys by hash.
	PartitionerHash PartitionerKind = "hash"
)

// PartitionerSpec describes output partitioning.
type PartitionerSpec struct {
	Kind          PartitionerKind `json:"kind"`
	NumPartitions int             `json:"num_partitions"`
}

// HashPartitioner returns a hash partitioner with the requested partition count.
// Plan validation rejects non-positive partition counts.
func HashPartitioner(numPartitions int) PartitionerSpec {
	return PartitionerSpec{
		Kind:          PartitionerHash,
		NumPartitions: numPartitions,
	}
}
