package plan

// RDDID identifies one immutable RDD node within a driver process.
type RDDID uint64

// JobID identifies one action-triggered job.
type JobID uint64

// StageID identifies one logical stage in a job's stage DAG.
type StageID uint64

// ShuffleID identifies one logical shuffle dependency.
type ShuffleID uint64

// TaskID identifies one logical stage-partition task.
type TaskID uint64

// PartitionID identifies a zero-based logical partition.
// It uses int because partition IDs index partition-sized slices throughout
// the planner and executor.
type PartitionID int

// StageAttemptID identifies one physical attempt to run a stage.
type StageAttemptID uint64

// TaskAttemptID identifies one physical attempt to run a logical task.
type TaskAttemptID uint64
