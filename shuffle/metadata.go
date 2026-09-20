// Package shuffle defines partitioned intermediate data independently of scheduling.
package shuffle

import (
	"crypto/sha256"
	"fmt"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// FormatVersion identifies the manifest and record format.
const FormatVersion = 1

// AttemptIdentity scopes a map output to one execution within a coordinator run.
// Numeric IDs may be zero. RunID is 128 random bits encoded as lowercase hex.
type AttemptIdentity struct {
	RunID          string              `json:"run_id"`
	JobID          plan.JobID          `json:"job_id"`
	ShuffleID      plan.ShuffleID      `json:"shuffle_id"`
	StageID        plan.StageID        `json:"stage_id"`
	StageAttemptID plan.StageAttemptID `json:"stage_attempt_id"`
	TaskID         plan.TaskID         `json:"task_id"`
	TaskAttemptID  plan.TaskAttemptID  `json:"task_attempt_id"`
	MapPartitionID plan.PartitionID    `json:"map_partition_id"`
}

// Validate checks identity values, not their presence in serialized input.
func (a AttemptIdentity) Validate() error {
	if !lowerHex(a.RunID, 32) {
		return fmt.Errorf("run_id must contain 32 lowercase hexadecimal characters")
	}
	if a.MapPartitionID < 0 {
		return fmt.Errorf("map_partition_id must be nonnegative")
	}
	return nil
}

// BucketMetadata describes the exact bytes of one published reduce bucket.
type BucketMetadata struct {
	PartitionID plan.PartitionID `json:"partition_id"`
	RecordCount int64            `json:"record_count"`
	ByteCount   int64            `json:"byte_count"`
	SHA256      string           `json:"sha256"`
}

// MapOutput describes every bucket produced by one map attempt. Treat published
// values as immutable; clone the Buckets slice when crossing ownership boundaries.
type MapOutput struct {
	Version             int              `json:"version"`
	Attempt             AttemptIdentity  `json:"attempt"`
	NumMapPartitions    int              `json:"num_map_partitions"`
	NumReducePartitions int              `json:"num_reduce_partitions"`
	Buckets             []BucketMetadata `json:"buckets"`
}

// Validate checks complete, ordered bucket metadata without consulting files or
// scheduler state. Wire decoding must separately enforce required-field presence.
func (o MapOutput) Validate() error {
	if o.Version != FormatVersion {
		return fmt.Errorf("unsupported shuffle format version %d", o.Version)
	}
	if err := o.Attempt.Validate(); err != nil {
		return err
	}
	if o.NumMapPartitions <= 0 || int(o.Attempt.MapPartitionID) >= o.NumMapPartitions {
		return fmt.Errorf("map partition must be within positive num_map_partitions")
	}
	if o.NumReducePartitions <= 0 || len(o.Buckets) != o.NumReducePartitions {
		return fmt.Errorf("buckets must contain every reduce partition")
	}
	emptyDigest := fmt.Sprintf("%x", sha256.Sum256(nil))
	for i, bucket := range o.Buckets {
		if bucket.PartitionID != plan.PartitionID(i) {
			return fmt.Errorf("bucket %d must have partition_id %d", i, i)
		}
		if bucket.RecordCount < 0 || bucket.ByteCount < 0 || !lowerHex(bucket.SHA256, 64) {
			return fmt.Errorf("bucket %d requires nonnegative counts and a SHA-256 digest", i)
		}
		if bucket.RecordCount == 0 {
			if bucket.ByteCount != 0 || bucket.SHA256 != emptyDigest {
				return fmt.Errorf("bucket %d has inconsistent empty metadata", i)
			}
		} else if bucket.ByteCount < bucket.RecordCount {
			return fmt.Errorf("bucket %d has too few bytes for its records", i)
		}
	}
	return nil
}

func lowerHex(s string, length int) bool {
	if len(s) != length {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
