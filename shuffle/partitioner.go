package shuffle

import (
	"fmt"
	"hash/fnv"
	"unicode/utf8"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// PartitionFor hashes exact UTF-8 key bytes with FNV-1a 64-bit. Equal keys map
// to equal partitions; distinct keys may share a partition and remain distinct.
func PartitionFor(key string, numPartitions int) (plan.PartitionID, error) {
	if numPartitions <= 0 {
		return 0, fmt.Errorf("shuffle partition count must be positive")
	}
	if !utf8.ValidString(key) {
		return 0, fmt.Errorf("shuffle key must be valid UTF-8")
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return plan.PartitionID(hash.Sum64() % uint64(numPartitions)), nil
}
