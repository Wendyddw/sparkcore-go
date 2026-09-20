package shuffle

import (
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestPartitionForStableHash(t *testing.T) {
	// Published FNV-1a 64-bit vectors, independent of the implementation.
	for _, tc := range []struct {
		key  string
		hash uint64
	}{
		{"", 0xcbf29ce484222325},
		{"a", 0xaf63dc4c8601ec8c},
		{"foobar", 0x85944171f73967e8},
	} {
		for _, partitions := range []int{1, 2, 7, 31} {
			got, err := PartitionFor(tc.key, partitions)
			want := plan.PartitionID(tc.hash % uint64(partitions))
			if err != nil || got != want {
				t.Fatalf("PartitionFor(%q, %d) = %d, %v; want %d", tc.key, partitions, got, err, want)
			}
		}
	}
	for _, partitions := range []int{0, -1} {
		if _, err := PartitionFor("a", partitions); err == nil {
			t.Fatalf("accepted partition count %d", partitions)
		}
	}
	if _, err := PartitionFor(string([]byte{0xff}), 2); err == nil {
		t.Fatal("accepted invalid UTF-8 key")
	}
}
