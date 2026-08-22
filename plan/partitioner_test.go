package plan

import "testing"

func TestHashPartitioner(t *testing.T) {
	t.Parallel()

	got := HashPartitioner(3)
	want := PartitionerSpec{
		Kind:          PartitionerHash,
		NumPartitions: 3,
	}
	if got != want {
		t.Fatalf("HashPartitioner(3) = %#v, want %#v", got, want)
	}
}
