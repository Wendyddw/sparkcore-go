package executor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestTextSourcePartitionsLoseOrDuplicateNoLines(t *testing.T) {
	path := writeTextFixture(t, "a\nbbbbb\ncc\ndddddddd\ne\n")
	var got []Record
	for partition := range 4 {
		iterator, err := (TextSourceReader{}).Open(context.Background(), path, plan.PartitionID(partition), 4)
		if err != nil {
			t.Fatalf("Open(partition %d) error = %v", partition, err)
		}
		got = append(got, drainIterator(t, iterator)...)
	}
	want := []Record{"a", "bbbbb", "cc", "dddddddd", "e"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partitioned lines = %#v, want %#v", got, want)
	}
}

func TestTextSourceHandlesEmptyFileAndPartitions(t *testing.T) {
	path := writeTextFixture(t, "")
	for partition := range 4 {
		iterator, err := (TextSourceReader{}).Open(context.Background(), path, plan.PartitionID(partition), 4)
		if err != nil {
			t.Fatalf("Open(partition %d) error = %v", partition, err)
		}
		if got := drainIterator(t, iterator); len(got) != 0 {
			t.Fatalf("partition %d records = %#v, want empty", partition, got)
		}
	}
}

func writeTextFixture(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "input.txt")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	return path
}
