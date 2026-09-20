package shuffle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func emptyMapOutput() MapOutput {
	empty := sha256.Sum256(nil)
	return MapOutput{
		Version:          FormatVersion,
		Attempt:          AttemptIdentity{RunID: strings.Repeat("0", 32)},
		NumMapPartitions: 1, NumReducePartitions: 2,
		Buckets: []BucketMetadata{
			{PartitionID: 0, SHA256: hex.EncodeToString(empty[:])},
			{PartitionID: 1, SHA256: hex.EncodeToString(empty[:])},
		},
	}
}

func TestMapOutputAllowsZeroIDsAndEmptyBuckets(t *testing.T) {
	output := emptyMapOutput()
	if err := output.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	var decoded MapOutput
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if decoded.Attempt != output.Attempt || len(decoded.Buckets) != 2 {
		t.Fatalf("round trip lost identity or buckets: %+v", decoded)
	}
}

func TestMapOutputWithNonemptyBucket(t *testing.T) {
	output := emptyMapOutput()
	data := []byte("{\"key\":\"a\",\"value\":1}\n")
	digest := sha256.Sum256(data)
	output.Buckets[0] = BucketMetadata{
		PartitionID: 0, RecordCount: 1, ByteCount: int64(len(data)), SHA256: hex.EncodeToString(digest[:]),
	}
	if err := output.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMapOutputRejectsInvalidMetadata(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*MapOutput)
	}{
		{"version", func(o *MapOutput) { o.Version++ }},
		{"missing namespace", func(o *MapOutput) { o.Attempt.RunID = "" }},
		{"path namespace", func(o *MapOutput) { o.Attempt.RunID = "../" + strings.Repeat("0", 29) }},
		{"uppercase namespace", func(o *MapOutput) { o.Attempt.RunID = strings.Repeat("A", 32) }},
		{"negative map partition", func(o *MapOutput) { o.Attempt.MapPartitionID = -1 }},
		{"map partition out of range", func(o *MapOutput) { o.Attempt.MapPartitionID = 1 }},
		{"no map partitions", func(o *MapOutput) { o.NumMapPartitions = 0 }},
		{"no reduce partitions", func(o *MapOutput) { o.NumReducePartitions = 0 }},
		{"missing bucket", func(o *MapOutput) { o.Buckets = o.Buckets[:1] }},
		{"duplicate bucket", func(o *MapOutput) { o.Buckets[1].PartitionID = 0 }},
		{"unordered buckets", func(o *MapOutput) { o.Buckets[0], o.Buckets[1] = o.Buckets[1], o.Buckets[0] }},
		{"negative count", func(o *MapOutput) { o.Buckets[0].RecordCount = -1 }},
		{"negative size", func(o *MapOutput) { o.Buckets[0].ByteCount = -1 }},
		{"missing digest", func(o *MapOutput) { o.Buckets[0].SHA256 = "" }},
		{"invalid digest", func(o *MapOutput) { o.Buckets[0].SHA256 = strings.Repeat("g", 64) }},
		{"wrong empty digest", func(o *MapOutput) { o.Buckets[0].SHA256 = strings.Repeat("0", 64) }},
		{"empty with bytes", func(o *MapOutput) { o.Buckets[0].ByteCount = 20 }},
		{"records without bytes", func(o *MapOutput) { o.Buckets[0].RecordCount = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := emptyMapOutput()
			tc.edit(&output)
			if err := output.Validate(); err == nil {
				t.Fatal("accepted invalid output")
			}
		})
	}
}
