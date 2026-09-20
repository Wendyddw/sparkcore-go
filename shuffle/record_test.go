package shuffle

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func TestRecordStreamPreservesValues(t *testing.T) {
	var stream bytes.Buffer
	for _, record := range []Record{
		{Key: "café\n", Value: int64(9007199254740993)},
		{Key: "", Value: nil},
		{Key: "nested", Value: map[string]any{"numbers": []any{json.Number("9223372036854775808"), json.Number("1.25")}}},
	} {
		data, err := EncodeRecord(record)
		if err != nil {
			t.Fatal(err)
		}
		stream.Write(data)
	}
	decoder := NewRecordDecoder(&stream)
	first, err := decoder.Next()
	if err != nil || first.Key != "café\n" || first.Value != json.Number("9007199254740993") {
		t.Fatalf("first = %+v, %v", first, err)
	}
	second, err := decoder.Next()
	if err != nil || second.Key != "" || second.Value != nil {
		t.Fatalf("null = %+v, %v", second, err)
	}
	third, err := decoder.Next()
	if err != nil {
		t.Fatal(err)
	}
	numbers := third.Value.(map[string]any)["numbers"].([]any)
	if numbers[0] != json.Number("9223372036854775808") || numbers[1] != json.Number("1.25") {
		t.Fatalf("numbers changed: %#v", numbers)
	}
	if _, err := decoder.Next(); err != io.EOF {
		t.Fatalf("end = %v, want EOF", err)
	}
}

func TestRecordDecoderRejectsMalformedLines(t *testing.T) {
	for _, line := range []string{
		"\n", "null\n", "[]\n", "{}\n",
		"{\"key\":\"a\"}\n", "{\"value\":1}\n",
		"{\"key\":null,\"value\":1}\n", "{\"key\":3,\"value\":1}\n",
		"{\"Key\":\"a\",\"value\":1}\n", "{\"key\":\"a\",\"value\":1,\"extra\":0}\n",
		"{\"key\":\"a\",\"key\":\"b\",\"value\":1}\n",
		"{\"key\":\"a\",\"k\\u0065y\":\"b\",\"value\":1}\n",
		"{\"key\":\"a\",\"value\":1,\"value\":2}\n",
		"{\"key\":\"a\",\"value\":1}{}\n",
		"{\"key\":\"a\",\"value\":1}", // Missing final newline is a truncated record.
		"{\"key\":\"a\",\"value\":\"\xff\"}\n",
		"{\"key\":\"a\",\"value\":NaN}\n",
	} {
		t.Run(line, func(t *testing.T) {
			decoder := NewRecordDecoder(strings.NewReader(line))
			if _, err := decoder.Next(); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("error = %v, want invalid record", err)
			}
			if _, err := decoder.Next(); !errors.Is(err, ErrInvalidRecord) {
				t.Fatalf("decoder continued after malformed record: %v", err)
			}
		})
	}
	if _, err := NewRecordDecoder(strings.NewReader("")).Next(); err != io.EOF {
		t.Fatalf("empty bucket = %v, want EOF", err)
	}
}

func TestRecordSizeBoundary(t *testing.T) {
	// The empty value envelope is 21 bytes, excluding its newline.
	prefix, suffix := `{"key":"a","value":"`, `"}`
	value := strings.Repeat("x", MaxRecordBytes-len(prefix)-len(suffix))
	data, err := EncodeRecord(Record{Key: "a", Value: value})
	if err != nil || len(data) != MaxRecordBytes+1 {
		t.Fatalf("boundary encoding: bytes=%d error=%v", len(data), err)
	}
	if _, err := NewRecordDecoder(bytes.NewReader(data)).Next(); err != nil {
		t.Fatalf("boundary decoding: %v", err)
	}
	if _, err := EncodeRecord(Record{Key: "a", Value: value + "x"}); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("oversize encode = %v", err)
	}
	for _, end := range []string{suffix + "\n", ""} {
		reader := &countedReader{Reader: strings.NewReader(prefix + value + strings.Repeat("x", MaxRecordBytes) + end)}
		if _, err := NewRecordDecoder(reader).Next(); !errors.Is(err, ErrRecordTooLarge) {
			t.Fatalf("oversize decode = %v", err)
		}
		if reader.bytes > MaxRecordBytes+8192 {
			t.Fatalf("decoder read oversized line in full: %d bytes", reader.bytes)
		}
	}
}

type countedReader struct {
	io.Reader
	bytes int
}

func (r *countedReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += n
	return n, err
}

func TestRecordEncodingErrors(t *testing.T) {
	for _, record := range []Record{
		{Key: "\xff", Value: 1},
		{Key: "a", Value: math.NaN()},
		{Key: "a", Value: make(chan int)},
	} {
		if _, err := EncodeRecord(record); !errors.Is(err, ErrInvalidRecord) {
			t.Fatalf("EncodeRecord(%+v) = %v", record, err)
		}
	}
}

type failedReader struct{ err error }

func (r failedReader) Read([]byte) (int, error) { return 0, r.err }

func TestRecordDecoderPreservesIOError(t *testing.T) {
	want := errors.New("disk read failed")
	if _, err := NewRecordDecoder(failedReader{want}).Next(); !errors.Is(err, want) {
		t.Fatalf("read error = %v", err)
	}
}

func TestRecordDecoderAcceptsReorderedFieldsAndWhitespace(t *testing.T) {
	input := " {\"value\": [true, null, \"hello\"], \"key\": \"世界\"} \r\n"
	record, err := NewRecordDecoder(strings.NewReader(input)).Next()
	if err != nil || record.Key != "世界" {
		t.Fatalf("record = %+v, %v", record, err)
	}
	values := record.Value.([]any)
	if len(values) != 3 || values[0] != true || values[1] != nil || values[2] != "hello" {
		t.Fatalf("values = %#v", values)
	}
}
