package shuffle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// MaxRecordBytes excludes the required final newline.
const MaxRecordBytes = 1 << 20

var (
	ErrInvalidRecord  = errors.New("invalid shuffle record")
	ErrRecordTooLarge = errors.New("shuffle record exceeds size limit")
)

// Record is the JSON representation of a keyed value, independent of executor
// types. Decoded numbers, including nested numbers, are json.Number values.
type Record struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// EncodeRecord returns one JSON line. Values follow encoding/json semantics;
// the key must be valid UTF-8 so encoding cannot change its partition identity.
func EncodeRecord(record Record) ([]byte, error) {
	if !utf8.ValidString(record.Key) {
		return nil, fmt.Errorf("%w: key must be valid UTF-8", ErrInvalidRecord)
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	if len(data) > MaxRecordBytes {
		return nil, ErrRecordTooLarge
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%w: encoded data must be valid UTF-8", ErrInvalidRecord)
	}
	return append(data, '\n'), nil
}

// RecordDecoder reads bounded JSON lines. It does not own or close its reader;
// the store controls file lifetime and cancellation. After any error it stops.
type RecordDecoder struct {
	reader *bufio.Reader
	err    error
}

func NewRecordDecoder(reader io.Reader) *RecordDecoder {
	if reader == nil {
		return &RecordDecoder{err: fmt.Errorf("%w: reader is nil", ErrInvalidRecord)}
	}
	return &RecordDecoder{reader: bufio.NewReaderSize(reader, 4096)}
}

// Next returns io.EOF only between complete records. A missing final newline
// is invalid; an oversized line is rejected without reading the entire line.
func (d *RecordDecoder) Next() (Record, error) {
	if d.err != nil {
		return Record{}, d.err
	}
	line, err := d.readLine()
	if err == nil {
		var record Record
		record, err = decodeRecord(line)
		if err == nil {
			return record, nil
		}
	}
	d.err = err
	return Record{}, err
}

func (d *RecordDecoder) readLine() ([]byte, error) {
	var line []byte
	for {
		fragment, err := d.reader.ReadSlice('\n')
		length := len(line) + len(fragment)
		if err == nil {
			length-- // Exclude the newline from the size limit.
		}
		if length > MaxRecordBytes {
			return nil, ErrRecordTooLarge
		}
		line = append(line, fragment...)
		switch err {
		case nil:
			return line[:len(line)-1], nil
		case bufio.ErrBufferFull:
			continue
		case io.EOF:
			if len(line) == 0 {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("%w: missing final newline", ErrInvalidRecord)
		default:
			return nil, fmt.Errorf("read shuffle record: %w", err)
		}
	}
}

func decodeRecord(data []byte) (Record, error) {
	var record Record
	if !utf8.Valid(data) {
		return record, fmt.Errorf("%w: data must be valid UTF-8", ErrInvalidRecord)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return record, fmt.Errorf("%w: expected an object", ErrInvalidRecord)
	}
	seen := make(map[string]bool, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return Record{}, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
		}
		name, ok := token.(string)
		if !ok || seen[name] || (name != "key" && name != "value") {
			return Record{}, fmt.Errorf("%w: unknown or duplicate field %q", ErrInvalidRecord, token)
		}
		seen[name] = true
		if name == "key" {
			key, err := decoder.Token()
			var ok bool
			record.Key, ok = key.(string)
			if err != nil || !ok {
				return Record{}, fmt.Errorf("%w: key must be a string", ErrInvalidRecord)
			}
		} else if err := decoder.Decode(&record.Value); err != nil {
			return Record{}, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return Record{}, fmt.Errorf("%w: %w", ErrInvalidRecord, err)
	}
	if !seen["key"] || !seen["value"] {
		return Record{}, fmt.Errorf("%w: key and value are required", ErrInvalidRecord)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return Record{}, fmt.Errorf("%w: trailing data", ErrInvalidRecord)
	}
	return record, nil
}
