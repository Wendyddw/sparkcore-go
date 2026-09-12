package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var (
	ErrInvalidMessage  = errors.New("invalid protocol message")
	ErrMessageTooLarge = errors.New("protocol message exceeds size limit")
)

// Message validates wire values without consulting scheduler state.
type Message interface{ Validate() error }

// DecodeAndValidate decodes exactly one JSON object into T and validates it.
// maxBytes limits the input, including whitespace. Errors return a zero T.
// Callers manage reader deadlines and closure.
//
// Fields without omitempty are required; names must be exact and unique.
// Missing required fields and null scalars/structs are rejected, preserving zero IDs.
// json.RawMessage records remain opaque, retaining keys and numeric precision.
func DecodeAndValidate[T Message](reader io.Reader, maxBytes int64) (T, error) {
	var zero T
	typ := reflect.TypeFor[T]()
	if reader == nil || maxBytes <= 0 || maxBytes == 1<<63-1 || typ.Kind() != reflect.Struct {
		return zero, fmt.Errorf("decode requires a reader, positive bounded limit, and struct message type")
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if int64(len(data)) > maxBytes {
		return zero, ErrMessageTooLarge
	}
	if err != nil {
		return zero, fmt.Errorf("read protocol message: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var message T
	if err := decoder.Decode(&message); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return zero, fmt.Errorf("%w: multiple JSON values", ErrInvalidMessage)
		}
		return zero, fmt.Errorf("%w: trailing data: %w", ErrInvalidMessage, err)
	}
	if err := checkFields(data, typ, "message"); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	if err := message.Validate(); err != nil {
		return zero, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return message, nil
}

// checkFields adds recursive presence, null, and field-name checks to typed
// decoding, using JSON tags as the schema.
func checkFields(data []byte, typ reflect.Type, path string) error {
	if typ == reflect.TypeFor[json.RawMessage]() {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		if typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice {
			return nil
		}
		return fmt.Errorf("%s must not be null", path)
	}
	if typ.Kind() == reflect.Pointer {
		return checkFields(data, typ.Elem(), path)
	}
	switch typ.Kind() {
	case reflect.Struct:
		fields := make(map[string]reflect.StructField)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := strings.Split(field.Tag.Get("json"), ",")
			if !field.IsExported() || tag[0] == "-" {
				continue
			}
			name := tag[0]
			if name == "" {
				name = field.Name
			}
			fields[name] = field
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		if token != json.Delim('{') {
			return fmt.Errorf("%s must be an object", path)
		}
		seen := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			name := token.(string)
			field, exists := fields[name]
			if !exists {
				return fmt.Errorf("%s has unknown field %q", path, name)
			}
			if seen[name] {
				return fmt.Errorf("%s has duplicate field %q", path, name)
			}
			seen[name] = true
			var value json.RawMessage
			if err := decoder.Decode(&value); err != nil {
				return err
			}
			if err := checkFields(value, field.Type, path+"."+name); err != nil {
				return err
			}
		}
		// Use declaration order for deterministic missing-field errors.
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			tag := strings.Split(field.Tag.Get("json"), ",")
			if !field.IsExported() || tag[0] == "-" {
				continue
			}
			name := tag[0]
			if name == "" {
				name = field.Name
			}
			optional := false
			for _, option := range tag[1:] {
				if option == "omitempty" {
					optional = true
				}
			}
			if !seen[name] && !optional {
				return fmt.Errorf("%s is missing field %q", path, name)
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if err := json.Unmarshal(data, &values); err != nil {
			return err
		}
		for i, value := range values {
			if err := checkFields(value, typ.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}
