package shuffle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func (s *Filesystem) OpenBucket(ctx context.Context, output MapOutput, partition plan.PartitionID) (BucketReader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("shuffle context is nil")
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	output.Buckets = append([]BucketMetadata(nil), output.Buckets...)
	expected, err := manifestBytes(output)
	if err != nil {
		return nil, err
	}
	if partition < 0 || int(partition) >= output.NumReducePartitions {
		return nil, fmt.Errorf("invalid reduce partition %d", partition)
	}
	if err := s.acquire(output.Attempt.RunID); err != nil {
		return nil, err
	}
	fail := func(err error) (BucketReader, error) {
		s.release(output.Attempt.RunID)
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, &InputError{Attempt: output.Attempt, PartitionID: partition, Err: err}
	}
	directory := attemptPath(output.Attempt)
	manifest, err := s.openRegular(filepath.Join(directory, "manifest.json"))
	if err != nil {
		return fail(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(manifest, MaxManifestBytes+1))
	if err := errors.Join(readErr, manifest.Close()); err != nil {
		return fail(err)
	}
	// Our on-disk manifest format is canonical encoding/json output. Exact matching
	// also rejects missing zero-valued fields, duplicate fields and trailing data.
	if !bytes.Equal(data, expected) {
		return fail(fmt.Errorf("manifest differs from accepted output"))
	}
	f, err := s.openRegular(filepath.Join(directory, bucketName(partition)))
	if err != nil {
		return fail(err)
	}
	stat, err := f.Stat()
	if err == nil && stat.Size() != output.Buckets[partition].ByteCount {
		err = fmt.Errorf("bucket size differs from accepted output")
	}
	if err != nil {
		return fail(errors.Join(err, f.Close()))
	}
	r := &fileBucketReader{store: s, ctx: ctx, file: f, output: output, partition: partition,
		digest: sha256.New(), done: make(chan struct{})}
	r.decoder = NewRecordDecoder(bucketStream{r})
	r.mu.Lock()
	r.stop = context.AfterFunc(ctx, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if !r.closed {
			r.finish(context.Cause(ctx))
		}
	})
	r.mu.Unlock()
	return r, nil
}

func (s *Filesystem) openRegular(name string) (*os.File, error) {
	info, err := s.root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("shuffle input is not a regular file: %s", name)
	}
	f, err := s.root.Open(name)
	if err != nil {
		return nil, err
	}
	info, err = f.Stat()
	if err == nil && !info.Mode().IsRegular() {
		err = fmt.Errorf("shuffle input is not a regular file: %s", name)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

type fileBucketReader struct {
	mu        sync.Mutex
	store     *Filesystem
	ctx       context.Context
	file      *os.File
	output    MapOutput
	partition plan.PartitionID
	decoder   *RecordDecoder
	digest    hash.Hash
	bytes     int64
	records   int64
	stop      func() bool
	done      chan struct{}
	closed    bool
	err       error
	closeErr  error
}

// bucketStream is used only while the reader mutex is held by Next.
type bucketStream struct{ reader *fileBucketReader }

func (s bucketStream) Read(p []byte) (int, error) {
	r := s.reader
	if r.ctx.Err() != nil {
		return 0, context.Cause(r.ctx)
	}
	n, err := r.file.Read(p)
	r.bytes += int64(n)
	_, _ = r.digest.Write(p[:n])
	if r.bytes > r.output.Buckets[r.partition].ByteCount {
		return n, fmt.Errorf("bucket grew beyond accepted size")
	}
	return n, err
}

func (r *fileBucketReader) Next() (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Record{}, r.err
	}
	if r.ctx.Err() != nil {
		r.finish(context.Cause(r.ctx))
		return Record{}, r.err
	}
	record, err := r.decoder.Next()
	if r.ctx.Err() != nil {
		r.finish(context.Cause(r.ctx))
		return Record{}, r.err
	}
	if err == nil {
		partition, partitionErr := PartitionFor(record.Key, r.output.NumReducePartitions)
		r.records++
		switch {
		case partitionErr != nil:
			err = partitionErr
		case partition != r.partition:
			err = fmt.Errorf("record belongs to reduce partition %d", partition)
		case r.records > r.output.Buckets[r.partition].RecordCount:
			err = fmt.Errorf("bucket contains more records than accepted output")
		default:
			return record, nil
		}
	}
	if err == io.EOF {
		bucket := r.output.Buckets[r.partition]
		if r.records != bucket.RecordCount || r.bytes != bucket.ByteCount || hex.EncodeToString(r.digest.Sum(nil)) != bucket.SHA256 {
			err = fmt.Errorf("bucket record count, size or SHA-256 differs from accepted output")
		}
	}
	if err != io.EOF {
		err = r.inputError(err)
	}
	r.finish(err)
	return Record{}, r.err
}

func (r *fileBucketReader) inputError(err error) error {
	return &InputError{Attempt: r.output.Attempt, PartitionID: r.partition, Err: err}
}

func (r *fileBucketReader) finish(err error) {
	r.closed = true
	r.stop()
	r.closeErr = r.file.Close()
	if r.closeErr != nil {
		if err == io.EOF {
			err = nil
		}
		err = errors.Join(err, r.inputError(r.closeErr))
	}
	r.err = err
	r.store.release(r.output.Attempt.RunID)
	close(r.done)
}

func (r *fileBucketReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.closed {
		r.finish(ErrClosed)
	}
	return r.closeErr
}
