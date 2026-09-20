package shuffle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type fileWriter struct {
	mu      sync.Mutex
	store   *Filesystem
	ctx     context.Context
	output  MapOutput
	hashes  []hash.Hash
	private string
	final   string
	stop    func() bool
	done    chan struct{}
	closed  bool
	err     error
}

func newFileWriter(s *Filesystem, ctx context.Context, output MapOutput, private, final string) *fileWriter {
	w := &fileWriter{store: s, ctx: ctx, output: output, hashes: make([]hash.Hash, len(output.Buckets)),
		private: private, final: final, done: make(chan struct{})}
	w.mu.Lock()
	w.stop = context.AfterFunc(ctx, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		if !w.closed {
			w.abort(context.Cause(ctx))
		}
	})
	w.mu.Unlock()
	return w
}

func (w *fileWriter) Write(record Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.check(); err != nil {
		return err
	}
	data, err := EncodeRecord(record)
	if err != nil {
		return w.abort(err)
	}
	partition, err := PartitionFor(record.Key, w.output.NumReducePartitions)
	if err != nil {
		return w.abort(err)
	}
	// Open only the selected bucket; no file descriptors accumulate across buckets.
	f, err := w.store.root.OpenFile(filepath.Join(w.private, bucketName(partition)), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return w.abort(err)
	}
	n, writeErr := f.Write(data)
	if writeErr == nil && n != len(data) {
		writeErr = io.ErrShortWrite
	}
	if err := errors.Join(writeErr, f.Close()); err != nil {
		return w.abort(err)
	}
	if w.hashes[partition] == nil {
		w.hashes[partition] = sha256.New()
	}
	_, _ = w.hashes[partition].Write(data)
	w.output.Buckets[partition].RecordCount++
	w.output.Buckets[partition].ByteCount += int64(len(data))
	return w.check()
}

func (w *fileWriter) Publish() (MapOutput, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.check(); err != nil {
		return MapOutput{}, err
	}
	for i := range w.output.Buckets {
		if err := w.check(); err != nil {
			return MapOutput{}, err
		}
		bucket := &w.output.Buckets[i]
		if w.hashes[i] != nil {
			bucket.SHA256 = hex.EncodeToString(w.hashes[i].Sum(nil))
			continue
		}
		f, err := w.store.root.OpenFile(filepath.Join(w.private, bucketName(bucket.PartitionID)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			err = f.Close()
		}
		if err != nil {
			return MapOutput{}, w.abort(err)
		}
	}
	data, err := manifestBytes(w.output)
	if err != nil {
		return MapOutput{}, w.abort(err)
	}
	if err := w.store.root.WriteFile(filepath.Join(w.private, "manifest.json"), data, 0600); err != nil {
		return MapOutput{}, w.abort(err)
	}
	if err := w.check(); err != nil {
		return MapOutput{}, err
	}
	if _, err := w.store.root.Lstat(w.final); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			err = ErrOutputExists
		}
		return MapOutput{}, w.abort(err)
	}
	// A published directory contains its manifest, so concurrent publishers cannot
	// replace it with Rename. The losing writer discards only its private directory.
	if err := w.store.root.Rename(w.private, w.final); err != nil {
		if _, statErr := w.store.root.Lstat(w.final); statErr == nil {
			err = errors.Join(ErrOutputExists, err)
		}
		return MapOutput{}, w.abort(err)
	}
	w.finish(nil)
	output := w.output
	output.Buckets = append([]BucketMetadata(nil), output.Buckets...)
	return output, nil
}

func (w *fileWriter) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.err
	}
	return w.abort(nil)
}

func (w *fileWriter) check() error {
	if w.closed {
		if w.err != nil {
			return w.err
		}
		return ErrClosed
	}
	if w.ctx.Err() != nil {
		return w.abort(context.Cause(w.ctx))
	}
	return nil
}

func (w *fileWriter) abort(cause error) error {
	err := errors.Join(cause, w.store.root.RemoveAll(w.private))
	w.finish(err)
	return err
}

func (w *fileWriter) finish(err error) {
	w.closed, w.err = true, err
	w.stop()
	w.store.release(w.output.Attempt.RunID)
	close(w.done)
}
