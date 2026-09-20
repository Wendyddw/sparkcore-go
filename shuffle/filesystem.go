package shuffle

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// MaxManifestBytes bounds manifest I/O and the number of buckets an attempt can describe.
const MaxManifestBytes = 1 << 20

// Filesystem stores immutable attempts under a confined root. Atomic directory
// publication requires Unix rename semantics on one filesystem. The root is
// owned by trusted application processes; this is not a multi-tenant storage service.
type Filesystem struct {
	root   *os.Root
	mu     sync.Mutex
	active map[string]int
	closed bool
}

var _ Store = (*Filesystem)(nil)

// NewFilesystem creates or opens an absolute shared directory. Close the store
// after all of its readers/writers have completed.
func NewFilesystem(path string) (*Filesystem, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("shuffle root must be absolute")
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	return &Filesystem{root: root, active: make(map[string]int)}, nil
}

func (s *Filesystem) acquire(run string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.active[run]++
	return nil
}

func (s *Filesystem) release(run string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[run]--
	if s.active[run] == 0 {
		delete(s.active, run)
	}
}

// CleanupRun removes only the named namespace. All processes using that run must
// be stopped first: the activity check covers this store instance, not other processes.
func (s *Filesystem) CleanupRun(run string) error {
	if !lowerHex(run, 32) {
		return fmt.Errorf("invalid shuffle run namespace")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.active[run] != 0 {
		return ErrStoreBusy
	}
	return s.root.RemoveAll(run)
}

// Close rejects active handles rather than invalidating their file operations.
func (s *Filesystem) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if len(s.active) != 0 {
		return ErrStoreBusy
	}
	s.closed = true
	return s.root.Close()
}

func attemptPath(a AttemptIdentity) string {
	return filepath.Join(a.RunID, fmt.Sprintf("job-%d", a.JobID), fmt.Sprintf("shuffle-%d", a.ShuffleID),
		fmt.Sprintf("stage-%d", a.StageID), fmt.Sprintf("stage-attempt-%d", a.StageAttemptID),
		fmt.Sprintf("map-%d", a.MapPartitionID), fmt.Sprintf("task-attempt-%d", a.TaskAttemptID))
}

func bucketName(id plan.PartitionID) string { return fmt.Sprintf("bucket-%d.jsonl", id) }

func initialOutput(a AttemptIdentity, maps, reduces int) (MapOutput, error) {
	if err := a.Validate(); err != nil {
		return MapOutput{}, err
	}
	if maps <= 0 || int(a.MapPartitionID) >= maps || reduces <= 0 {
		return MapOutput{}, fmt.Errorf("invalid shuffle partition counts or map partition")
	}
	digest := sha256.Sum256(nil)
	empty := BucketMetadata{SHA256: hex.EncodeToString(digest[:])}
	minimum, _ := json.Marshal(empty)
	// Reject impossible manifests before allocating a caller-sized slice.
	if reduces > MaxManifestBytes/len(minimum) {
		return MapOutput{}, fmt.Errorf("shuffle manifest exceeds size limit")
	}
	output := MapOutput{Version: FormatVersion, Attempt: a, NumMapPartitions: maps,
		NumReducePartitions: reduces, Buckets: make([]BucketMetadata, reduces)}
	for i := range output.Buckets {
		output.Buckets[i] = empty
		output.Buckets[i].PartitionID = plan.PartitionID(i)
	}
	if _, err := manifestBytes(output); err != nil {
		return MapOutput{}, err
	}
	return output, nil
}

func manifestBytes(output MapOutput) ([]byte, error) {
	if err := output.Validate(); err != nil {
		return nil, err
	}
	data, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxManifestBytes {
		return nil, fmt.Errorf("shuffle manifest exceeds size limit")
	}
	return data, nil
}

func (s *Filesystem) Begin(ctx context.Context, a AttemptIdentity, maps, reduces int) (AttemptWriter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("shuffle context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	output, err := initialOutput(a, maps, reduces)
	if err != nil {
		return nil, err
	}
	if err := s.acquire(a.RunID); err != nil {
		return nil, err
	}
	final := attemptPath(a)
	if _, err := s.root.Lstat(final); !errors.Is(err, os.ErrNotExist) {
		s.release(a.RunID)
		if err == nil {
			err = ErrOutputExists
		}
		return nil, err
	}
	parent := filepath.Dir(final)
	if err := s.root.MkdirAll(parent, 0700); err != nil {
		s.release(a.RunID)
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		s.release(a.RunID)
		return nil, err
	}
	private := filepath.Join(parent, ".tmp-"+hex.EncodeToString(nonce[:]))
	if err := s.root.Mkdir(private, 0700); err != nil {
		s.release(a.RunID)
		return nil, err
	}
	w := newFileWriter(s, ctx, output, private, final)
	return w, nil
}
