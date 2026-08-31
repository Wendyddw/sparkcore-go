package executor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// SourceReader opens one lazy input partition during task execution.
type SourceReader interface {
	Open(context.Context, string, plan.PartitionID, int) (Iterator, error)
}

// TextSourceReader reads deterministic contiguous byte ranges from local files.
type TextSourceReader struct{}

// Open opens path and positions an iterator at partition's first complete line.
func (TextSourceReader) Open(
	ctx context.Context,
	path string,
	partition plan.PartitionID,
	numPartitions int,
) (Iterator, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if numPartitions <= 0 || partition < 0 || int(partition) >= numPartitions {
		return nil, fmt.Errorf("invalid partition %d of %d", partition, numPartitions)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open text source %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("stat text source %q: %w", path, err)
	}

	size := info.Size()
	start := size * int64(partition) / int64(numPartitions)
	end := size * int64(partition+1) / int64(numPartitions)
	iterator := &textPartitionIterator{file: file, end: end, last: int(partition) == numPartitions-1}
	if err := iterator.position(start); err != nil {
		file.Close()
		return nil, fmt.Errorf("position text source %q partition %d: %w", path, partition, err)
	}
	return iterator, nil
}

type textPartitionIterator struct {
	file   *os.File
	reader *bufio.Reader
	pos    int64
	end    int64
	last   bool
	done   bool
}

func (i *textPartitionIterator) position(start int64) error {
	if start > 0 {
		if _, err := i.file.Seek(start-1, io.SeekStart); err != nil {
			return err
		}
		previous := []byte{0}
		if _, err := io.ReadFull(i.file, previous); err != nil {
			return err
		}
		if previous[0] != '\n' {
			discard := bufio.NewReader(i.file)
			fragment, err := discard.ReadString('\n')
			start += int64(len(fragment))
			if err != nil && err != io.EOF {
				return err
			}
		}
	}
	if _, err := i.file.Seek(start, io.SeekStart); err != nil {
		return err
	}
	i.reader = bufio.NewReader(i.file)
	i.pos = start
	return nil
}

func (i *textPartitionIterator) Next(ctx context.Context) (Record, bool, error) {
	if i.done {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		i.close()
		return nil, false, err
	}
	if !i.last && i.pos >= i.end {
		i.close()
		return nil, false, nil
	}

	line, err := i.reader.ReadString('\n')
	i.pos += int64(len(line))
	if err != nil && err != io.EOF {
		i.close()
		return nil, false, err
	}
	if err == io.EOF {
		i.close()
	}
	if len(line) == 0 {
		return nil, false, nil
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), true, nil
}

func (i *textPartitionIterator) close() {
	if !i.done {
		i.done = true
		_ = i.file.Close()
	}
}
