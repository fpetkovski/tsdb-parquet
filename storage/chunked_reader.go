package storage

import (
	"io"

	"golang.org/x/sync/errgroup"
)

type chunkedBucketReader struct {
	maxReadSize      int
	concurrencyLimit int
	reader           io.ReaderAt
}

func NewChunkedBucketReader(reader io.ReaderAt, maxReadSize int) *chunkedBucketReader {
	return &chunkedBucketReader{
		maxReadSize:      maxReadSize,
		concurrencyLimit: 16,
		reader:           reader,
	}
}

func (r chunkedBucketReader) ReadAt(p []byte, off int64) (n int, err error) {
	var wg errgroup.Group
	var errChan = make(chan error, len(p)/r.maxReadSize+1)
	for bytesRead := 0; bytesRead < len(p); bytesRead += r.maxReadSize {
		readUntil := minInt(bytesRead+r.maxReadSize, len(p))
		part := p[bytesRead:readUntil]
		partOffset := int64(bytesRead) + off
		wg.Go(func() error {
			_, err := r.reader.ReadAt(part, partOffset)
			return err
		})
	}
	if err := wg.Wait(); err != nil {
		return 0, err
	}

	close(errChan)
	for err := range errChan {
		return 0, err
	}

	return len(p), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
