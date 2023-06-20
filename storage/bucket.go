package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/thanos-io/objstore"
)

type GCSConfig struct {
	Bucket string `yaml:"bucket"`
}

type BucketReader struct {
	name   string
	bucket objstore.Bucket
}

func NewBucketReader(name string, bucket objstore.Bucket) *BucketReader {
	return &BucketReader{
		name:   name,
		bucket: bucket,
	}
}
func (r BucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return r.bucket.Get(ctx, name)
}

func (r BucketReader) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return r.bucket.Attributes(ctx, name)
}

func (r BucketReader) ReadAt(p []byte, off int64) (n int, err error) {
	fmt.Println("Read bucket at", off, fmt.Sprintf("%dKB", len(p)/1024))
	rangeReader, err := r.bucket.GetRange(context.Background(), r.name, off, int64(len(p)))
	if err != nil {
		return 0, err
	}

	return io.ReadFull(rangeReader, p)
}

func (r BucketReader) ReaderAt(p []byte, off int64) (closer io.ReadCloser, err error) {
	fmt.Println("Read bucket at", off, fmt.Sprintf("%dKB", len(p)/1024))
	return r.bucket.GetRange(context.Background(), r.name, off, int64(len(p)))
}
