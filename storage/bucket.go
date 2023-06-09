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
func (i BucketReader) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	return i.bucket.Get(ctx, name)
}

func (i BucketReader) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	return i.bucket.Attributes(ctx, name)
}

func (i BucketReader) ReadAt(p []byte, off int64) (n int, err error) {
	fmt.Println("Reading", len(p), "at", off)
	rangeReader, err := i.bucket.GetRange(context.Background(), i.name, off, int64(len(p)))
	if err != nil {
		return 0, err
	}

	return io.ReadFull(rangeReader, p)
}
