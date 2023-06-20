package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/gcs"
	"gopkg.in/yaml.v3"
)

const (
	kb = 1 * 1024
	mb = 1 * 1024 * 1024
)

func BenchmarkBucketReads(b *testing.B) {
	config := GCSConfig{
		Bucket: "shopify-o11y-metrics-scratch",
	}
	conf, err := yaml.Marshal(config)
	require.NoError(b, err)

	bucket, err := gcs.NewBucket(context.Background(), nil, conf, "parquet-reader")
	require.NoError(b, err)

	bucketReader := NewBucketReader("compact-2.7.parquet", bucket)
	chunkSizes := []int{
		16 * mb,
		8 * mb,
		4 * mb,
		1 * mb,
		256 * kb,
	}
	for _, chunkSize := range chunkSizes {
		b.Run(fmt.Sprintf("%dKB", chunkSize), func(b *testing.B) {
			chunkedReader := NewChunkedBucketReader(bucketReader, chunkSize)
			buffer := make([]byte, 16*mb)
			_, err = chunkedReader.ReadAt(buffer, 0)
			require.NoError(b, err)
		})
	}
}
