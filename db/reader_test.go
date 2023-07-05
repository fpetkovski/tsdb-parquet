package db

import (
	"context"
	"io"
	"os"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"Shopify/thanos-parquet-engine/schema"
)

func TestSectionLoading(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, generatePart(dir, 10000))

	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	inspector := &bucketInspector{Bucket: bucket}

	cacheDir := t.TempDir()
	reader, err := NewFileReader("part.0", inspector, WithSectionCacheDir(cacheDir))
	require.NoError(t, err)

	assertNumSections(t, cacheDir, 1)
	require.Equal(t, 1, inspector.getRangeRequests)

	loader := reader.SectionLoader()
	var readBatchSize int64 = 4 * 1024
	sec, err := loader.NewSectionSize(0, reader.FileSize(), readBatchSize)
	require.NoError(t, err)

	for chunk := 0; chunk < 5; chunk++ {
		require.NoError(t, sec.LoadNext())

		buf := make([]byte, readBatchSize)
		for readNum := 0; readNum < 10; readNum++ {
			_, err = reader.ReadAt(buf, int64(chunk)*readBatchSize)
			require.NoError(t, err)
		}
	}

	assertNumSections(t, cacheDir, 2)
	require.Equal(t, 2, inspector.getRangeRequests)

	require.NoError(t, sec.Close())
	assertNumSections(t, cacheDir, 1)

	require.NoError(t, reader.Close())
	assertNumSections(t, cacheDir, 0)
}

func generatePart(dir string, numSeries int) error {
	columns := []string{"a", "b"}
	writer := NewWriter(dir, columns)
	defer writer.Close()

	for i := 0; i < numSeries; i++ {
		lblA := strconv.Itoa(i)
		lblB := strconv.Itoa(i % 2)
		lbls := labels.FromStrings("a", lblA, "b", lblB)
		chunk := schema.Chunk{
			Labels:     lbls.Map(),
			MinT:       0,
			MaxT:       100,
			ChunkBytes: nil,
		}
		if err := writer.Write(chunk); err != nil {
			return err
		}
	}
	return nil
}

func assertNumSections(t *testing.T, cacheDir string, expectedSections int) {
	sections, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	require.Len(t, sections, expectedSections)
}

type bucketInspector struct {
	objstore.Bucket

	getRangeRequests int
}

func (b *bucketInspector) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.getRangeRequests++
	return b.Bucket.GetRange(ctx, name, off, length)
}
