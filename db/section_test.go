package db

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"fpetkovski/tsdb-parquet/schema"
)

func TestSectionLoading(t *testing.T) {
	series := []labels.Labels{
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "1", "b", "2"),
		labels.FromStrings("a", "1", "b", "1"),
		labels.FromStrings("a", "2", "b", "2"),
	}
	dir := createFile(t, series)

	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	inspector := &bucketInspector{Bucket: bucket}

	cacheDir := t.TempDir()
	reader, err := OpenFileReader("part.0", inspector, WithSectionCacheDir(cacheDir))
	require.NoError(t, err)
	assertNumSections(t, cacheDir, 1)
	require.Equal(t, 1, inspector.getRangeRequests)

	loader := reader.SectionLoader()
	closer, err := loader.LoadSection(0, 100)
	require.NoError(t, err)

	buf := make([]byte, 100)
	for i := 0; i < 100; i++ {
		_, err = reader.ReadAt(buf, 0)
		require.NoError(t, err)
	}
	assertNumSections(t, cacheDir, 2)
	require.Equal(t, 2, inspector.getRangeRequests)

	require.NoError(t, closer.Close())
	assertNumSections(t, cacheDir, 1)

	require.NoError(t, reader.Close())
	assertNumSections(t, cacheDir, 0)
}

func assertNumSections(t *testing.T, cacheDir string, expectedSections int) {
	sections, err := os.ReadDir(cacheDir)
	require.NoError(t, err)
	require.Len(t, sections, expectedSections)
}

func createFile(t *testing.T, series []labels.Labels) string {
	lbls := []string{"a", "b"}
	chunkSchema := schema.MakeChunkSchema(lbls)

	dir := t.TempDir()
	writer := NewWriter(dir, lbls, chunkSchema)

	for _, s := range series {
		err := writer.Write(schema.Chunk{Labels: s, MinT: 0, MaxT: 60})
		require.NoError(t, err)
	}

	require.NoError(t, writer.Close())
	return dir
}

type bucketInspector struct {
	objstore.Bucket

	getRangeRequests int
}

func (b *bucketInspector) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.getRangeRequests++
	return b.Bucket.GetRange(ctx, name, off, length)
}
