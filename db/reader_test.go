package db

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func TestSectionLoading(t *testing.T) {
	dir := t.TempDir()
	partName, err := generatePart(dir, 50_000)
	require.NoError(t, err)

	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	inspector := &bucketInspector{Bucket: bucket}

	cacheDir := t.TempDir()
	reader, err := OpenFileReader(partName, inspector, WithSectionCacheDir(cacheDir))
	require.NoError(t, err)

	loader := reader.SectionLoader()
	sec, err := loader.NewSection(0, 5*1000)
	require.NoError(t, err)

	for chunk := 0; chunk < 5; chunk++ {
		require.NoError(t, sec.LoadNext())

		buf := make([]byte, 500)
		for readNum := 0; readNum < 100; readNum++ {
			_, err = reader.ReadAt(buf, int64(chunk*1000))
			require.NoError(t, err)
		}
		fmt.Println(string(buf))
		fmt.Println()
	}

	assertNumSections(t, cacheDir, 1)
	require.Equal(t, 1, inspector.getRangeRequests)

	require.NoError(t, sec.Close())
	assertNumSections(t, cacheDir, 0)

	require.NoError(t, reader.Close())
	assertNumSections(t, cacheDir, 0)
}

func generatePart(dir string, length int) (string, error) {
	partName := fmt.Sprintf("%s/test.parquet", dir)
	f, err := os.Create(partName)
	if err != nil {
		return "", err
	}
	defer f.Close()

	writer := bufio.NewWriter(f)
	for i := 0; i < length; i++ {
		if _, err := writer.WriteString(fmt.Sprintf("%d-", i)); err != nil {
			return "", err
		}
	}
	if err := writer.Flush(); err != nil {
		return "", err
	}

	return "test", nil
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
