package prometheus

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/schema"
)

func TestQuerier(t *testing.T) {
	series := []labels.Labels{
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "1"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "2"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "0"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "1"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "2"),
	}

	dir := t.TempDir()
	writer := db.NewWriter(dir, []string{labels.MetricName, "job", "instance"})
	for i, lbls := range series {
		for ts := int64(0); ts < 3; ts++ {
			chunk := schema.Chunk{Labels: lbls, SeriesID: int64(i), MinT: ts * 100, MaxT: (ts + 1) * 100}
			require.NoError(t, writer.Write(chunk))
		}
	}
	require.NoError(t, writer.Close())
	require.NoError(t, writer.Compact())

	bucket, err := filesystem.NewBucket(dir)
	require.NoError(t, err)

	cacheDir := t.TempDir()
	reader, err := db.NewFileReader("compact", bucket, db.WithSectionCacheDir(cacheDir))
	require.NoError(t, err)
	defer reader.Close()

	fmt.Println("Opening parquet file")
	pqFile, err := parquet.OpenFile(reader, reader.FileSize())
	require.NoError(t, err)

	q, err := NewQuerier(context.Background(), pqFile, reader.SectionLoader(), math.MinInt64, math.MaxInt64)
	require.NoError(t, err)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
	}
	hints := &storage.SelectHints{
		Grouping: []string{"instance"},
	}
	sset := q.Select(false, hints, matchers...)
	for sset.Next() {
		fmt.Println(sset.At().Labels())
	}
	require.NoError(t, sset.Err())
}
