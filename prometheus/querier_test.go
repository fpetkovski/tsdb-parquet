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

	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"
)

func TestQuerier(t *testing.T) {
	cases := []struct {
		name      string
		series    []labels.Labels
		batchSize int64
		numChunks int
		expected  []labels.Labels
	}{
		{
			name:      "1 series with batch size 1",
			batchSize: 1,
			series: []labels.Labels{
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
			},
			expected: []labels.Labels{
				labels.FromStrings(schema.SeriesIDColumn, "0", "instance", "0"),
			},
		},
		{
			name:      "3 series with batch size 2",
			batchSize: 2,
			series: []labels.Labels{
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "1"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "0"),
			},
			expected: []labels.Labels{
				labels.FromStrings(schema.SeriesIDColumn, "0", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "2", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "1", "instance", "1"),
			},
		},
		{
			name:      "3 series with batch size 1",
			batchSize: 1,
			series: []labels.Labels{
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "1"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "0"),
			},
			expected: []labels.Labels{
				labels.FromStrings(schema.SeriesIDColumn, "0", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "2", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "1", "instance", "1"),
			},
		},
		{
			name:      "6 series with batch size 2",
			batchSize: 2,
			series: []labels.Labels{
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "1"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "2"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "0"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "1"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "2"),
			},
			expected: []labels.Labels{
				labels.FromStrings(schema.SeriesIDColumn, "0", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "3", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "1", "instance", "1"),
				labels.FromStrings(schema.SeriesIDColumn, "4", "instance", "1"),
				labels.FromStrings(schema.SeriesIDColumn, "2", "instance", "2"),
				labels.FromStrings(schema.SeriesIDColumn, "5", "instance", "2"),
			},
		},
		{
			name:      "6 series with batch size 3",
			batchSize: 3,
			series: []labels.Labels{
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "1"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "2"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "0"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "1"),
				labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "2"),
			},
			expected: []labels.Labels{
				labels.FromStrings(schema.SeriesIDColumn, "0", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "3", "instance", "0"),
				labels.FromStrings(schema.SeriesIDColumn, "1", "instance", "1"),
				labels.FromStrings(schema.SeriesIDColumn, "4", "instance", "1"),
				labels.FromStrings(schema.SeriesIDColumn, "2", "instance", "2"),
				labels.FromStrings(schema.SeriesIDColumn, "5", "instance", "2"),
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := createParquetFile(t, c.series)

			pqFile, reader, err := openParquetFile(dir, t.TempDir())
			require.NoError(t, err)

			ctx := context.Background()
			pqStorage := NewParquetFile(pqFile, reader.SectionLoader(), WithLabelsBatchSize(c.batchSize))
			q, err := pqStorage.Querier(ctx, math.MinInt64, math.MaxInt64)
			require.NoError(t, err)

			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"),
			}
			hints := &storage.SelectHints{Grouping: []string{"instance"}}
			sset := q.Select(false, hints, matchers...)
			result, err := expandSeries(sset)
			require.NoError(t, err)
			require.Equal(t, result, c.expected)
		})
	}
}

func openParquetFile(dir string, cacheDir string) (*parquet.File, *db.FileReader, error) {
	bucket, err := filesystem.NewBucket(dir)
	if err != nil {
		return nil, nil, err
	}

	reader, err := db.NewFileReader("compact", bucket, db.WithSectionCacheDir(cacheDir))
	if err != nil {
		return nil, nil, err
	}

	fmt.Println("Opening parquet file")
	pqFile, err := parquet.OpenFile(reader, reader.FileSize())
	if err != nil {
		return nil, nil, err
	}

	return pqFile, reader, nil
}

func createParquetFile(t *testing.T, sset []labels.Labels) string {
	const (
		numChunks = 3
		oneMinute = 60_000
	)

	dir := t.TempDir()
	writer := db.NewWriter(dir, []string{labels.MetricName, "job", "instance"})
	minTime := int64(0)
	for iChunk := 0; iChunk < numChunks; iChunk++ {
		chunks := make([]schema.Chunk, 0, len(sset))
		for iSeries, s := range sset {
			chunk := schema.Chunk{
				Labels:   s.Map(),
				SeriesID: int64(iSeries),
				MinT:     minTime,
				MaxT:     minTime + oneMinute,
			}
			chunks = append(chunks, chunk)
		}
		require.NoError(t, writer.Write(chunks))
		minTime += oneMinute
	}
	require.NoError(t, writer.Close())
	require.NoError(t, writer.Compact())
	return dir
}

func expandSeries(sset storage.SeriesSet) ([]labels.Labels, error) {
	var result []labels.Labels
	for sset.Next() {
		result = append(result, sset.At().Labels().Copy())
	}
	return result, sset.Err()
}
