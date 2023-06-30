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
		expected  []labels.Labels
	}{
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
			name: "6 series with batch size 3",
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
		{
			name: "3 series with batch size 2",
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
			name: "3 series with batch size 1",
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
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			writer := db.NewWriter(dir, []string{labels.MetricName, "job", "instance"})
			for i, lbls := range c.series {
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

			ctx := context.Background()
			q, err := NewQuerier(ctx, pqFile, reader.SectionLoader(), math.MinInt64, math.MaxInt64, WithLabelsBatchSize(c.batchSize))
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

func expandSeries(sset storage.SeriesSet) ([]labels.Labels, error) {
	var result []labels.Labels
	for sset.Next() {
		result = append(result, sset.At().Labels().Copy())
	}
	return result, sset.Err()
}
