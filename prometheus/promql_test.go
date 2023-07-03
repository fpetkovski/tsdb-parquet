package prometheus

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/engine"
)

func TestPromQL(t *testing.T) {
	series := []labels.Labels{
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "0"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "api-server", "instance", "1"),
		labels.FromStrings(labels.MetricName, "http_requests_total", "job", "kubelet", "instance", "0"),
	}

	dir := createParquetFile(t, series)
	pqFile, reader, err := openParquetFile(dir, t.TempDir())
	require.NoError(t, err)

	q := NewParquetFile(pqFile, reader.SectionLoader())
	require.NoError(t, err)

	ng := engine.New(engine.Opts{
		EngineOpts: promql.EngineOpts{
			Timeout: 30 * time.Second,
		},
	})
	var (
		queryStr = `sum(http_requests_total)`
		start    = time.Unix(0, 0)
		end      = time.Unix(300, 0)
		step     = 60 * time.Second
	)
	query, err := ng.NewRangeQuery(context.Background(), q, nil, queryStr, start, end, step)
	require.NoError(t, err)

	result := query.Exec(context.Background())
	require.NoError(t, result.Err)

	expectedSeries := promql.Matrix{
		promql.Series{
			Metric: labels.Labels{},
			Floats: []promql.FPoint{
				{T: 0, F: 3},
				{T: 60_000, F: 3},
				{T: 120_000, F: 3},
				{T: 180_000, F: 3},
				{T: 240_000, F: 3},
				{T: 300_000, F: 3},
			},
		},
	}
	require.Equal(t, expectedSeries, result.Value)
}
