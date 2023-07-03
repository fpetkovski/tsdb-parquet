package db_test

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func BenchmarkWriter(b *testing.B) {
	b.StopTimer()
	numSeries := 10_000
	chunksPerSeries := 100
	chunkSeries := make([]storage.ChunkSeries, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		lbls := []string{labels.MetricName, "http_requests_total", "job", "api-server", "instance", fmt.Sprintf("instance-%d", i)}
		chunkSeries = append(chunkSeries, newSeries(b, chunksPerSeries, lbls...))
	}

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		createParquetFile(b, chunkSeries)
	}
}
