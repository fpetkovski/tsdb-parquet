package db_test

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/storage"
)

func BenchmarkWriter(b *testing.B) {
	b.StopTimer()
	numSeries := 1000
	chunksPerSeries := 100
	numLabels := 20
	chunkSeries := make([]storage.ChunkSeries, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		lbls := make([]string, 0, 2*numLabels)
		for j := 0; j < numLabels; j++ {
			lbls = append(lbls, fmt.Sprintf("label_%d", j), fmt.Sprintf("value_%d", j))
		}
		chunkSeries = append(chunkSeries, newSeries(b, chunksPerSeries, lbls...))
	}

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		createParquetFile(b, chunkSeries)
	}
}
