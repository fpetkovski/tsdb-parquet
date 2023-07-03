package db_test

import (
	"io"
	"os"
	"path"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"
)

func TestWriter(t *testing.T) {
	chunkSeries := []storage.ChunkSeries{
		newSeries(t, 47, labels.MetricName, "http_requests_total", "job", "api-server", "instance", "abc"),
		newSeries(t, 47, labels.MetricName, "http_requests_total", "job", "api-server", "instance", "def"),
		newSeries(t, 47, labels.MetricName, "http_requests_total", "job", "api-server", "instance", "ghi"),
		newSeries(t, 47, labels.MetricName, "http_requests_total", "job", "api-server", "instance", "jke"),
	}
	dir := createParquetFile(t, chunkSeries)
	pqFile, err := openParquetFile(dir)
	require.NoError(t, err)

	readBatch := 30
	nread := 0
	for _, rowGroup := range pqFile.RowGroups() {
		rowGroupRows := rowGroup.Rows()
		for {
			rows := make([]parquet.Row, readBatch)
			n, err := rowGroupRows.ReadRows(rows)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			if n == 0 {
				break
			}

			for _, row := range rows[:n] {
				chk, err := chunkenc.FromData(chunkenc.EncXOR, row[schema.ChunkPos].ByteArray())
				require.NoError(t, err)
				require.Equal(t, 120, chk.NumSamples())
			}
			nread += n
		}
	}
}

func openParquetFile(dir string) (*parquet.File, error) {
	fpath := path.Join(dir, "compact.parquet")
	file, err := os.Open(fpath)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	pqFile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, err
	}

	return pqFile, nil
}

func createParquetFile(t testing.TB, series []storage.ChunkSeries) string {
	dir := t.TempDir()
	writer := db.NewWriter(dir, []string{labels.MetricName, "job", "instance"})
	for i, chunkSeries := range series {
		seriesChunks, err := storage.ExpandChunks(chunkSeries.Iterator(nil))
		require.NoError(t, err)

		for _, chk := range seriesChunks {
			chunk := schema.Chunk{
				Labels:     chunkSeries.Labels(),
				SeriesID:   int64(i),
				MinT:       chk.MinTime,
				MaxT:       chk.MaxTime,
				ChunkBytes: chk.Chunk.Bytes(),
			}
			require.NoError(t, writer.Write(chunk))
		}
	}
	require.NoError(t, writer.Close())
	require.NoError(t, writer.Compact())
	return dir
}

func newSeries(tb testing.TB, numChunks int, lbls ...string) storage.ChunkSeries {
	seriesChunks := make([]chunks.Meta, numChunks)
	var ts int64
	var val float64
	for i := 0; i < numChunks; i++ {
		var chunk chunkenc.Chunk
		minTS := ts
		chunk, ts, val = makeChunk(tb, ts, val)
		seriesChunks[i] = chunks.Meta{
			Chunk:   chunk,
			MinTime: minTS,
			MaxTime: ts,
		}
	}

	return &storage.ChunkSeriesEntry{
		Lset: labels.FromStrings(lbls...),
		ChunkIteratorFn: func(iterator chunks.Iterator) chunks.Iterator {
			return storage.NewListChunkSeriesIterator(seriesChunks...)
		},
	}
}

func makeChunk(tb testing.TB, ts int64, val float64) (*chunkenc.XORChunk, int64, float64) {
	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	require.NoError(tb, err)

	for j := 0; j < 120; j++ {
		app.Append(ts, val)
		ts += 30
		val += 10
	}
	return chunk, ts, val
}
