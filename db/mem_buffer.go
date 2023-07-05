package db

import (
	"io"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"

	"Shopify/thanos-parquet-engine/schema"
)

type memBuffer struct {
	schema        *schema.ChunkSchema
	sortingConfig []parquet.SortingColumn

	chunks []schema.Chunk
}

func newMemBuffer(
	chunkSchema *schema.ChunkSchema,
	sortingConfig []parquet.SortingColumn,
	size int,
) *memBuffer {
	return &memBuffer{
		schema:        chunkSchema,
		sortingConfig: sortingConfig,

		chunks: make([]schema.Chunk, 0, size),
	}
}

func (m *memBuffer) Write(chunks schema.Chunk) (int, error) {
	m.chunks = append(m.chunks, chunks)
	return len(m.chunks), nil
}

func (m *memBuffer) Rows() parquet.RowReader {
	return &memBufferRows{
		schema: m.schema,
		chunks: m.chunks,
	}
}

func (m *memBuffer) NumRows() int {
	return len(m.chunks)
}

func (m *memBuffer) Reset() {
	m.chunks = m.chunks[:0]
}

func (m *memBuffer) Len() int {
	return m.NumRows()
}

func (m *memBuffer) Less(i, j int) bool {
	return compareChunks(m.chunks[i], m.chunks[j], m.schema) < 0
}

func (m *memBuffer) Swap(i, j int) {
	m.chunks[i], m.chunks[j] = m.chunks[j], m.chunks[i]
}

type memBufferRows struct {
	schema *schema.ChunkSchema
	chunks []schema.Chunk
}

func (m *memBufferRows) ReadRows(rows []parquet.Row) (int, error) {
	n := len(rows)
	defer func() {
		m.chunks = m.chunks[n:]
	}()
	if len(m.chunks) < n {
		n = len(m.chunks)
	}
	if n == 0 {
		return 0, io.EOF
	}

	for i := 0; i < n; i++ {
		rows[i] = m.schema.MakeChunkRow(m.chunks[i])
	}
	return n, nil
}

func compareChunks(a, b schema.Chunk, chunkSchema *schema.ChunkSchema) int {
	nameOrder := strings.Compare(a.Labels[labels.MetricName], b.Labels[labels.MetricName])
	if nameOrder != 0 {
		return nameOrder
	}

	mintOrder := compareInts(a.MinT, b.MinT)
	if mintOrder != 0 {
		return mintOrder
	}

	maxtOrder := compareInts(a.MaxT, b.MaxT)
	if maxtOrder != 0 {
		return maxtOrder
	}

	for _, lbl := range chunkSchema.Labels() {
		order := strings.Compare(a.Labels[lbl], b.Labels[lbl])
		if order != 0 {
			return order
		}
	}
	return 0
}

func compareInts(a, b int64) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}
