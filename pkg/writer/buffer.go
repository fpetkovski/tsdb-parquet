package writer

import (
	"fpetkovski/prometheus-parquet/schema"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/segmentio/parquet-go"
	"unicode"
)

type WriteBuffer struct {
	labels map[string]struct{}
	rows   []parquet.Row
	schema *schema.ChunkSchema
}

func newBuffer(schema *schema.ChunkSchema, size int) *WriteBuffer {
	return &WriteBuffer{
		labels: make(map[string]struct{}),
		rows:   make([]parquet.Row, 0, size),
		schema: schema,
	}
}

func (b *WriteBuffer) Append(lbls labels.Labels, chunk chunks.Meta) {
	for _, lbl := range lbls {
		if !unicode.IsLetter(rune(lbl.Name[0])) {
			continue
		}
		b.labels[lbl.Name] = struct{}{}
	}

	b.rows = append(b.rows, Chunk{
		Labels: lbls,
		MinT:   chunk.MinTime,
		MaxT:   chunk.MaxTime,
		Chunk:  chunk.Chunk,
	}.ToParquetRow(b.schema))
}

func (b *WriteBuffer) Len() int {
	return len(b.rows)
}

func (b *WriteBuffer) Reset() {
	b.labels = make(map[string]struct{})
	b.rows = b.rows[:0]
}

func (b *WriteBuffer) IsFull() bool {
	return len(b.rows) == cap(b.rows)
}

func (b *WriteBuffer) ToRowGroup() (parquet.RowGroup, error) {
	buffer := parquet.NewBuffer(b.schema.ParquetSchema())
	if _, err := buffer.WriteRows(b.rows); err != nil {
		return nil, err
	}
	return buffer, nil
}
