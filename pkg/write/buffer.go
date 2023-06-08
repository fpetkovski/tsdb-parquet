package write

import (
	"unicode"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/pkg/model"
	"fpetkovski/prometheus-parquet/schema"
)

type Buffer struct {
	labels map[string]struct{}
	chunks []model.Chunk
	schema *schema.ChunkSchema
}

func newBuffer(schema *schema.ChunkSchema, size int) *Buffer {
	return &Buffer{
		labels: make(map[string]struct{}),
		chunks: make([]model.Chunk, 0, size),
		schema: schema,
	}
}

func (b *Buffer) Append(lbls labels.Labels, chunk chunks.Meta) {
	for _, lbl := range lbls {
		if !unicode.IsLetter(rune(lbl.Name[0])) {
			continue
		}
		b.labels[lbl.Name] = struct{}{}
	}

	b.chunks = append(b.chunks, model.Chunk{
		Labels:     lbls,
		MinT:       chunk.MinTime,
		MaxT:       chunk.MaxTime,
		ChunkBytes: chunk.Chunk.Bytes(),
	})
}

func (b *Buffer) Len() int {
	return len(b.chunks)
}

func (b *Buffer) Reset() {
	b.labels = make(map[string]struct{})
	b.chunks = b.chunks[:0]
}

func (b *Buffer) IsFull() bool {
	return len(b.chunks) == cap(b.chunks)
}

func (b *Buffer) ToRowGroup() (parquet.RowGroup, error) {
	buffer := parquet.NewBuffer(b.schema.ParquetSchema())
	rows := make([]parquet.Row, 0, len(b.chunks))
	for _, chunk := range b.chunks {
		rows = append(rows, b.schema.MakeChunkRow(chunk))
	}

	if _, err := buffer.WriteRows(rows); err != nil {
		return nil, err
	}
	return buffer, nil
}
