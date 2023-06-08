package write

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/pkg/model"
	"fpetkovski/prometheus-parquet/schema"
)

type RowGroup struct {
	schema *schema.ChunkSchema
	buffer *parquet.RowBuffer[any]
}

func (r *RowGroup) WriteRows(rows []parquet.Row) (int, error) {
	return r.buffer.WriteRows(rows)
}

func NewRowGroup(schema *schema.ChunkSchema) *RowGroup {
	return &RowGroup{
		schema: schema,
		buffer: parquet.NewRowBuffer[any](schema.ParquetSchema()),
	}
}

func (r *RowGroup) WriteChunks(lbls labels.Labels, chunkMeta chunks.Meta) (int, error) {
	chunk := model.Chunk{
		Labels:     lbls,
		MinT:       chunkMeta.MinTime,
		MaxT:       chunkMeta.MaxTime,
		ChunkBytes: chunkMeta.Chunk.Bytes(),
	}
	rows := []parquet.Row{r.schema.MakeChunkRow(chunk)}
	return r.buffer.WriteRows(rows)
}

func (r *RowGroup) NumRows() int64 {
	return r.buffer.NumRows()
}

func (r *RowGroup) ColumnChunks() []parquet.ColumnChunk {
	return r.buffer.ColumnChunks()
}

func (r *RowGroup) Schema() *parquet.Schema {
	return r.schema.ParquetSchema()
}

func (r *RowGroup) SortingColumns() []parquet.SortingColumn {
	return nil
}

func (r *RowGroup) Rows() parquet.Rows {
	return r.buffer.Rows()
}
