package schema

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/encoding"
)

const (
	SeriesIDColumn   = "__series__id"
	MinTColumn       = "__mint"
	MaxTColumn       = "__maxt"
	ChunkBytesColumn = "__chunk_bytes"

	SeriesIDPos = 0
	MinTPos     = 1
	MaxTPos     = 2
	ChunkPos    = 3
)

type Chunk struct {
	Labels map[string]string

	// SeriesID is the ID of the series inside the parquet file.
	// It can be used to compare series within a single file, but not across files.
	SeriesID int64
	// MinT is the min time for the chunk.
	MinT int64
	// MaxT is the max time for the chunk.
	MaxT int64
	// ChunkBytes are the encoded bytes of the chunk.
	ChunkBytes []byte
}

type chunkLabels []string

type chunkRow struct {
	labels chunkLabels
}

func newChunkRow(labels chunkLabels) *chunkRow {
	return &chunkRow{labels: labels}
}

func (c chunkRow) String() string {
	return fmt.Sprintf("%v", c.labels)
}

func (c chunkRow) Type() parquet.Type { return groupType{} }

func (c chunkRow) Optional() bool { return false }

func (c chunkRow) Repeated() bool { return false }

func (c chunkRow) Required() bool { return true }

func (c chunkRow) Leaf() bool { return false }

func (c chunkRow) Fields() []parquet.Field {
	fields := make([]parquet.Field, 4, 4+len(c.labels))
	fields[0] = newInt64Column(SeriesIDColumn)
	fields[1] = newInt64Column(MinTColumn)
	fields[2] = newInt64Column(MaxTColumn)
	fields[3] = newByteArrayColumn(ChunkBytesColumn)

	for _, lbl := range c.labels {
		fields = append(fields, newStringColumn(lbl))
	}
	return fields
}

func (c chunkRow) Encoding() encoding.Encoding { return nil }

func (c chunkRow) Compression() compress.Codec { return nil }

func (c chunkRow) GoType() reflect.Type { return reflect.TypeOf(chunkRow{}) }

type ChunkSchema struct {
	schema *parquet.Schema
	labels []string
}

func MakeChunkSchema(lbls []string) *ChunkSchema {
	sort.Strings(lbls)

	schema := parquet.NewSchema("chunk", newChunkRow(lbls))
	return &ChunkSchema{
		schema: schema,
		labels: lbls,
	}
}

func (c *ChunkSchema) ParquetSchema() *parquet.Schema {
	return c.schema
}

func (c *ChunkSchema) MakeChunkRow(chunk Chunk) parquet.Row {
	row := make(parquet.Row, 4, len(c.labels)+4)

	row[0] = parquet.Int64Value(chunk.SeriesID).Level(0, 0, SeriesIDPos)
	row[1] = parquet.Int64Value(chunk.MinT).Level(0, 0, MinTPos)
	row[2] = parquet.Int64Value(chunk.MaxT).Level(0, 0, MaxTPos)
	row[3] = parquet.ByteArrayValue(chunk.ChunkBytes).Level(0, 0, ChunkPos)

	for labelIndex, labelName := range c.labels {
		labelVal := chunk.Labels[labelName]
		colVal := parquet.ByteArrayValue([]byte(labelVal)).Level(0, 0, 4+labelIndex)
		row = append(row, colVal)
	}

	return row
}
