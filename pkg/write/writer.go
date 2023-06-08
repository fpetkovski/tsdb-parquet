package write

import (
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/schema"
)

type BufferedWriter struct {
	writer *parquet.Writer
	buffer *Buffer
	schema *schema.ChunkSchema
}

func NewBufferedWriter(writer *parquet.Writer, schema *schema.ChunkSchema, bufferSize int) *BufferedWriter {
	return &BufferedWriter{
		buffer: newBuffer(schema, bufferSize),
		writer: writer,
		schema: schema,
	}
}

func (bw *BufferedWriter) Append(lbls labels.Labels, chunk chunks.Meta) error {
	bw.buffer.Append(lbls, chunk)
	if bw.buffer.IsFull() {
		return bw.flush()
	}
	return nil
}

func (bw *BufferedWriter) flush() error {
	if bw.buffer.Len() == 0 {
		return nil
	}

	group, err := bw.buffer.ToRowGroup()
	if err != nil {
		return err
	}

	if _, err := bw.writer.WriteRowGroup(group); err != nil {
		return err
	}

	bw.buffer.Reset()
	return nil
}

func (bw *BufferedWriter) Close() {
	if err := bw.flush(); err != nil {
		fmt.Println(err.Error())
	}
}
