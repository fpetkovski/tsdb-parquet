package db

import (
	"os"
	"sort"
	"strconv"

	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/schema"
)

const bufferMaxRows = 100000

type Writer struct {
	dir    string
	partID int
	buffer *parquet.GenericBuffer[any]

	sortingColumns []parquet.SortingColumn
	schema         *schema.ChunkSchema
	bloomFilters   []parquet.BloomFilterColumn
}

func NewWriter(dir string, columns []string, chunkSchema *schema.ChunkSchema) *Writer {
	sortingColums := make([]parquet.SortingColumn, 0, len(columns)+2)
	sortingColums = append(sortingColums, parquet.Ascending(schema.MinTColumn))
	sortingColums = append(sortingColums, parquet.Ascending(schema.MaxTColumn))
	for _, lbl := range columns {
		sortingColums = append(sortingColums, parquet.Ascending(lbl))
	}

	bloomFilters := make([]parquet.BloomFilterColumn, 0, len(columns))
	for _, lbl := range columns {
		bloomFilters = append(bloomFilters, parquet.SplitBlockFilter(10, lbl))
	}

	writer := &Writer{
		dir:            dir,
		partID:         -1,
		sortingColumns: sortingColums,
		bloomFilters:   bloomFilters,
		schema:         chunkSchema,
	}
	writer.openBuffer()

	return writer
}

func (w *Writer) Write(chunk schema.Chunk) error {
	if _, err := w.buffer.WriteRows([]parquet.Row{w.schema.MakeChunkRow(chunk)}); err != nil {
		return err
	}

	if w.buffer.NumRows() >= bufferMaxRows {
		if err := w.flushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) Close() error {
	return w.flushBuffer()
}

func (w *Writer) flushBuffer() error {
	defer w.buffer.Reset()

	w.partID++
	f, err := os.Create(w.dir + "/part." + strconv.Itoa(w.partID))
	if err != nil {
		return err
	}
	writerConfig := parquet.DefaultWriterConfig()
	pqWriter := parquet.NewGenericWriter[any](f,
		writerConfig,
		w.schema.ParquetSchema(),
		parquet.WriteBufferSize(bufferMaxRows),
		parquet.DataPageStatistics(true),
		parquet.BloomFilters(w.bloomFilters...),
	)
	defer pqWriter.Close()

	sort.Sort(w.buffer)
	_, err = parquet.CopyRows(pqWriter, w.buffer.Rows())
	return err
}

func (w *Writer) openBuffer() {
	w.buffer = parquet.NewGenericBuffer[any](
		w.schema.ParquetSchema(),
		parquet.ColumnBufferCapacity(bufferMaxRows),
		parquet.SortingRowGroupConfig(parquet.SortingColumns(w.sortingColumns...)),
	)
}
