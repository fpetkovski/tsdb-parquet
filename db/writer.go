package db

import (
	"fmt"
	"os"
	"sort"

	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/schema"
)

const (
	bufferMaxRows      = 200000
	dataFileSuffix     = ".parquet"
	metadataFileSuffix = ".metadata"
)

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
	w.partID++
	partName := fmt.Sprintf("%s/part.%d", w.dir, w.partID)

	if err := w.writeData(partName); err != nil {
		return err
	}
	return w.writeMetadata(partName)
}

func (w *Writer) openWriter(f *os.File) *parquet.GenericWriter[any] {
	return parquet.NewGenericWriter[any](f,
		parquet.DefaultWriterConfig(),
		w.schema.ParquetSchema(),
		parquet.WriteBufferSize(bufferMaxRows),
		parquet.DataPageStatistics(true),
		parquet.BloomFilters(w.bloomFilters...),
	)
}

func (w *Writer) openBuffer() {
	w.buffer = parquet.NewGenericBuffer[any](
		w.schema.ParquetSchema(),
		parquet.ColumnBufferCapacity(bufferMaxRows),
		parquet.SortingRowGroupConfig(parquet.SortingColumns(w.sortingColumns...)),
	)
}

func (w *Writer) writeData(partName string) error {
	defer w.buffer.Reset()
	f, err := os.Create(partName + dataFileSuffix)
	if err != nil {
		return err
	}

	sort.Sort(w.buffer)
	pqWriter := w.openWriter(f)
	defer pqWriter.Close()

	_, err = parquet.CopyRows(pqWriter, w.buffer.Rows())

	return err
}

func (w *Writer) writeMetadata(partName string) error {
	f, err := os.Open(partName + dataFileSuffix)
	if err != nil {
		return err
	}
	pqReader, err := file.NewParquetReader(f)
	defer pqReader.Close()

	metaFile, err := os.Create(partName + metadataFileSuffix)
	if err != nil {
		return err
	}
	defer metaFile.Close()

	_, err = pqReader.MetaData().WriteTo(metaFile, nil)
	return err
}
