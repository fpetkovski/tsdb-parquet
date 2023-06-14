package db

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/pkg/errors"
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

func (w *Writer) Compact() error {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return errors.Wrap(err, "failed listing directory")
	}

	pqFiles := make([]*parquet.File, 0, len(files))
	for _, fileName := range files {
		if fileName.IsDir() {
			continue
		}
		if !strings.HasSuffix(fileName.Name(), dataFileSuffix) {
			continue
		}
		fileReader, err := os.Open(w.dir + "/" + fileName.Name())
		if err != nil {
			return errors.Wrap(err, "failed opening file "+fileName.Name())
		}
		defer fileReader.Close()

		stat, err := fileReader.Stat()
		if err != nil {
			return errors.Wrap(err, "failed getting file stats")
		}

		pqFile, err := parquet.OpenFile(fileReader, stat.Size())
		pqFiles = append(pqFiles, pqFile)
	}

	output, err := os.Create(w.dir + "/compact.parquet")
	if err != nil {
		return errors.Wrap(err, "failed creating output file")
	}
	rowGroups := make([]parquet.RowGroup, 0)
	for _, reader := range pqFiles {
		if reader.NumRows() > 0 {
			rowGroups = append(rowGroups, reader.RowGroups()...)
		}
	}

	mergeGroups, err := parquet.MergeRowGroups(
		rowGroups,
		w.schema.ParquetSchema(),
		parquet.SortingRowGroupConfig(parquet.SortingColumns(w.sortingColumns...)),
	)
	if err != nil {
		return errors.Wrap(err, "failed merging row groups")
	}
	writer := w.openWriter(output)
	_, err = parquet.CopyRows(writer, mergeGroups.Rows())
	if err != nil {
		return errors.Wrap(err, "failed copying rows")
	}
	if err := writer.Close(); err != nil {
		return errors.Wrap(err, "failed closing writer")
	}

	if err := w.createMetadataFile(path.Join(w.dir, "compact")); err != nil {
		return errors.Wrap(err, "failed writing metadata")
	}

	return nil
}

func (w *Writer) Close() error {
	return w.Flush()
}

func (w *Writer) Flush() error {
	return w.flushBuffer()
}

func (w *Writer) flushBuffer() error {
	defer w.buffer.Reset()
	if w.buffer.NumRows() == 0 {
		return nil
	}

	w.partID++
	partName := fmt.Sprintf("%s/part.%d", w.dir, w.partID)
	if err := w.flushBufferToFile(partName); err != nil {
		return err
	}
	return w.createMetadataFile(partName)
}

func (w *Writer) openWriter(f *os.File) *parquet.GenericWriter[any] {
	return parquet.NewGenericWriter[any](f,
		w.schema.ParquetSchema(),
		parquet.SortingWriterConfig(parquet.SortingColumns(w.sortingColumns...)),
		parquet.DefaultWriterConfig(),
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

func (w *Writer) flushBufferToFile(partName string) error {
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

func (w *Writer) createMetadataFile(partName string) error {
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
