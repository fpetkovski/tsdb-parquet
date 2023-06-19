package db

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"

	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"golang.org/x/exp/slices"

	"fpetkovski/tsdb-parquet/schema"
)

const (
	MaxPageSize = 8 * 1024

	writeBufferSize = 256 * 1024
	dataFileSuffix  = ".parquet"
	metadataFileSuffix = ".metadata"
)

var partRegex = regexp.MustCompile(`part.(\d+).parquet`)

type WriterOption func(*Writer)

type Writer struct {
	dir    string
	partID int
	buffer *parquet.GenericBuffer[any]

	sortingColumns []parquet.SortingColumn
	schema         *schema.ChunkSchema
	bloomFilters   []parquet.BloomFilterColumn

	pageBufferSize int
}

func NewWriter(dir string, columns []string, chunkSchema *schema.ChunkSchema, option ...WriterOption) *Writer {
	sortingColums := make([]parquet.SortingColumn, 0, len(columns)+2)
	sortingColums = append(sortingColums, parquet.Ascending(schema.MinTColumn))
	sortingColums = append(sortingColums, parquet.Ascending(schema.MaxTColumn))
	for _, lbl := range columns {
		sortingColums = append(sortingColums, parquet.Ascending(lbl))
	}
	slices.SortFunc(sortingColums, func(a, b parquet.SortingColumn) bool {
		aName, bName := a.Path()[0], b.Path()[0]
		return CompareColumns(aName, bName)
	})

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

		pageBufferSize: MaxPageSize,
	}
	for _, opt := range option {
		opt(writer)
	}
	writer.openBuffer()

	return writer
}

func (w *Writer) Write(chunk schema.Chunk) error {
	if _, err := w.buffer.WriteRows([]parquet.Row{w.schema.MakeChunkRow(chunk)}); err != nil {
		return err
	}

	if w.buffer.NumRows() >= writeBufferSize {
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
		if !partRegex.MatchString(fileName.Name()) {
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

func (w *Writer) openWriter(f *os.File) *parquet.GenericWriter[any] {
	return parquet.NewGenericWriter[any](f,
		w.schema.ParquetSchema(),
		parquet.SortingWriterConfig(parquet.SortingColumns(w.sortingColumns...)),
		parquet.DefaultWriterConfig(),
		parquet.WriteBufferSize(writeBufferSize),
		parquet.PageBufferSize(w.pageBufferSize),
		parquet.DataPageStatistics(true),
		parquet.BloomFilters(w.bloomFilters...),
	)
}

func (w *Writer) openBuffer() {
	w.buffer = parquet.NewGenericBuffer[any](
		w.schema.ParquetSchema(),
		parquet.ColumnBufferCapacity(writeBufferSize),
		parquet.SortingRowGroupConfig(parquet.SortingColumns(w.sortingColumns...)),
	)
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

func CompareColumns(aName string, bName string) bool {
	if aName == labels.MetricName {
		return true
	}
	if bName == labels.MetricName {
		return false
	}

	if aName == schema.MinTColumn {
		return true
	}
	if bName == schema.MinTColumn {
		return false
	}

	if aName == schema.MaxTColumn {
		return true
	}
	if bName == schema.MaxTColumn {
		return false
	}
	return aName < bName
}
