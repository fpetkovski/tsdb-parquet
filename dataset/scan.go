package dataset

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/format"

	"fpetkovski/tsdb-parquet/db"
)

const readPageSize = 4 * 1024


type Scanner struct {
	reader      *db.FileReader
	file        *parquet.File
	columnIndex map[string]int

	filters     []predicate
	projections []string
}

type ScannerOption func(*Scanner)

func Projection(columns ...string) ScannerOption {
	return func(scanner *Scanner) {
		scanner.projections = columns
	}
}

func Predicate(column parquet.Column, value string) ScannerOption {
	return func(scanner *Scanner) {
		scanner.filters = append(scanner.filters, newPredicate(column, value))
	}
}

func NewScanner(file *parquet.File, reader *db.FileReader, options ...ScannerOption) *Scanner {
	scanner := &Scanner{
		file:        file,
		reader:      reader,
		filters:     make([]predicate, 0),
		projections: make([]string, 0),
	}
	for _, option := range options {
		option(scanner)
	}

	columnIndex := make(map[string]int)
	for i, col := range file.Schema().Columns() {
		columnIndex[col[0]] = i
	}
	scanner.columnIndex = columnIndex

	compact := thrift.CompactProtocol{}
	decoder := thrift.NewDecoder(compact.NewReader(nil))
	for rowID, rowGroup := range file.Metadata().RowGroups {
		for colID, chunk := range rowGroup.Columns {
			if chunk.MetaData.DictionaryPageOffset == 0 {
				continue
			}

			sectionReader := io.NewSectionReader(reader, chunk.MetaData.DictionaryPageOffset, chunk.MetaData.TotalCompressedSize)
			buffer := bufio.NewReader(sectionReader)
			decoder.Reset(compact.NewReader(buffer))
			header := &format.PageHeader{}
			if err := decoder.Decode(header); err != nil {
				panic(err)
			}
			if header.DictionaryPageHeader.NumValues == 0 {
				continue
			}
			capacity := header.CompressedPageSize + (readPageSize - header.CompressedPageSize%readPageSize)
			pageData := make([]byte, header.CompressedPageSize, capacity)
			if _, err := io.ReadFull(buffer, pageData); err != nil && err != io.EOF {
				panic(err)
			}

			encoding := header.DictionaryPageHeader.Encoding
			if encoding == format.PlainDictionary {
				encoding = format.Plain
			}

			//fmt.Println("Decoding dictionary for row group", rowID, "column", file.Schema().Columns()[colID], "with encoding", encoding)
			column := file.RowGroups()[rowID].ColumnChunks()[colID]
			pageType := column.Type()

			_, err := pageType.Decode(pageType.NewValues(nil, nil), pageData, parquet.LookupEncoding(encoding))
			if err != nil {
				fmt.Println("column", file.Schema().Columns()[colID], err.Error())
			}
		}
	}

	return scanner
}

func (s *Scanner) Scan() error {
	start := time.Now()
	defer func() {
		fmt.Println("Time taken:", time.Since(start))
	}()

	for rowID, rowGroup := range s.file.RowGroups() {
		for _, f := range s.filters {
			colID := s.columnIndex[f.column.Name()]
			columnChunk := rowGroup.ColumnChunks()[colID]
			filterValue := f.value
			bloom := rowGroup.ColumnChunks()[colID].BloomFilter()
			if bloom != nil {
				columnName := s.file.Metadata().RowGroups[rowID].Columns[colID].MetaData.PathInSchema
				fmt.Println("Checking bloom filter for columnChunk", columnName)
				hasValue, err := bloom.Check(filterValue)
				if err != nil {
					return err
				}
				if !hasValue {
					continue
				}
			}

			fmt.Println("Page statistics")
			columnIndex := columnChunk.ColumnIndex()
			for i := 0; i < columnIndex.NumPages(); i++ {
				fmt.Println(columnIndex.MinValue(i).String())
				fmt.Println(columnIndex.MaxValue(i).String())
			}

			fmt.Println("Row IDs")
			offsetIndex := columnChunk.OffsetIndex()
			for i := 0; i < offsetIndex.NumPages(); i++ {
				fmt.Println(offsetIndex.FirstRowIndex(i))
			}

			lastPageIndex := columnIndex.NumPages() - 1
			from := offsetIndex.Offset(0)
			to := offsetIndex.Offset(lastPageIndex) + offsetIndex.CompressedPageSize(lastPageIndex)
			fmt.Println("Loading section", from, to)
			if err := s.reader.LoadSection(from, to); err != nil {
				return err
			}

			chunk := rowGroup.ColumnChunks()[colID]
			pages := chunk.Pages()
			if err := pages.SeekToRow(0); err != nil {
				return err
			}
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}

				values := make([]parquet.Value, page.NumValues())
				_, err = page.Values().ReadValues(values)
				if err != nil && !errors.Is(err, io.EOF) {
					panic(err)
				}
				fmt.Println("Read new page for columnChunk", chunk.Column(), page.NumRows(), page.Size()/1024, "KB")
			}
			pages.Close()
		}
	}
	return nil
}
