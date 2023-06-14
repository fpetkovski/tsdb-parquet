package dataset

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/format"

	"fpetkovski/tsdb-parquet/db"
)

const readPageSize = 4 * 1024

type Scanner struct {
	reader *db.FileReader
	file   *parquet.File

	predicates  []RowSelector
	projections ColumnSelector
}

type ScannerOption func(*Scanner)

func Projection(columns ...string) ScannerOption {
	return func(scanner *Scanner) {
		scanner.projections = NewColumnSelection(columns...)
	}
}

func Equals(column string, value string) ScannerOption {
	return func(scanner *Scanner) {
		col, ok := scanner.file.Schema().Lookup(column)
		if !ok {
			return
		}
		scanner.predicates = append(scanner.predicates, newEqualsMatcher(col, value))
	}
}

func GreaterThanOrEqual(column string, value parquet.Value) ScannerOption {
	return func(scanner *Scanner) {
		col, ok := scanner.file.Schema().Lookup(column)
		if !ok {
			return
		}
		scanner.predicates = append(scanner.predicates, NewGTEMatcher(col, value))
	}
}

func LessThanOrEqual(column string, value parquet.Value) ScannerOption {
	return func(scanner *Scanner) {
		col, ok := scanner.file.Schema().Lookup(column)
		if !ok {
			return
		}
		scanner.predicates = append(scanner.predicates, NewLTEMatcher(col, value))
	}
}

func NewScanner(file *parquet.File, reader *db.FileReader, options ...ScannerOption) *Scanner {
	scanner := &Scanner{
		file:        file,
		reader:      reader,
		predicates:  make([]RowSelector, 0),
		projections: NewColumnSelection(),
	}
	for _, option := range options {
		option(scanner)
	}

	// loadDictionaries(file, reader)

	return scanner
}

func (s *Scanner) Scan() error {
	start := time.Now()
	defer func() {
		fmt.Println("Time taken:", time.Since(start))
	}()

	for _, rowGroup := range s.file.RowGroups() {
		rowSelections := make([]RowSelection, 0, len(s.predicates))
		for _, predicate := range s.predicates {
			selection := predicate.SelectRows(rowGroup)
			rowSelections = append(rowSelections, selection)
		}
		selectedRows := pickRanges(rowGroup.NumRows(), rowSelections...)

		for _, predicate := range s.predicates {
			chunk := rowGroup.ColumnChunks()[predicate.Column().ColumnIndex]
			rowSelection, err := s.filterRows(chunk, selectedRows, predicate)
			if err != nil {
				return err
			}

			rowSelections = append(rowSelections, rowSelection)
		}

		selectedRows = pickRanges(rowGroup.NumRows(), rowSelections...)
		fmt.Println(selectedRows, selectedRows.NumRows())
	}
	return nil
}

func (s *Scanner) filterRows(chunk parquet.ColumnChunk, ranges SelectionResult, predicate RowSelector) (RowSelection, error) {
	pages := chunk.Pages()
	defer pages.Close()

	var numMatches int64
	var selections RowSelection
	for _, rows := range ranges {
		cursor := rows.from
		for cursor < rows.to {
			skipFrom, skipTo := cursor, cursor

			if err := pages.SeekToRow(cursor); err != nil {
				return nil, err
			}
			page, err := pages.ReadPage()
			if err != nil {
				return nil, err
			}
			numValues := rows.to - cursor
			if numValues > page.NumValues() {
				numValues = page.NumValues()
			}
			values := make([]parquet.Value, numValues)
			n, err := page.Values().ReadValues(values)
			if err != nil && err != io.EOF {
				return nil, err
			}
			for i := 0; i < n; i++ {
				skipTo++
				matches := predicate.Matches(values[i])
				if matches {
					numMatches++
					selections = append(selections, skip(skipFrom, skipTo-1))
					skipFrom = skipTo
				}
			}
			selections = append(selections, skip(skipFrom, skipTo))
			cursor += numValues
		}
	}
	fmt.Println(numMatches)
	return selections, nil
}

func loadDictionaries(file *parquet.File, reader *db.FileReader) {
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
}
