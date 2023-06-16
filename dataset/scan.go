package dataset

import (
	"fmt"
	"io"
	"time"

	"github.com/segmentio/encoding/thrift"
	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

const readPageSize = 4 * 1024

var compact = thrift.CompactProtocol{}

type Scanner struct {
	reader *db.FileReader
	file   *parquet.File

	predicates  []Predicate
	projections Projection
}

type ScannerOption func(*Scanner)

func Project(columns ...string) ScannerOption {
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
		scanner.predicates = append(scanner.predicates, newGTEMatcher(col, value))
	}
}

func LessThanOrEqual(column string, value parquet.Value) ScannerOption {
	return func(scanner *Scanner) {
		col, ok := scanner.file.Schema().Lookup(column)
		if !ok {
			return
		}
		scanner.predicates = append(scanner.predicates, newLTEMatcher(col, value))
	}
}

func NewScanner(file *parquet.File, reader *db.FileReader, options ...ScannerOption) *Scanner {
	scanner := &Scanner{
		file:        file,
		reader:      reader,
		predicates:  make([]Predicate, 0),
		projections: NewColumnSelection(),
	}
	for _, option := range options {
		option(scanner)
	}
	return scanner
}

func (s *Scanner) Scan() ([]SelectionResult, error) {
	fmt.Println("Scanning...")
	start := time.Now()
	defer func() {
		fmt.Println("Time taken:", time.Since(start))
	}()

	result := make([]SelectionResult, 0, len(s.file.RowGroups()))
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
				return nil, err
			}
			rowSelections = append(rowSelections, rowSelection)
		}

		filteredRows := pickRanges(rowGroup.NumRows(), rowSelections...)
		result = append(result, filteredRows)
	}
	return result, nil
}

func (s *Scanner) filterRows(chunk parquet.ColumnChunk, ranges SelectionResult, predicate Predicate) (RowSelection, error) {
	pages := chunk.Pages()
	defer pages.Close()

	pageRange := selectPageOffsets(chunk, ranges)
	if err := s.reader.LoadSection(pageRange.from, pageRange.to); err != nil {
		return nil, err
	}

	var numMatches int64
	var selection RowSelection
	for _, rows := range ranges {
		cursor := rows.from
		for cursor < rows.to {
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
			skipFrom, skipTo := cursor, cursor
			for i := 0; i < n; i++ {
				skipTo++
				matches := predicate.Matches(values[i])
				if matches {
					numMatches++
					selection = selection.Skip(skipFrom, skipTo-1)
					skipFrom = skipTo
				}
			}
			selection = selection.Skip(skipFrom, skipTo)
			cursor += numValues
		}
	}
	return selection, nil
}

func selectPageOffsets(chunk parquet.ColumnChunk, ranges SelectionResult) rowRange {
	offsetIndex := chunk.OffsetIndex()
	if len(ranges) == 0 {
		return emptyRange()
	}

	pageOffsets := make([]int64, 0)
	iRange := 0
	iPages := 0
	for iPages < offsetIndex.NumPages() && iRange < len(ranges) {
		firstRowIndex := offsetIndex.FirstRowIndex(iPages)
		var lastRowIndex int64
		if iPages < offsetIndex.NumPages()-1 {
			lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
		} else {
			lastRowIndex = chunk.NumValues()
		}
		pageRange := rowRange{from: firstRowIndex, to: lastRowIndex}
		if ranges[iRange].overlaps(pageRange) {
			pageOffsets = append(pageOffsets, offsetIndex.Offset(iPages))
		}

		if ranges[iRange].before(pageRange) {
			iRange++
		} else {
			iPages++
		}
	}

	if len(pageOffsets) == 0 {
		return emptyRange()
	}
	return rowRange{from: pageOffsets[0], to: pageOffsets[len(pageOffsets)-1]}
}

//type pageDictionaries map[int64]parquet.DictionaryPageHeader
//
//func (s *Scanner) decodeDictionaries() error {
//	decoder := thrift.NewDecoder(compact.NewReader(nil))
//	for rowID, rowGroup := range s.file.Metadata().RowGroups {
//		for colID, column := range rowGroup.Columns {
//			if column.MetaData.DictionaryPageOffset == 0 {
//				continue
//			}
//			sectionReader := io.NewSectionReader(s.reader, column.MetaData.DictionaryPageOffset, column.MetaData.TotalCompressedSize)
//			buffer := bufio.NewReader(sectionReader)
//
//			chunk := s.file.RowGroups()[rowID].ColumnChunks()[colID]
//			dictionary, err := decodeChunkDictionaries(buffer, decoder, chunk)
//			if err != nil {
//				return err
//			}
//			if dictionary == nil {
//				continue
//			}
//		}
//	}
//}
//
//func decodeChunkDictionaries(buffer *bufio.Reader, decoder *thrift.Decoder, chunk parquet.ColumnChunk) (pageDictionaries, error) {
//	dictionaries := make(pageDictionaries)
//
//	decoder.Reset(compact.NewReader(buffer))
//	header := &format.PageHeader{}
//	if err := decoder.Decode(header); err != nil {
//		panic(err)
//	}
//	if header.DictionaryPageHeader.NumValues == 0 {
//		return nil, nil
//	}
//	capacity := header.CompressedPageSize + (readPageSize - header.CompressedPageSize%readPageSize)
//	pageData := make([]byte, header.CompressedPageSize, capacity)
//	if _, err := io.ReadFull(buffer, pageData); err != nil && err != io.EOF {
//		panic(err)
//	}
//
//	encoding := header.DictionaryPageHeader.Encoding
//	if encoding == format.PlainDictionary {
//		encoding = format.Plain
//	}
//
//	pageType := chunk.Type()
//	values, err := pageType.Decode(pageType.NewValues(nil, nil), pageData, parquet.LookupEncoding(encoding))
//	if err != nil {
//		return nil, err
//	}
//
//
//	return pageType.NewDictionary(chunk.Column(), int(values.Size()), values), nil
//}
