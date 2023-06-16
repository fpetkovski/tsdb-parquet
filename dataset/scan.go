package dataset

import (
	"fmt"
	"time"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

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
		scanner.predicates = append(scanner.predicates, newEqualsMatcher(scanner.reader, col, value))
	}
}

func GreaterThanOrEqual(column string, value parquet.Value) ScannerOption {
	return func(scanner *Scanner) {
		col, ok := scanner.file.Schema().Lookup(column)
		if !ok {
			return
		}
		scanner.predicates = append(scanner.predicates, newGTEMatcher(scanner.reader, col, value))
	}
}

func LessThanOrEqual(column string, value parquet.Value) ScannerOption {
	return func(scanner *Scanner) {
		col, ok := scanner.file.Schema().Lookup(column)
		if !ok {
			return
		}
		scanner.predicates = append(scanner.predicates, newLTEMatcher(scanner.reader, col, value))
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
			rowSelections = append(rowSelections, predicate.SelectRows(rowGroup))
		}
		selectedRows := pickRanges(rowGroup.NumRows(), rowSelections...)

		for _, predicate := range s.predicates {
			selection, err := predicate.FilterRows(rowGroup, selectedRows)
			if err != nil {
				return nil, err
			}
			rowSelections = append(rowSelections, selection)
		}

		filteredRows := pickRanges(rowGroup.NumRows(), rowSelections...)
		result = append(result, filteredRows)
	}
	return result, nil
}

//var compact = thrift.CompactProtocol{}

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
