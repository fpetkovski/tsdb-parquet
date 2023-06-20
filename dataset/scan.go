package dataset

import (
	"sort"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type Scanner struct {
	reader db.SectionLoader
	file   *parquet.File

	predicates Predicates
}

type ScannerOption func(*Scanner)

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

func NewScanner(file *parquet.File, reader db.SectionLoader, options ...ScannerOption) *Scanner {
	scanner := &Scanner{
		file:       file,
		reader:     reader,
		predicates: make(Predicates, 0),
	}
	for _, option := range options {
		option(scanner)
	}
	sort.Sort(scanner.predicates)
	return scanner
}

func (s *Scanner) Scan() ([]SelectionResult, error) {
	result := make([]SelectionResult, 0, len(s.file.RowGroups()))
	for _, rowGroup := range s.file.RowGroups() {
		rowSelections := s.predicates.SelectRows(rowGroup)
		filteredRows, err := s.predicates.FilterRows(rowGroup, rowSelections)
		if err != nil {
			return nil, err
		}

		rowGroupRows := SelectRows(rowGroup, filteredRows)
		result = append(result, rowGroupRows)
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
