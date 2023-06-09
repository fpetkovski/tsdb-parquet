package dataset

import (
	"io"
	"sync"

	"github.com/segmentio/parquet-go"

	"Shopify/thanos-parquet-engine/db"
)

type RowFilter interface {
	FilterRows(parquet.ColumnChunk, SelectionResult) (RowSelection, error)
}

type matchFunc func(parquet.Value) bool

type decodingFilter struct {
	reader  db.SectionLoader
	matches func(parquet.Value) bool
}

func NewDecodingFilter(reader db.SectionLoader, matches matchFunc) RowFilter {
	return &decodingFilter{
		reader:  reader,
		matches: matches,
	}
}

func (r decodingFilter) FilterRows(chunk parquet.ColumnChunk, ranges SelectionResult) (RowSelection, error) {
	pages := SelectPages(chunk, ranges)
	defer pages.Close()

	offsetFrom, offsetTo := pages.PageOffset(0), pages.PageOffset(pages.NumPages()-1)
	section, err := r.reader.NewSection(offsetFrom, offsetTo)
	if err != nil {
		return nil, err
	}
	section = db.AsyncSection(section, 3)
	defer section.Close()

	var numMatches int64
	var selection RowSelection
	values := make([]parquet.Value, 4*1024)
	for {
		if loadErr := section.LoadNext(); loadErr != nil && loadErr != io.EOF {
			return nil, loadErr
		}
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		n, err := page.Values().ReadValues(values)
		if err != nil && err != io.EOF {
			return nil, err
		}

		skipFrom, skipTo := pages.CurrentRowIndex(), pages.CurrentRowIndex()
		for i := 0; i < n; i++ {
			skipTo++
			matches := r.matches(values[i])
			if matches {
				numMatches++
				selection = selection.Skip(skipFrom, skipTo-1)
				skipFrom = skipTo
			}
		}
		parquet.Release(page)
		selection = selection.Skip(skipFrom, skipTo)
	}
	return selection, nil
}

type dictionaryFilter struct {
	reader  db.SectionLoader
	matches func(parquet.Value) bool
}

func NewDictionaryFilter(reader db.SectionLoader, matches matchFunc) RowFilter {
	return &dictionaryFilter{
		reader:  reader,
		matches: matches,
	}
}

func (r dictionaryFilter) FilterRows(chunk parquet.ColumnChunk, ranges SelectionResult) (RowSelection, error) {
	pages := SelectPages(chunk, ranges)
	defer pages.Close()

	offsetFrom, offsetTo := pages.PageOffset(0), pages.PageOffset(pages.NumPages()-1)
	section, err := r.reader.NewSection(offsetFrom, offsetTo)
	if err != nil {
		return nil, err
	}
	section = db.AsyncSection(section, 3)
	defer section.Close()

	var dictionaryValue int32 = -1
	var once sync.Once
	var selection RowSelection
	for {
		if loadErr := section.LoadNext(); loadErr != nil && loadErr != io.EOF {
			return nil, loadErr
		}
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		data := page.Data()
		once.Do(func() {
			dictionaryValue = getDictionaryEncodedValue(page, r.matches)
		})
		if dictionaryValue == -1 {
			selection = selection.Skip(pages.CurrentRowIndex(), page.NumRows())
			parquet.Release(page)
			break
		}

		encodedValues := data.Int32()
		skipFrom, skipTo := pages.CurrentRowIndex(), pages.CurrentRowIndex()
		for _, val := range encodedValues {
			skipTo++
			if val == dictionaryValue {
				selection = selection.Skip(skipFrom, skipTo-1)
				skipFrom = skipTo
			}
		}
		parquet.Release(page)
		selection = selection.Skip(skipFrom, skipTo)
	}
	return selection, nil
}

func getDictionaryEncodedValue(page parquet.Page, matches matchFunc) int32 {
	dictionaryData := page.Dictionary().Page().Data()
	vals, offsets := dictionaryData.ByteArray()
	for i := 0; i < len(offsets)-1; i++ {
		val := parquet.ByteArrayValue(vals[offsets[i]:offsets[i+1]])
		if matches(val) {
			return int32(i)
		}
	}
	return -1
}
