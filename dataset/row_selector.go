package dataset

import (
	"github.com/segmentio/parquet-go"
)

type PageSelector interface {
	SelectRows(mode parquet.ColumnChunk) RowSelection
}

type PageSelectors []PageSelector

func (p PageSelectors) SelectRows(chunk parquet.ColumnChunk) RowSelection {
	var selection RowSelection
	for _, selector := range p {
		selection = append(selection, selector.SelectRows(chunk)...)
	}
	return selection
}

type bloomRowSelector struct {
	value parquet.Value
}

func newBloomRowSelector(value parquet.Value) *bloomRowSelector {
	return &bloomRowSelector{value: value}
}

func (s bloomRowSelector) SelectRows(chunk parquet.ColumnChunk) RowSelection {
	var selection RowSelection
	bloomFilter := chunk.BloomFilter()
	if bloomFilter == nil {
		return SelectAll()
	}

	ok, err := bloomFilter.Check(s.value)
	if err != nil || ok {
		return SelectAll()
	}
	return selection.Skip(0, chunk.NumValues())
}

type compareFunc func(min, max parquet.Value) bool

type statsRowSelector struct {
	compare compareFunc
}

func newStatsRowSelector(compare compareFunc) *statsRowSelector {
	return &statsRowSelector{compare: compare}
}

func (s statsRowSelector) SelectRows(chunk parquet.ColumnChunk) RowSelection {
	var selection RowSelection
	offsetIndex := chunk.OffsetIndex()
	columnIndex := chunk.ColumnIndex()
	for i := 0; i < columnIndex.NumPages(); i++ {
		fromRow := offsetIndex.FirstRowIndex(i)
		var toRow int64
		if i < columnIndex.NumPages()-1 {
			toRow = offsetIndex.FirstRowIndex(i + 1)
		} else {
			toRow = chunk.NumValues()
		}

		matches := s.compare(columnIndex.MinValue(i), columnIndex.MaxValue(i))
		if !matches {
			selection = append(selection, skip(fromRow, toRow))
		}
	}

	return selection
}
