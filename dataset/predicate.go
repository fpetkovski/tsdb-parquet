package dataset

import (
	"github.com/segmentio/parquet-go"
)

type RowSelector struct {
	column parquet.LeafColumn
	value  parquet.Value
}

func newRowSelector(column parquet.LeafColumn, value string) RowSelector {
	return RowSelector{
		column: column,
		value:  parquet.ByteArrayValue([]byte(value)),
	}
}

func (p RowSelector) selectRows(rowGroup parquet.RowGroup) predicateResult {
	var selection predicateResult

	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	if !p.matchesBloom(chunk.BloomFilter()) {
		return predicateResult{skipRows(0, rowGroup.NumRows())}
	}

	columnIndex := chunk.ColumnIndex()
	offsetIndex := chunk.OffsetIndex()
	for i := 0; i < columnIndex.NumPages(); i++ {
		fromRow := offsetIndex.FirstRowIndex(i)
		toRow := rowGroup.NumRows()
		if i < columnIndex.NumPages()-1 {
			toRow = offsetIndex.FirstRowIndex(i + 1)
		}

		if !p.matchesStatistics(columnIndex.MinValue(i), columnIndex.MaxValue(i)) {
			selection = append(selection, skipRows(fromRow, toRow))
		}
	}

	return selection
}
func (p RowSelector) matchesBloom(bloom parquet.BloomFilter) bool {
	if bloom == nil {
		return true
	}
	match, err := bloom.Check(p.value)
	return err == nil && match
}

func (p RowSelector) matchesStatistics(minValue, maxValue parquet.Value) bool {
	compare := p.column.Node.Type().Compare

	if compare(p.value, minValue) < 0 {
		return false
	}
	if compare(p.value, maxValue) > 0 {
		return false
	}
	return true
}

func (p RowSelector) matchDictionary(dictionary parquet.Dictionary) bool {
	if dictionary == nil {
		return true
	}
	return true
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
