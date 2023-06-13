package dataset

import "github.com/segmentio/parquet-go"

type predicate struct {
	columnIndex int
	column      parquet.Column
	value       parquet.Value
}

func newPredicate(column parquet.Column, value string) predicate {
	return predicate{
		column: column,
		value:  parquet.ByteArrayValue([]byte(value)),
	}
}

func (p predicate) matchRowGroup(rowGroup parquet.RowGroup) rowSelection {
	var selection rowSelection

	chunk := rowGroup.ColumnChunks()[p.columnIndex]
	if !p.matchBloom(chunk.BloomFilter()) {
		return rowSelection{skipRows(0, rowGroup.NumRows())}
	}

	columnIndex := chunk.ColumnIndex()
	offsetIndex := chunk.OffsetIndex()
	for i := 0; i < columnIndex.NumPages(); i++ {
		fromRow := offsetIndex.FirstRowIndex(i)
		toRow := rowGroup.NumRows()
		if i < columnIndex.NumPages()-1 {
			toRow = offsetIndex.FirstRowIndex(i + 1)
		}

		if p.matchStatistics(columnIndex.MinValue(i), columnIndex.MaxValue(i)) {
			selection = append(selection, pickRange{
				from: fromRow,
				to:   toRow,
			})
		}
	}

	return selection
}
func (p predicate) matchBloom(bloom parquet.BloomFilter) bool {
	if bloom == nil {
		return true
	}
	match, err := bloom.Check(p.value)
	return err == nil && match
}

func (p predicate) matchStatistics(minValue parquet.Value, maxValue parquet.Value) bool {
	compare := p.column.Type().Compare
	if compare(p.value, minValue) < 0 {
		return false
	}
	if compare(p.value, maxValue) > 0 {
		return false
	}
	return true
}

func (p predicate) matchDictionary(dictionary parquet.Dictionary) bool {
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
