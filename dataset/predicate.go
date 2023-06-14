package dataset

import (
	"fmt"

	"github.com/segmentio/parquet-go"
)

type RowSelector interface {
	String() string
	Column() parquet.LeafColumn
	Matches(parquet.Value) bool
	SelectRows(parquet.RowGroup) RowSelection
}

type EqualsMatcher struct {
	column parquet.LeafColumn
	value  parquet.Value
}

func newEqualsMatcher(column parquet.LeafColumn, value string) EqualsMatcher {
	return EqualsMatcher{
		column: column,
		value:  parquet.ByteArrayValue([]byte(value)),
	}
}

func (p EqualsMatcher) Column() parquet.LeafColumn {
	return p.column
}

func (p EqualsMatcher) Matches(value parquet.Value) bool {
	return p.column.Node.Type().Compare(p.value, value) == 0
}

func (p EqualsMatcher) String() string {
	return fmt.Sprintf("%s = %s", p.column.Node.String(), p.value)
}

func (p EqualsMatcher) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	var selection RowSelection

	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	if !p.matchesBloom(chunk.BloomFilter()) {
		return RowSelection{skip(0, rowGroup.NumRows())}
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
			selection = append(selection, skip(fromRow, toRow))
		}
	}

	return selection
}

func (p EqualsMatcher) matchesBloom(bloom parquet.BloomFilter) bool {
	if bloom == nil {
		return true
	}
	match, err := bloom.Check(p.value)
	return err == nil && match
}

func (p EqualsMatcher) matchesStatistics(minValue, maxValue parquet.Value) bool {
	compare := p.column.Node.Type().Compare

	if compare(p.value, minValue) < 0 {
		return false
	}
	if compare(p.value, maxValue) > 0 {
		return false
	}
	return true
}

func (p EqualsMatcher) matchDictionary(dictionary parquet.Dictionary) bool {
	if dictionary == nil {
		return true
	}
	return true
}

type GTEMatcher struct {
	column parquet.LeafColumn
	value  parquet.Value
}

func (p GTEMatcher) String() string {
	return fmt.Sprintf("%s >= %s", p.column.Node.String(), p.value)
}

func NewGTEMatcher(column parquet.LeafColumn, value parquet.Value) *GTEMatcher {
	return &GTEMatcher{
		column: column,
		value:  value,
	}
}

func (p GTEMatcher) Column() parquet.LeafColumn {
	return p.column
}

func (p GTEMatcher) Matches(value parquet.Value) bool {
	return p.column.Node.Type().Compare(value, p.value) >= 0
}

func (p GTEMatcher) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	columnIndex := chunk.ColumnIndex()
	offsetIndex := chunk.OffsetIndex()
	compare := chunk.Type().Compare

	var selection RowSelection
	for i := 0; i < columnIndex.NumPages(); i++ {
		fromRow := offsetIndex.FirstRowIndex(i)
		toRow := rowGroup.NumRows()
		if i < columnIndex.NumPages()-1 {
			toRow = offsetIndex.FirstRowIndex(i + 1)
		}
		if compare(p.value, columnIndex.MaxValue(i)) > 0 {
			selection = append(selection, skip(fromRow, toRow))
		}
	}

	return selection
}

type LTEMatcher struct {
	column parquet.LeafColumn
	value  parquet.Value
}

func (p LTEMatcher) Column() parquet.LeafColumn {
	return p.column
}

func (p LTEMatcher) Matches(value parquet.Value) bool {
	return p.column.Node.Type().Compare(value, p.value) <= 0
}

func (p LTEMatcher) String() string {
	return fmt.Sprintf("%s <= %s", p.column.Node.String(), p.value)
}

func NewLTEMatcher(column parquet.LeafColumn, value parquet.Value) *LTEMatcher {
	return &LTEMatcher{
		column: column,
		value:  value,
	}
}

func (p LTEMatcher) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	columnIndex := chunk.ColumnIndex()
	offsetIndex := chunk.OffsetIndex()
	compare := chunk.Type().Compare

	var selection RowSelection
	for i := 0; i < columnIndex.NumPages(); i++ {
		fromRow := offsetIndex.FirstRowIndex(i)
		toRow := rowGroup.NumRows()
		if i < columnIndex.NumPages()-1 {
			toRow = offsetIndex.FirstRowIndex(i + 1)
		}
		if compare(p.value, columnIndex.MinValue(i)) < 0 {
			selection = append(selection, skip(fromRow, toRow))
		}
	}

	return selection
}
