package dataset

import (
	"fmt"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type Predicate interface {
	fmt.Stringer

	SelectRows(rowGroup parquet.RowGroup) RowSelection
	FilterRows(rowGroup parquet.RowGroup, selection SelectionResult) (RowSelection, error)
	Column() parquet.LeafColumn
}

type Matcher struct {
	column parquet.LeafColumn
	value  parquet.Value

	selectors RowSelectors
	filter    RowFilter
}

func newEqualsMatcher(reader *db.FileReader, column parquet.LeafColumn, value string) Matcher {
	pqValue := parquet.ByteArrayValue([]byte(value))
	compare := column.Node.Type().Compare

	return Matcher{
		column: column,
		value:  pqValue,

		selectors: []RowSelector{
			newBloomSelector(pqValue),
			newStatsSelector(func(min, max parquet.Value) bool {
				return compare(min, pqValue) <= 0 && compare(max, pqValue) >= 0
			}),
		},
		filter: NewRowFilter(reader, func(value parquet.Value) bool {
			return compare(value, pqValue) == 0
		}),
	}
}

func (p Matcher) Column() parquet.LeafColumn {
	return p.column
}

func (p Matcher) String() string {
	return fmt.Sprintf("%s = %s", p.column.Node.String(), p.value)
}

func (p Matcher) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.selectors.SelectRows(chunk)
}

func (p Matcher) FilterRows(rowGroup parquet.RowGroup, selection SelectionResult) (RowSelection, error) {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.filter.FilterRows(chunk, selection)
}

func newGTEMatcher(reader *db.FileReader, column parquet.LeafColumn, threshold parquet.Value) *Matcher {
	compare := column.Node.Type().Compare
	return &Matcher{
		column: column,
		value:  threshold,

		selectors: []RowSelector{
			newStatsSelector(func(_, max parquet.Value) bool {
				return compare(max, threshold) >= 0
			}),
		},
		filter: NewRowFilter(reader, func(rowValue parquet.Value) bool {
			return compare(rowValue, threshold) >= 0
		}),
	}
}

func newLTEMatcher(reader *db.FileReader, column parquet.LeafColumn, value parquet.Value) *Matcher {
	compare := column.Node.Type().Compare
	return &Matcher{
		column: column,
		value:  value,

		selectors: []RowSelector{
			newStatsSelector(func(min, _ parquet.Value) bool {
				return compare(min, value) <= 0
			}),
		},
		filter: NewRowFilter(reader, func(rowValue parquet.Value) bool {
			return compare(rowValue, value) <= 0
		}),
	}
}
