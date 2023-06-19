package dataset

import (
	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type Predicate interface {
	SelectRows(rowGroup parquet.RowGroup) RowSelection
	FilterRows(rowGroup parquet.RowGroup, selection SelectionResult) (RowSelection, error)
}

type Predicates []Predicate

func (ps Predicates) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	var selection RowSelection
	for _, p := range ps {
		selection = append(selection, p.SelectRows(rowGroup)...)
	}
	return selection
}

func (ps Predicates) FilterRows(rowGroup parquet.RowGroup, ranges SelectionResult) (RowSelection, error) {
	var result RowSelection
	for _, p := range ps {
		filteredRows, err := p.FilterRows(rowGroup, ranges)
		if err != nil {
			return nil, err
		}
		result = append(result, filteredRows...)
	}

	return result, nil
}

type Matcher struct {
	column parquet.LeafColumn
	value  parquet.Value

	selectors RowSelectors
	filter    RowFilter
}

func (p Matcher) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.selectors.SelectRows(chunk)
}

func (p Matcher) FilterRows(rowGroup parquet.RowGroup, selection SelectionResult) (RowSelection, error) {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.filter.FilterRows(chunk, selection)
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
		filter: NewDictionaryFilter(reader, func(value parquet.Value) bool {
			return compare(value, pqValue) == 0
		}),
	}
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
		filter: NewDecodingFilter(reader, func(rowValue parquet.Value) bool {
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
		filter: NewDecodingFilter(reader, func(rowValue parquet.Value) bool {
			return compare(rowValue, value) <= 0
		}),
	}
}
