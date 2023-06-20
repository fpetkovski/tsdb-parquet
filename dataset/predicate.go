package dataset

import (
	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type Predicate interface {
	SelectRows(rowGroup parquet.RowGroup) RowSelection
	FilterRows(rowGroup parquet.RowGroup, selection RowSelection) (RowSelection, error)
}

type Predicates []columnPredicate

func (ps Predicates) Len() int { return len(ps) }

func (ps Predicates) Swap(i, j int) { ps[i], ps[j] = ps[j], ps[i] }

func (ps Predicates) Less(i, j int) bool {
	return db.CompareColumns(ps[i].column.Path[0], ps[j].column.Path[0])
}

func (ps Predicates) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	var selection RowSelection
	for _, p := range ps {
		selection = append(selection, p.SelectRows(rowGroup)...)
	}
	return selection
}

func (ps Predicates) FilterRows(rowGroup parquet.RowGroup, rowSelection RowSelection) (RowSelection, error) {
	for _, p := range ps {
		filteredRows, err := p.FilterRows(rowGroup, rowSelection)
		if err != nil {
			return nil, err
		}
		rowSelection = append(rowSelection, filteredRows...)
	}

	return rowSelection, nil
}

type columnPredicate struct {
	column parquet.LeafColumn
	value  parquet.Value

	selectors RowSelectors
	filter    RowFilter
}

func (p columnPredicate) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.selectors.SelectRows(chunk)
}

func (p columnPredicate) FilterRows(rowGroup parquet.RowGroup, selection RowSelection) (RowSelection, error) {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.filter.FilterRows(chunk, SelectRows(rowGroup, selection))
}

func newEqualsMatcher(reader db.SectionLoader, column parquet.LeafColumn, value string) columnPredicate {
	pqValue := parquet.ByteArrayValue([]byte(value))
	compare := column.Node.Type().Compare

	return columnPredicate{
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

func newGTEMatcher(reader db.SectionLoader, column parquet.LeafColumn, threshold parquet.Value) columnPredicate {
	compare := column.Node.Type().Compare
	return columnPredicate{
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

func newLTEMatcher(reader db.SectionLoader, column parquet.LeafColumn, value parquet.Value) columnPredicate {
	compare := column.Node.Type().Compare
	return columnPredicate{
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
