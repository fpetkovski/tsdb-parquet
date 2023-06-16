package dataset

import (
	"fmt"

	"github.com/segmentio/parquet-go"
)

type Predicate interface {
	String() string
	Column() parquet.LeafColumn
	Matches(parquet.Value) bool
	SelectRows(parquet.RowGroup) RowSelection
}

type Matcher struct {
	column parquet.LeafColumn
	value  parquet.Value

	selectors PageSelectors
}

func (p Matcher) Column() parquet.LeafColumn {
	return p.column
}

func (p Matcher) Matches(value parquet.Value) bool {
	return p.column.Node.Type().Compare(p.value, value) == 0
}

func (p Matcher) String() string {
	return fmt.Sprintf("%s = %s", p.column.Node.String(), p.value)
}

func newEqualsMatcher(column parquet.LeafColumn, value string) Matcher {
	pqValue := parquet.ByteArrayValue([]byte(value))
	compare := column.Node.Type().Compare

	return Matcher{
		column: column,
		value:  pqValue,

		selectors: []PageSelector{
			newBloomRowSelector(pqValue),
			newStatsRowSelector(func(min, max parquet.Value) bool {
				return compare(min, pqValue) <= 0 && compare(max, pqValue) >= 0
			}),
		},
	}
}

func (p Matcher) SelectRows(rowGroup parquet.RowGroup) RowSelection {
	chunk := rowGroup.ColumnChunks()[p.column.ColumnIndex]
	return p.selectors.SelectRows(chunk)
}

func newGTEMatcher(column parquet.LeafColumn, value parquet.Value) *Matcher {
	compare := column.Node.Type().Compare
	return &Matcher{
		column: column,
		value:  value,

		selectors: []PageSelector{
			newStatsRowSelector(func(_, max parquet.Value) bool {
				return compare(max, value) >= 0
			}),
		},
	}
}

func newLTEMatcher(column parquet.LeafColumn, value parquet.Value) *Matcher {
	compare := column.Node.Type().Compare
	return &Matcher{
		column: column,
		value:  value,

		selectors: []PageSelector{
			newStatsRowSelector(func(min, _ parquet.Value) bool {
				return compare(min, value) <= 0
			}),
		},
	}
}
