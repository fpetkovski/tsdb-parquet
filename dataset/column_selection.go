package dataset

import (
	"github.com/segmentio/parquet-go"
)

type ColumnSelector struct {
	columnNames   []string
}

func NewColumnSelection(columns ...string) ColumnSelector {
	return ColumnSelector{
		columnNames:   columns,
	}
}

func (cs ColumnSelector) SelectColumns(rowGroup parquet.RowGroup) []parquet.ColumnChunk {
	columnChunks := make([]parquet.ColumnChunk, 0, len(cs.columnNames))
	for _, columnName := range cs.columnNames {
		column, ok := rowGroup.Schema().Lookup(columnName)
		if ok {
			columnChunks = append(columnChunks, rowGroup.ColumnChunks()[column.ColumnIndex])
		}
	}
	return columnChunks
}
