package dataset

import (
	"github.com/segmentio/parquet-go"
)

type Projection struct {
	columnNames   []string
}

func NewColumnSelection(columns ...string) Projection {
	return Projection{
		columnNames:   columns,
	}
}

func (cs Projection) SelectColumns(rowGroup parquet.RowGroup) []parquet.ColumnChunk {
	columnChunks := make([]parquet.ColumnChunk, 0, len(cs.columnNames))
	for _, columnName := range cs.columnNames {
		column, ok := rowGroup.Schema().Lookup(columnName)
		if ok {
			columnChunks = append(columnChunks, rowGroup.ColumnChunks()[column.ColumnIndex])
		}
	}
	return columnChunks
}
