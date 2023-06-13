package dataset

import (
	"github.com/segmentio/parquet-go"
)

type ColumnSelection struct {
	columnNames   []string
	columnIndices []int
}

func NewColumnSelection(metadata *parquet.Schema, columns ...string) ColumnSelection {
	columnIndices := make([]int, 0, len(columns))
	columnNames := make([]string, 0, len(columns))
	for i, column := range metadata.Columns() {
		columnIndices = append(columnIndices, i)
		columnNames = append(columnNames, column[0])
	}

	return ColumnSelection{
		columnNames:   columnNames,
		columnIndices: columnIndices,
	}
}

func (cs ColumnSelection) SelectColumns(rowGroup parquet.RowGroup) []parquet.ColumnChunk {
	columnChunks := make([]parquet.ColumnChunk, 0, len(cs.columnIndices))
	for _, columnIndex := range cs.columnIndices {
		columnChunks = append(columnChunks, rowGroup.ColumnChunks()[columnIndex])
	}
	return columnChunks
}
