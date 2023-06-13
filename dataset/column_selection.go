package dataset

import (
	"github.com/segmentio/parquet-go"
)

type ColumnSelector struct {
	columnNames   []string
	columnIndices []int
}

func NewColumnSelection(metadata *parquet.Schema, columns ...string) ColumnSelector {
	columnIndices := make([]int, 0, len(columns))
	columnNames := make([]string, 0, len(columns))
	for i, column := range metadata.Columns() {
		columnIndices = append(columnIndices, i)
		columnNames = append(columnNames, column[0])
	}

	return ColumnSelector{
		columnNames:   columnNames,
		columnIndices: columnIndices,
	}
}

func (cs ColumnSelector) SelectColumns(rowGroup parquet.RowGroup) []parquet.ColumnChunk {
	columnChunks := make([]parquet.ColumnChunk, 0, len(cs.columnIndices))
	for _, columnIndex := range cs.columnIndices {
		columnChunks = append(columnChunks, rowGroup.ColumnChunks()[columnIndex])
	}
	return columnChunks
}
