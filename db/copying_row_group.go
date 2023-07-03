package db

import (
	"github.com/segmentio/parquet-go"
)

type copyingRowGroup struct {
	parquet.RowGroup
}

func newCopyingRowGroup(rowGroup parquet.RowGroup) *copyingRowGroup {
	return &copyingRowGroup{RowGroup: rowGroup}
}

func (b copyingRowGroup) Rows() parquet.Rows {
	return newCopyingRows(b.RowGroup.Rows())
}

type copyingRows struct {
	parquet.Rows
}

func newCopyingRows(rows parquet.Rows) *copyingRows {
	return &copyingRows{Rows: rows}
}

func (b copyingRows) ReadRows(rows []parquet.Row) (int, error) {
	rowClones := make([]parquet.Row, len(rows))
	n, err := b.Rows.ReadRows(rowClones)
	if err != nil {
		return n, err
	}
	for i, row := range rowClones[:n] {
		rows[i] = row.Clone()
	}
	return n, err
}
