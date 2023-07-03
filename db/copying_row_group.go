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
	rowCopies := make([]parquet.Row, len(rows))
	n, err := b.Rows.ReadRows(rowCopies)
	if err != nil {
		return n, err
	}
	for i, rowCopy := range rowCopies[:n] {
		rows[i] = rowCopy.Clone()
	}
	return n, err
}
