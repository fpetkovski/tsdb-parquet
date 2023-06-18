package dataset

import (
	"io"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type Projections struct {
	scanner *Scanner
	columns []parquet.LeafColumn
	reader  *db.FileReader
}

func newProjection(file *parquet.File, reader *db.FileReader, columnNames ...string) Projections {
	columns := make([]parquet.LeafColumn, 0, len(columnNames))
	for _, columnName := range columnNames {
		column, ok := file.Schema().Lookup(columnName)
		if ok {
			columns = append(columns, column)
		}
	}

	return Projections{
		reader:  reader,
		columns: columns,
	}
}

func (p *Projections) ReadColumnRanges(rowGroup parquet.RowGroup, selection SelectionResult) ([][]parquet.Value, error) {
	columns := make([][]parquet.Value, 0, len(p.columns))
	for _, column := range p.columns {
		chunk := rowGroup.ColumnChunks()[column.ColumnIndex]
		values, err := p.readColumn(chunk, selection)
		if err != nil {
			return nil, err
		}
		columns = append(columns, values)
	}

	return columns, nil
}

func (p *Projections) readColumn(chunk parquet.ColumnChunk, selection SelectionResult) ([]parquet.Value, error) {
	pagesRange, _ := selectPagesRange(chunk, selection)
	if err := p.reader.LoadSection(pagesRange.from, pagesRange.to); err != nil {
		return nil, err
	}

	values := make([]parquet.Value, 0, selection.NumRows())
	pages := chunk.Pages()
	defer pages.Close()
	for _, rows := range selection {
		cursor := rows.from
		if err := pages.SeekToRow(cursor); err != nil {
			return nil, err
		}
		for cursor < rows.to {
			page, err := pages.ReadPage()
			if err != nil {
				return nil, err
			}

			numValues := rows.to - cursor
			if numValues > page.NumValues() {
				numValues = page.NumValues()
			}
			pageValues := make([]parquet.Value, numValues)
			n, err := page.Values().ReadValues(pageValues)
			if err != nil && err != io.EOF {
				return nil, err
			}
			values = append(values, pageValues[:n]...)
			cursor += int64(n)
		}
	}
	return values, nil
}
