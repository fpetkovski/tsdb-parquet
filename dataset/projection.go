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
	pages := SelectPages(chunk.OffsetIndex(), chunk.NumValues(), chunk.Pages(), selection)
	defer pages.Close()

	offsetFrom, offsetTo := pages.OffsetRange()
	if err := p.reader.LoadSection(offsetFrom, offsetTo); err != nil {
		return nil, err
	}

	values := make([]parquet.Value, 0, selection.NumRows())
	for {
		page, _, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		pageValues := make([]parquet.Value, page.NumValues())
		n, err := page.Values().ReadValues(pageValues)
		if err != nil && err != io.EOF {
			return nil, err
		}
		values = append(values, pageValues[:n]...)
	}

	return values, nil
}
