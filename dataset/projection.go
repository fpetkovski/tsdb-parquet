package dataset

import (
	"io"
	"sync"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type Projections struct {
	selection SelectionResult
	columns   []parquet.LeafColumn
	reader    db.SectionLoader
}

func Projection(selection SelectionResult, reader db.SectionLoader, columnNames ...string) Projections {
	columns := make([]parquet.LeafColumn, 0, len(columnNames))
	for _, columnName := range columnNames {
		column, ok := selection.rowGroup.Schema().Lookup(columnName)
		if ok {
			columns = append(columns, column)
		}
	}

	return Projections{
		selection: selection,
		reader:    reader,
		columns:   columns,
	}
}

func (p *Projections) ReadColumnRanges() ([][]parquet.Value, error) {
	columns := make([][]parquet.Value, len(p.columns))
	var (
		wg      sync.WaitGroup
		errChan = make(chan error, len(p.columns))
	)
	wg.Add(len(p.columns))
	for i, column := range p.columns {
		go func(i int, columnIndex int) {
			defer wg.Done()

			chunk := p.selection.rowGroup.ColumnChunks()[columnIndex]
			values, err := p.readColumn(chunk, p.selection)
			if err != nil {
				errChan <- err
			}
			columns[i] = values
		}(i, column.ColumnIndex)
	}
	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return columns, nil
}

func (p *Projections) readColumn(chunk parquet.ColumnChunk, selection SelectionResult) ([]parquet.Value, error) {
	pages := SelectPages(chunk, selection)
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
		parquet.Release(page)
	}

	return values, nil
}
