package dataset

import (
	"io"
	"sync"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/generic"
)

type Projections struct {
	pool    *valuesPool
	columns []*columnProjection
}

func ProjectColumns(selection SelectionResult, reader db.SectionLoader, batchSize int64, columnNames ...string) Projections {
	pool := newValuesPool(int(batchSize))
	projections := make([]*columnProjection, 0, len(columnNames))
	for _, columnName := range columnNames {
		column, ok := selection.rowGroup.Schema().Lookup(columnName)
		if !ok {
			continue
		}
		projections = append(projections, newColumnProjection(column, selection, reader, batchSize, pool))
	}

	return Projections{
		pool:    pool,
		columns: projections,
	}
}

func (p Projections) NextBatch() ([][]parquet.Value, error) {
	batch := make([][]parquet.Value, len(p.columns))
	err := generic.ParallelEach(p.columns, func(i int, column *columnProjection) error {
		var err error
		batch[i], err = column.nextBatch()
		return err
	})

	return batch, err
}

func (p Projections) Release(batch [][]parquet.Value) {
	for _, column := range batch {
		p.pool.put(column)
	}
}

func (p Projections) Close() error {
	var lastErr error
	for _, column := range p.columns {
		if colErr := column.Close(); colErr != nil {
			lastErr = colErr
		}
	}
	return lastErr
}

type columnProjection struct {
	once   sync.Once
	pool   *valuesPool
	pages  RowIndexedPages
	loader db.SectionLoader

	batchSize     int64
	currentPage   parquet.Page
	currentReader parquet.ValueReader
}

func newColumnProjection(
	column parquet.LeafColumn,
	selection SelectionResult,
	loader db.SectionLoader,
	batchSize int64,
	pool *valuesPool,
) *columnProjection {
	chunk := selection.rowGroup.ColumnChunks()[column.ColumnIndex]
	pages := SelectPages(chunk, selection)

	return &columnProjection{
		batchSize: batchSize,
		pages:     pages,
		pool:      pool,
		loader:    loader,
		currentReader: parquet.ValueReaderFunc(func(values []parquet.Value) (int, error) {
			return 0, io.EOF
		}),
	}
}

func (p *columnProjection) nextBatch() ([]parquet.Value, error) {
	if err := p.loadColumnData(); err != nil {
		return nil, err
	}
	var (
		numRead int64
		err     error
		values  = p.pool.get()
	)
	for numRead < p.batchSize {
		n, readValsErr := p.currentReader.ReadValues(values[numRead:])
		numRead += int64(n)

		// If the current page is exhausted, move over to the next page.
		if readValsErr == io.EOF {
			parquet.Release(p.currentPage)
			p.currentPage, _, err = p.pages.ReadPage()
			if err != nil {
				break
			}
			p.currentReader = p.currentPage.Values()
		}
	}
	// If we've exhausted all pages, and we haven't read any values, return EOF.
	if err == io.EOF && numRead == 0 {
		return nil, io.EOF
	}
	// Return errors that are not EOF.
	if err != nil && err != io.EOF {
		return nil, err
	}

	return values[:numRead], nil
}

func (p *columnProjection) loadColumnData() error {
	var err error
	p.once.Do(func() {
		offsetFrom, offsetTo := p.pages.OffsetRange()
		err = p.loader.LoadSection(offsetFrom, offsetTo)
	})
	return err
}

func (p *columnProjection) Close() error {
	if p.currentPage != nil {
		parquet.Release(p.currentPage)
	}
	return p.pages.Close()
}
