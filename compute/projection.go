package compute

import (
	"io"
	"sync"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/dataset"
	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/generic"
)

type Projections struct {
	pool      *valuesPool
	columns   []*columnProjection
	batchSize int64
}

func ProjectColumns(selection dataset.SelectionResult, reader db.SectionLoader, batchSize int64, columnNames ...string) Projections {
	pool := newValuesPool(int(batchSize))
	projections := make([]*columnProjection, 0, len(columnNames))
	for _, columnName := range columnNames {
		column, ok := selection.RowGroup().Schema().Lookup(columnName)
		if !ok {
			continue
		}
		colProjection, err := newColumnProjection(column, selection, reader, batchSize, pool)
		if err != nil {
			panic(err)
		}
		projections = append(projections, colProjection)
	}

	return Projections{
		pool:      pool,
		columns:   projections,
		batchSize: batchSize,
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

func (p Projections) BatchSize() int64 {
	return p.batchSize
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
	pages  dataset.RowIndexedPages
	loader db.SectionLoader

	batchSize     int64
	currentPage   parquet.Page
	currentReader parquet.ValueReader

	section db.Section
}

func newColumnProjection(
	column parquet.LeafColumn,
	selection dataset.SelectionResult,
	loader db.SectionLoader,
	batchSize int64,
	pool *valuesPool,
) (*columnProjection, error) {
	chunk := selection.RowGroup().ColumnChunks()[column.ColumnIndex]
	pages := dataset.SelectPages(chunk, selection)
	section, err := loader.NewSection(pages.PageOffset(0), pages.PageOffset(pages.NumPages()-1))
	if err != nil {
		return nil, err
	}

	return &columnProjection{
		batchSize: batchSize,
		pages:     pages,
		pool:      pool,
		loader:    loader,
		section:   db.AsyncSection(section, 3),
		currentReader: parquet.ValueReaderFunc(func(values []parquet.Value) (int, error) {
			return 0, io.EOF
		}),
	}, nil
}

func (p *columnProjection) nextBatch() ([]parquet.Value, error) {
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
			p.currentPage, err = p.pages.ReadPage()
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

func (p *columnProjection) loadPages() error {
	var err error
	p.once.Do(func() {
		err = p.section.LoadAll()
	})
	return err
}

func (p *columnProjection) Close() error {
	if p.currentPage != nil {
		parquet.Release(p.currentPage)
	}
	if p.section != nil {
		p.section.Close()
	}
	return p.pages.Close()
}
