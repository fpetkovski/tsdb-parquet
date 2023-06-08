package dataset

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/segmentio/parquet-go"
)

type Queryable struct {
	reader        *parquet.File
	columnMapping map[string]int
	arrowReader   *pqarrow.FileReader
}

func NewParquetStorage(reader *parquet.File, arrowReader *pqarrow.FileReader) *Queryable {
	metadata := reader.Schema()
	columnMapping := make(map[string]int, len(metadata.Columns()))
	for i, column := range metadata.Columns() {
		columnMapping[column[0]] = i
	}

	return &Queryable{
		reader:        reader,
		arrowReader:   arrowReader,
		columnMapping: columnMapping,
	}
}

func (q Queryable) Querier(ctx context.Context, mint, maxt int64) *querier {
	return &querier{
		reader:      q.reader,
		arrowReader: q.arrowReader,
		columnIndex: q.columnMapping,
		mint:        mint,
		maxt:        maxt,
	}
}

type querier struct {
	columnIndex map[string]int
	reader      *parquet.File
	arrowReader *pqarrow.FileReader
	maxt        int64
	mint        int64
}

func (q querier) Select(labels []string, matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	labels = append(labels)
	columnsToRead := make([]int, 0, len(labels))
	for _, label := range labels {
		columnsToRead = append(columnsToRead, q.columnIndex[label])
	}
	filter := newRowFilter(q.columnIndex, q.mint, q.maxt, matchers)

	pagesByRow := make([]map[int64]int64, len(columnsToRead))
	for i := range columnsToRead {
		pagesByRow[i] = make(map[int64]int64)
	}

	var rowOffset int
	for i := 0; i < len(q.reader.RowGroups()); i++ {
		for j, colIdx := range columnsToRead {
			indexes := q.reader.OffsetIndexes()[rowOffset+colIdx]
			for _, pageLocation := range indexes.PageLocations {
				pagesByRow[j][pageLocation.FirstRowIndex] = pageLocation.Offset
			}
		}
		rowOffset += int(q.reader.RowGroups()[i].NumRows())
	}

	pageLocations := make([][]int64, len(columnsToRead))
	for i := range columnsToRead {
		pageLocations[i] = make([]int64, 0)
	}

	var rowCursor int64
	for _, group := range q.reader.RowGroups() {
		matches, err := filter.Matches(group)
		if err != nil {
			return nil, err
		}
		if matches {
			for i := range columnsToRead {
				pageLocation := pagesByRow[i][rowCursor]
				pageLocations[i] = append(pageLocations[i], pageLocation)
			}
		}
		rowCursor += group.NumRows()
	}

	fmt.Println(pagesByRow[0])
	fmt.Println(pageLocations[0])

	return nil, nil
}
