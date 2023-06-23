package dataset

import "github.com/segmentio/parquet-go"

type SelectionResult struct {
	rowGroup parquet.RowGroup
	ranges   []pickRange
}

func (s SelectionResult) NumRows() int64 {
	var numRows int64
	for _, r := range s.ranges {
		numRows += r.length()
	}
	return numRows
}

type RowsIterator struct {
	i      int
	result SelectionResult
}

func NewRowRangeIterator(result SelectionResult) *RowsIterator {
	return &RowsIterator{
		i:      -1,
		result: result,
	}
}

func (r *RowsIterator) Next() bool {
	r.i++
	return r.i < len(r.result.ranges)
}

func (r *RowsIterator) At() (int64, int64) {
	return r.result.ranges[r.i].from, r.result.ranges[r.i].to
}
