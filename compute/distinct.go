package compute

import (
	"github.com/segmentio/parquet-go"
)

type Unique struct {
	projection     Projections
	distinctColumn int

	seenValues map[parquet.Value]struct{}
	batchRows  []int
	pool       *valuesPool
}

func UniqueByColumn(byColumnIndex int, projections Projections) *Unique {
	return &Unique{
		projection:     projections,
		distinctColumn: byColumnIndex,

		pool:       newValuesPool(int(projections.BatchSize())),
		batchRows:  make([]int, int(projections.BatchSize())),
		seenValues: make(map[parquet.Value]struct{}),
	}
}

func (d *Unique) NextBatch() (Batch, error) {
	inputBatch, err := d.projection.NextBatch()
	if err != nil {
		return nil, err
	}
	defer d.projection.Release(inputBatch)

	outputBatch := make([][]parquet.Value, len(d.projection.columns))
	for i := range inputBatch {
		outputBatch[i] = d.pool.get()[:0]
	}

	d.batchRows = d.batchRows[:0]
	for i, distinctValue := range inputBatch[d.distinctColumn] {
		if _, ok := d.seenValues[distinctValue]; ok {
			continue
		}
		d.seenValues[distinctValue] = struct{}{}
		d.batchRows = append(d.batchRows, i)
	}

	for colIdx := range inputBatch {
		for _, row := range d.batchRows {
			outputBatch[colIdx] = append(outputBatch[colIdx], inputBatch[colIdx][row])
		}
	}

	return outputBatch, nil
}

func (d *Unique) Release(batch Batch) {
	for _, column := range batch {
		d.pool.put(column)
	}
}

func (d *Unique) Close() error {
	return d.projection.Close()
}
