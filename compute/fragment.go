package compute

import (
	"io"

	"github.com/segmentio/parquet-go"
)

type Batch [][]parquet.Value

type Fragment interface {
	io.Closer
	NextBatch() (Batch, error)
	MaxBatchSize() int64
	Release(Batch)
}
