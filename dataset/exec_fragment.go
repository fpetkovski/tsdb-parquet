package dataset

import (
	"io"

	"github.com/segmentio/parquet-go"
)

type ExecFragment interface {
	io.Closer
	NextBatch() ([][]parquet.Value, error)
	Release([][]parquet.Value)
}
