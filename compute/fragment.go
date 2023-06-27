package compute

import (
	"io"

	"github.com/segmentio/parquet-go"
)

type Fragment interface {
	io.Closer
	NextBatch() ([][]parquet.Value, error)
	Release([][]parquet.Value)
}
