package schema

import (
	"github.com/apache/arrow/go/v10/arrow"
)

func ArrowType(column string) arrow.DataType {
	switch column {
	case MinTColumn, MaxTColumn, SeriesIDColumn:
		return arrow.PrimitiveTypes.Int64
	case ChunkBytesColumn:
		return arrow.BinaryTypes.Binary
	default:
		return arrow.BinaryTypes.String
	}
}
