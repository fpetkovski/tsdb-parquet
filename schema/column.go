package schema

import (
	"reflect"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress/zstd"
	"github.com/segmentio/parquet-go/deprecated"
	"github.com/segmentio/parquet-go/format"
)

type column struct {
	parquet.Node
	name string
}

func newColumn(name string, node parquet.Node) *column {
	return &column{Node: node, name: name}
}

func newInt64Column(name string) *column {
	node := parquet.Leaf(parquet.Int64Type)
	node = parquet.Encoded(node, &parquet.DeltaBinaryPacked)
	node = parquet.Compressed(node, &zstd.Codec{})
	return newColumn(name, node)
}

func newStringColumn(name string) *column {
	node := parquet.Leaf(parquet.ByteArrayType)
	//node = parquet.Optional(node)
	node = parquet.Encoded(node, &parquet.RLEDictionary)
	return newColumn(name, node)
}

func newByteArrayColumn(name string) *column {
	node := parquet.Leaf(parquet.ByteArrayType)
	node = parquet.Encoded(node, &parquet.DeltaLengthByteArray)
	node = parquet.Compressed(node, &zstd.Codec{})
	return newColumn(name, node)
}

func (l column) Name() string { return l.name }

func (l column) Value(base reflect.Value) reflect.Value { return base }

type groupType struct {
	parquet.Type
}

func (groupType) String() string { return "group" }

func (groupType) Length() int { return 0 }

func (groupType) EstimateSize(int) int { return 0 }

func (groupType) EstimateNumValues(int) int { return 0 }

func (groupType) ColumnOrder() *format.ColumnOrder { return nil }

func (groupType) PhysicalType() *format.Type { return nil }

func (groupType) LogicalType() *format.LogicalType { return nil }

func (groupType) ConvertedType() *deprecated.ConvertedType { return nil }
