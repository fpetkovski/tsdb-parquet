package schema

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/segmentio/parquet-go"
)

const (
	timestampTag = `parquet:",delta,snappy"`
	valueTag     = `parquet:",split,snappy"`
	chunkTag     = `parquet:",snappy"`
)

var int64Val int64 = 0
var float64Val float64 = 0
var byteArrayVal []byte = nil

func Prometheus() *schemapb.Schema {
	return &schemapb.Schema{
		Name: "prometheus",
		Columns: []*schemapb.Column{{
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_INT64,
				Encoding: schemapb.StorageLayout_ENCODING_DELTA_BINARY_PACKED,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_DOUBLE,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:      "timestamp",
			Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
		}, {
			Name:       "labels",
			NullsFirst: true,
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
		}},
	}
}

type ChunkSchema struct {
	schema      *parquet.Schema
	labelsIndex map[string]int
}

func (c *ChunkSchema) ParquetSchema() *parquet.Schema {
	return c.schema
}

func (c *ChunkSchema) ColumnIndex(columnName string) int {
	return c.labelsIndex[columnName]
}

func MakeChunkSchema(lbls []string) *ChunkSchema {
	structFields := []reflect.StructField{
		{
			Name: "MinT",
			Type: reflect.TypeOf(int64Val),
			Tag:  reflect.StructTag(timestampTag),
		},
		{
			Name: "MaxT",
			Type: reflect.TypeOf(int64Val),
			Tag:  reflect.StructTag(timestampTag),
		},
		{
			Name: "ChunkBytes",
			Type: reflect.TypeOf(byteArrayVal),
			Tag:  reflect.StructTag(chunkTag),
		},
	}

	labelsIndex := make(map[string]int, 0)
	sort.Strings(lbls)
	for _, field := range lbls {
		if !unicode.IsLetter(rune(field[0])) {
			continue
		}
		tag := fmt.Sprintf(`parquet:"%v,dict,snappy"`, field)
		structFields = append(structFields, reflect.StructField{
			Name: strings.ToUpper(field),
			Type: reflect.TypeOf(field),
			Tag:  reflect.StructTag(tag),
		})
		labelsIndex[field] = len(labelsIndex)
	}
	structType := reflect.StructOf(structFields)
	structElem := reflect.New(structType)

	return &ChunkSchema{
		schema:      parquet.SchemaOf(structElem.Interface()),
		labelsIndex: labelsIndex,
	}
}
