package schema

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"

	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/segmentio/parquet-go"
)

const (
	timestampTag = `parquet:",delta,snappy"`
	valueTag     = `parquet:",split,snappy"`
)

var int64Val int64 = 0
var float64Val float64 = 0

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
				Type: schemapb.StorageLayout_TYPE_INT64,
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

func For(labels map[string]struct{}) (*parquet.Schema, []reflect.StructField) {
	structFields := []reflect.StructField{
		{
			Name: "OBSERVED_TIMESTAMP",
			Type: reflect.TypeOf(int64Val),
			Tag:  reflect.StructTag(timestampTag),
		},
		{
			Name: "OBSERVED_VALUE",
			Type: reflect.TypeOf(float64Val),
			Tag:  reflect.StructTag(valueTag),
		},
	}
	for field := range labels {
		if !unicode.IsLetter(rune(field[0])) {
			continue
		}
		tag := fmt.Sprintf(`parquet:"%v,optional,dict,snappy"`, field)
		structFields = append(structFields, reflect.StructField{
			Name: strings.ToUpper(field),
			Type: reflect.TypeOf(field),
			Tag:  reflect.StructTag(tag),
		})
	}

	structType := reflect.StructOf(structFields)
	structElem := reflect.New(structType)

	return parquet.SchemaOf(structElem.Interface()), structFields
}
