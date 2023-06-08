package schema

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/compress"
	"github.com/segmentio/parquet-go/encoding"
)

const (
	TimestampColumn = "timestamp"
	ValueColumn     = "value"
)

type FloatSample struct {
	SeriesID int64

	Labels labels.Labels
	// T is the timestamp of the sample in milliseconds.
	T int64
	// V is the value of the sample.
	V float64
}

func NewFloatSample(labels labels.Labels, t int64, v float64) *FloatSample {
	return &FloatSample{
		Labels: labels,
		T:      t,
		V:      v,
	}
}

type floatSampleRow struct {
	labels chunkLabels
}

func newFloatSampleRow(labels chunkLabels) *floatSampleRow {
	return &floatSampleRow{labels: labels}
}

func (c floatSampleRow) String() string {
	return fmt.Sprintf("%v", c.labels)
}

func (c floatSampleRow) Type() parquet.Type { return groupType{} }

func (c floatSampleRow) Optional() bool { return false }

func (c floatSampleRow) Repeated() bool { return false }

func (c floatSampleRow) Required() bool { return true }

func (c floatSampleRow) Leaf() bool { return false }

func (c floatSampleRow) Fields() []parquet.Field {
	fields := make([]parquet.Field, 3, 3+len(c.labels))
	fields[0] = newInt64Column(SeriesIDColumn)
	fields[1] = newInt64Column(TimestampColumn)
	fields[2] = newFloat64Column(ValueColumn)

	for _, lbl := range c.labels {
		fields = append(fields, newStringColumn(lbl))
	}
	return fields
}

func (c floatSampleRow) Encoding() encoding.Encoding { return nil }

func (c floatSampleRow) Compression() compress.Codec { return nil }

func (c floatSampleRow) GoType() reflect.Type { return reflect.TypeOf(chunkRow{}) }

type FloatSchema struct {
	schema *parquet.Schema
	labels []string
}

func MakeFloatSchema(lbls []string) *FloatSchema {
	sort.Strings(lbls)

	schema := parquet.NewSchema("chunk", newFloatSampleRow(lbls))
	return &FloatSchema{
		schema: schema,
		labels: lbls,
	}
}

func (c *FloatSchema) ParquetSchema() *parquet.Schema {
	return c.schema
}

func (c *FloatSchema) MakeFloatSampleRow(sample FloatSample) parquet.Row {
	row := make(parquet.Row, 3, len(c.labels)+3)

	row[0] = parquet.Int64Value(sample.SeriesID).Level(0, 0, 0)
	row[1] = parquet.Int64Value(sample.T).Level(0, 0, 1)
	row[2] = parquet.DoubleValue(sample.V).Level(0, 0, 2)

	for labelIndex, labelName := range c.labels {
		labelVal := sample.Labels.Get(labelName)
		val := parquet.ByteArrayValue([]byte(labelVal)).Level(0, 0, 3+labelIndex)
		row = append(row, val)
	}

	return row
}
