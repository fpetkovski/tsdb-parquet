package writer

import (
	"fpetkovski/prometheus-parquet/schema"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/segmentio/parquet-go"
	"golang.org/x/exp/maps"
	"sort"
	"unicode"
)

type Chunk struct {
	Labels labels.Labels

	MinT  int64
	MaxT  int64
	Chunk chunkenc.Chunk
}

func (s Chunk) ToParquetRow(schema *schema.ChunkSchema) parquet.Row {
	row := make(parquet.Row, 3, len(s.Labels)+3)
	row[0] = parquet.Int64Value(s.MinT).Level(0, 0, 0)
	row[1] = parquet.Int64Value(s.MaxT).Level(0, 0, 1)
	row[2] = parquet.ByteArrayValue(s.Chunk.Bytes()).Level(0, 0, 2)

	labelsMap := s.Labels.Map()
	delete(labelsMap, labels.MetricName)

	labelNames := maps.Keys(labelsMap)
	sort.Strings(labelNames)

	for _, labelName := range labelNames {
		if !unicode.IsLetter(rune(labelName[0])) {
			continue
		}
		labelVal := s.Labels.Get(labelName)
		val := parquet.ByteArrayValue([]byte(labelVal))
		columnIndex := schema.ColumnIndex(labelName)
		row = append(row, val.Level(0, 0, 3+columnIndex))
	}

	return row
}
