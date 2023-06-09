package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"

	froststorage "github.com/polarsignals/frostdb/storage"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/polarsignals/frostdb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/objstore/providers/filesystem"
	"golang.org/x/exp/maps"

	"fpetkovski/tsdb-parquet/schema"
)

func main() {
	db, block, err := openBlock("data", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	blockQuerier, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	defer blockQuerier.Close()
	if err != nil {
		log.Fatal(err)
	}

	ir, err := block.Index()
	if err != nil {
		log.Fatal(err)
	}
	defer ir.Close()

	metricNames, err := ir.LabelValues(labels.MetricName)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Converting metrics to parquet", "num_metrics", len(metricNames))

	// Create a new column store
	dir := "./out"
	bucket, err := filesystem.NewBucket(dir)
	if err != nil {
		log.Fatal(err)
	}
	columnstore, _ := frostdb.New(
		frostdb.WithBucketStorage(bucket),
		frostdb.WithStorage(froststorage.NewBucketReaderAt(bucket)),
		frostdb.WithStoragePath(dir),
	)
	defer columnstore.Close()

	// Open up a database in the column store
	database, _ := columnstore.DB(context.Background(), "tsdb")

	schema := schema.Prometheus()
	table, _ := database.Table("metrics", frostdb.NewTableConfig(schema))

	for _, metric := range metricNames {
		if !strings.HasPrefix(metric, "prometheus_") {
			continue
		}
		fmt.Println("Writing metric", metric)
		matchMetric := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric)
		sset := blockQuerier.Select(true, nil, matchMetric)
		writeSeries(table, sset)
	}
}

func writeSeries(table *frostdb.Table, sset storage.SeriesSet) {
	for sset.Next() {
		series := sset.At()
		lbls := series.Labels()
		samples := series.Iterator(nil)
		for samples.Next() != chunkenc.ValNone {
			t, v := samples.At()
			sample := Sample{T: t, V: v, Labels: lbls}
			//fmt.Println("Inserting record")
			_, err := table.Write(context.Background(), sample)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

type Sample struct {
	T      int64
	V      float64
	Labels labels.Labels
}

type samplesBatch struct {
	labels  map[string]struct{}
	samples []Sample
}

func newSeriesBatch() *samplesBatch {
	return &samplesBatch{
		labels:  make(map[string]struct{}),
		samples: make([]Sample, 0),
	}
}

func (s *samplesBatch) append(lbls labels.Labels, t int64, v float64) {
	for _, lbl := range lbls {
		s.labels[lbl.Name] = struct{}{}
	}
	s.samples = append(s.samples, Sample{
		T:      t,
		V:      v,
		Labels: lbls,
	})
}

func (s *samplesBatch) size() int {
	return len(s.samples)
}

func (s *samplesBatch) reset() {
	s.labels = make(map[string]struct{})
	s.samples = s.samples[:0]
}

func (s *samplesBatch) toArrowRecord(allocator memory.Allocator) arrow.Record {
	fields := make([]arrow.Field, 0, len(s.labels)+2)
	builders := make([]*array.BinaryBuilder, 0, len(s.labels))

	keys := maps.Keys(s.labels)
	sort.Strings(keys)

	for _, k := range keys {
		fields = append(fields, arrow.Field{Name: "labels." + k, Type: arrow.BinaryTypes.String})
		builders = append(builders, array.NewBinaryBuilder(allocator, arrow.BinaryTypes.String))
	}

	fields = append(fields, arrow.Field{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64})
	bt := array.NewInt64Builder(allocator)

	fields = append(fields, arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Float64})
	bv := array.NewFloat64Builder(allocator)

	for _, s := range s.samples {
		for i, k := range keys {
			b := builders[i]
			v := s.Labels.Get(k)
			if v != "" {
				b.AppendString(v)
			} else {
				b.AppendNull()
			}
		}
		bt.Append(s.T)
		bv.Append(s.V)
	}

	arrays := make([]arrow.Array, 0, len(s.labels)+2)
	for _, b := range builders {
		arrays = append(arrays, b.NewArray())
	}
	arrays = append(arrays, bt.NewArray())
	arrays = append(arrays, bv.NewArray())

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, arrays, int64(len(s.samples)))
}

func openBlock(path string, blockID string) (*tsdb.DBReadOnly, tsdb.BlockReader, error) {
	db, err := tsdb.OpenDBReadOnly(path, nil)
	if err != nil {
		return nil, nil, err
	}
	blocks, err := db.Blocks()
	if err != nil {
		return nil, nil, err
	}
	var block tsdb.BlockReader
	if blockID != "" {
		for _, b := range blocks {
			if b.Meta().ULID.String() == blockID {
				block = b
				break
			}
		}
	} else if len(blocks) > 0 {
		block = blocks[len(blocks)-1]
	}
	if block == nil {
		return nil, nil, fmt.Errorf("block %s not found", blockID)
	}
	return db, block, nil
}
