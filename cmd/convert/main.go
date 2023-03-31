package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"runtime/pprof"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/schema"
)

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

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

	for _, metric := range metricNames {
		if metric != "container_cpu_usage_seconds_total" {
			continue
		}
		matchMetric := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric)
		sset := blockQuerier.Select(true, nil, matchMetric)
		labelSet := make(map[string]struct{}, 0)

		for sset.Next() {
			lbls := sset.At().Labels()
			for _, lbl := range lbls {
				labelSet[lbl.Name] = struct{}{}
			}
		}
		schema, fields := schema.For(labelSet)
		sset = blockQuerier.Select(true, nil, matchMetric)
		writeSeries(metric, sset, schema, fields)
	}
}

func writeSeries(metric string, s storage.SeriesSet, schema *parquet.Schema, fields []reflect.StructField) {
	fmt.Println("Writing metric", metric)
	f, err := os.Create(fmt.Sprintf("./out/%s.parquet", metric))
	if err != nil {
		log.Fatal(err)
	}

	bloomFilters := make([]parquet.BloomFilterColumn, 0, len(fields)-2)
	for _, field := range fields {
		if field.Name == "OBSERVED_TIMESTAMP" || field.Name == "OBSERVED_VALUE" {
			continue
		}
		bloomFilters = append(bloomFilters, parquet.SplitBlockFilter(10, field.Name))
	}

	pqWriter := parquet.NewWriter(f, schema, parquet.BloomFilters(bloomFilters...))
	defer pqWriter.Close()

	row := reflect.StructOf(fields)
	batchSize := 10_000
	batch := reflect.MakeSlice(reflect.SliceOf(row), batchSize, batchSize)

	i := -1
	for s.Next() {
		series := s.At()
		it := series.Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			i++
			item := batch.Index(i)
			for _, lbl := range series.Labels() {
				if strings.HasPrefix(lbl.Name, "_") {
					continue
				}
				item.FieldByName(strings.ToUpper(lbl.Name)).SetString(lbl.Value)
			}
			t, v := it.At()
			item.FieldByName("OBSERVED_TIMESTAMP").SetInt(t)
			item.FieldByName("OBSERVED_VALUE").SetFloat(v)
			pqWriter.Write(item.Interface())
			if i == batchSize-1 {
				pqWriter.Flush()
				i = -1
			}
		}
	}
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
