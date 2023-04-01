package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/schema"
)

func main() {
	db, block, err := openBlock("data", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	blockQuerier, err := tsdb.NewBlockChunkQuerier(block, math.MinInt64, math.MaxInt64)
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
		labelSet := make(map[string]struct{}, 0)
		matchMetric := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric)
		sset := blockQuerier.Select(true, nil, matchMetric)

		for sset.Next() {
			lbls := sset.At().Labels()
			for _, lbl := range lbls {
				labelSet[lbl.Name] = struct{}{}
			}
		}

		schema, fields := schema.ChunkSchema(labelSet)
		chunkSet := blockQuerier.Select(true, nil, matchMetric)
		writeSeries(metric, schema, chunkSet, fields)
	}
}

func writeSeries(metric string, schema *parquet.Schema, s storage.ChunkSeriesSet, fields []reflect.StructField) {
	f, err := os.Create(fmt.Sprintf("./out/%s.parquet", metric))
	if err != nil {
		log.Fatal(err)
	}

	bloomFilters := make([]parquet.BloomFilterColumn, 0, len(fields)-2)
	for _, field := range fields {
		if field.Name == "TIMESTAMP_FROM" || field.Name == "TIMESTAMP_FROM" || field.Name == "CHUNK" {
			continue
		}
		bloomFilters = append(bloomFilters, parquet.SplitBlockFilter(10, strings.ToLower(field.Name)))
	}

	fmt.Println("Writing metric", metric)
	pqWriter := parquet.NewWriter(f, schema,
		parquet.BloomFilters(bloomFilters...),
		parquet.DataPageStatistics(true),
	)
	defer pqWriter.Close()

	batchSize := 1000
	row := reflect.StructOf(fields)
	batch := reflect.MakeSlice(reflect.SliceOf(row), batchSize, batchSize)

	i := -1
	for s.Next() {
		series := s.At()
		it := series.Iterator(nil)
		for it.Next() {
			i++
			item := batch.Index(i)
			for _, lbl := range series.Labels() {
				if lbl.Name == labels.MetricName {
					lbl.Name = "SPECIAL_METRIC_NAME"
				}
				if strings.HasPrefix(lbl.Name, "_") {
					continue
				}
				item.FieldByName(strings.ToUpper(lbl.Name)).SetString(lbl.Value)
			}

			chk := it.At()
			item.FieldByName("TIMESTAMP_FROM").SetInt(chk.MinTime)
			item.FieldByName("TIMESTAMP_TO").SetInt(chk.MaxTime)
			item.FieldByName("CHUNK").SetBytes(chk.Chunk.Bytes())
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
