package main

import (
	"fmt"
	"fpetkovski/prometheus-parquet/pkg/writer"
	"fpetkovski/prometheus-parquet/schema"
	"log"
	"math"
	"os"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/segmentio/parquet-go"
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

	allLabels, _, err := blockQuerier.LabelNames()
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

	f, err := os.Create(fmt.Sprintf("./out/data.parquet"))
	if err != nil {
		log.Fatal(err)
	}
	bloomFilters := make([]parquet.BloomFilterColumn, 0, len(allLabels))
	for _, lbl := range allLabels {
		bloom := parquet.SplitBlockFilter(10, strings.ToLower(lbl))
		bloomFilters = append(bloomFilters, bloom)
	}

	schema := schema.MakeChunkSchema(allLabels)
	if err != nil {
		log.Fatal(err)
	}

	pqWriter := parquet.NewWriter(f,
		schema.ParquetSchema(),
		parquet.BloomFilters(bloomFilters...),
		parquet.DataPageStatistics(true),
	)
	defer pqWriter.Close()

	bufferedWriter := writer.NewBufferedWriter(pqWriter, schema, 5)
	defer bufferedWriter.Close()

	for _, metric := range metricNames {
		matchMetric := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metric)
		chunkSet := blockQuerier.Select(true, nil, matchMetric)
		writeSeries(bufferedWriter, chunkSet)
	}
}

func writeSeries(writer *writer.BufferedWriter, s storage.ChunkSeriesSet) {
	for s.Next() {
		series := s.At()
		it := series.Iterator(nil)
		for it.Next() {
			chk := it.At()
			if err := writer.Append(series.Labels(), chk); err != nil {
				log.Fatal(err)
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
