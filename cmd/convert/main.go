package main

import (
	"fmt"
	"log"
	"math"
	"net/http"
	"os"

	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/schollz/progressbar/v3"

	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"

	_ "net/http/pprof"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()

	tsdbBlock, block, err := openBlock("data", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer tsdbBlock.Close()

	blockQuerier, err := tsdb.NewBlockChunkQuerier(block, math.MinInt64, math.MaxInt64)
	defer blockQuerier.Close()
	if err != nil {
		log.Fatal(err)
	}

	chunkReader, err := block.Chunks()
	if err != nil {
		log.Fatal(err)
	}
	defer chunkReader.Close()

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

	writer := db.NewWriter("./out", allLabels)
	defer writer.Close()

	ps, err := ir.Postings(index.AllPostingsKey())
	if err != nil {
		log.Fatal(err)
	}
	var numPostings int64
	for ps.Next() {
		numPostings++
	}
	log.Println("Converting postings to parquet", "num_postings", numPostings)
	ps, err = ir.Postings(index.AllPostingsKey())
	if err != nil {
		log.Fatal(err)
	}
	ps = ir.SortedPostings(ps)
	var (
		lblBuilder labels.ScratchBuilder
		chks       []chunks.Meta
		seriesID   int64 = -1
	)

	bar := progressbar.Default(numPostings)
	for ps.Next() {
		seriesID++
		lblBuilder.Reset()
		if err := ir.Series(ps.At(), &lblBuilder, &chks); err != nil {
			log.Fatal(err)
		}

		lbls := lblBuilder.Labels()
		for _, chunkMeta := range chks {
			chk, err := chunkReader.Chunk(chunkMeta)
			if err != nil {
				log.Fatal(err)
			}
			chunk := schema.Chunk{
				SeriesID:   seriesID,
				Labels:     lbls.Map(),
				MinT:       chunkMeta.MinTime,
				MaxT:       chunkMeta.MaxTime,
				ChunkBytes: chk.Bytes(),
			}
			if err := writer.Write(chunk); err != nil {
				log.Fatal(err)
			}
		}
		if err := bar.Add(1); err != nil {
			log.Fatal(err)
		}
	}

	if err := writer.Flush(); err != nil {
		log.Fatal(err)
	}

	if err := writer.Compact(); err != nil {
		log.Fatal(err)
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
