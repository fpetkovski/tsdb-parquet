package main

import (
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"log"
	"math"
	"os"
)

func main() {
	db, block, err := openBlock("data", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	blockQuerier, err := tsdb.NewBlockQuerier(block, math.MinInt64, math.MaxInt64)
	if err != nil {
		log.Fatal(err)
	}

	matchAll := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, ".+")
	sset := blockQuerier.Select(false, nil, matchAll)
	for sset.Next() {
		s := sset.At()
		fmt.Println(s.Labels())
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
