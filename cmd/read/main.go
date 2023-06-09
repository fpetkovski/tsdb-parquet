package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore/providers/gcs"
	"gopkg.in/yaml.v3"

	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/storage"
)

func main() {
	config := storage.GCSConfig{
		Bucket: "shopify-o11y-metrics-scratch",
	}
	conf, err := yaml.Marshal(config)
	if err != nil {
		log.Fatalln(err)
	}

	bucket, err := gcs.NewBucket(context.Background(), nil, conf, "thanos-querier")
	if err != nil {
		log.Fatalln(err)
	}

	reader, err := db.OpenReader("part.0", bucket)
	if err != nil {
		log.Fatalln(err)
	}

	pqreader, err := parquet.OpenFile(
		reader,
		reader.FileSize(),
		parquet.SkipPageIndex(true),
	)
	if err != nil {
		log.Fatalln(err)
	}

	start := time.Now()

	namespace := parquet.ByteArrayValue([]byte("monitoring"))

	columnIndex := make(map[int]struct{})
	columns := pqreader.Schema().Columns()
	for i, col := range columns {
		if col[0] == "namespace" {
			columnIndex[i] = struct{}{}
		}
	}

	fmt.Println("Starting to read pages")

	groups := pqreader.RowGroups()
	for _, group := range groups {
		chunks := group.ColumnChunks()
		for _, chunk := range chunks {
			if _, ok := columnIndex[chunk.Column()]; !ok {
				continue
			}

			bloom := chunk.BloomFilter()
			if bloom != nil {
				fmt.Println("Checking bloom filter for column", chunk.Column())
				hasValue, err := chunk.BloomFilter().Check(namespace)
				if err != nil {
					log.Fatalln(err)
				}
				if !hasValue {
					continue
				}
			}

			pages := chunk.Pages()
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}

				minVal, maxVal, ok := page.Bounds()
				if ok {
					fmt.Println("Checking page bounds for column", chunk.Column())
					if chunk.Type().Compare(namespace, minVal) < 0 || chunk.Type().Compare(namespace, maxVal) > 0 {
						continue
					}
				}

				values := make([]parquet.Value, page.NumValues())
				_, err = page.Values().ReadValues(values)
				if err != nil && !errors.Is(err, io.EOF) {
					panic(err)
				}
				fmt.Println("Read new page for column", chunk.Column(), page.NumRows(), page.Size()/1024, "KB")
			}
		}
	}
	fmt.Println("Time taken:", time.Since(start))
}
