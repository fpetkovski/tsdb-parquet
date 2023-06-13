package main

import (
	"context"
	"log"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore/providers/gcs"
	"gopkg.in/yaml.v3"

	"fpetkovski/tsdb-parquet/dataset"
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

	bucket, err := gcs.NewBucket(context.Background(), nil, conf, "parquet-reader")
	if err != nil {
		log.Fatalln(err)
	}

	reader, err := db.OpenFileReader("part.4", bucket)
	if err != nil {
		log.Fatalln(err)
	}

	pqreader, err := parquet.OpenFile(reader, reader.FileSize())
	if err != nil {
		log.Fatalln(err)
	}

	scanner := dataset.NewScanner(pqreader, reader,
		dataset.Predicate(labels.MetricName, "container_fs_writes_total"),
		dataset.Predicate("namespace", "monitoring"),
		//dataset.Predicate(schema.ChunkBytesColumn, "0"),
	)
	if scanner.Scan() != nil {
		log.Fatalln(err)
	}
}
