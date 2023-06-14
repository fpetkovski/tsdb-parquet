package main

import (
	"log"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore/providers/filesystem"

	"fpetkovski/tsdb-parquet/dataset"
	"fpetkovski/tsdb-parquet/db"
)

func main() {
	//config := storage.GCSConfig{
	//	Bucket: "shopify-o11y-metrics-scratch",
	//}
	//conf, err := yaml.Marshal(config)
	//if err != nil {
	//	log.Fatalln(err)
	//}

	//bucket, err := gcs.NewBucket(context.Background(), nil, conf, "parquet-reader")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	bucket, err := filesystem.NewBucket("./out")
	if err != nil {
		log.Fatalln(err)
	}

	reader, err := db.OpenFileReader("part.5", bucket)
	if err != nil {
		log.Fatalln(err)
	}

	pqreader, err := parquet.OpenFile(reader, reader.FileSize())
	if err != nil {
		log.Fatalln(err)
	}

	scanner := dataset.NewScanner(pqreader, reader,
		//dataset.GreaterThanOrEqual(schema.MinTColumn, parquet.Int64Value(1680050000000)),
		//dataset.LessThanOrEqual(schema.MaxTColumn, parquet.Int64Value(1680052000000)),
		dataset.Equals(labels.MetricName, "container_fs_writes_total"),
		dataset.Equals("namespace", "monitoring"),
		dataset.Equals("container", "prometheus"),
		dataset.Projection(
			labels.MetricName,
			"namespace",
		),
	)
	if scanner.Scan() != nil {
		log.Fatalln(err)
	}
}
