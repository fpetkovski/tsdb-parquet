package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore/providers/gcs"
	"gopkg.in/yaml.v3"

	"fpetkovski/tsdb-parquet/dataset"
	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/storage"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

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

	//bucket, err := filesystem.NewBucket("./out")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	reader, err := db.OpenFileReader("compact", bucket)
	if err != nil {
		log.Fatalln(err)
	}

	pqreader, err := parquet.OpenFile(reader, reader.FileSize())
	if err != nil {
		log.Fatalln(err)
	}

	scanner := dataset.NewScanner(pqreader, reader,
		//dataset.GreaterThanOrEqual(schema.MinTColumn, parquet.Int64Value(1680052000000)),
		//dataset.LessThanOrEqual(schema.MaxTColumn, parquet.Int64Value(1680052000000)),
		dataset.Equals(labels.MetricName, "container_cpu_usage_seconds_total"),
		//dataset.Equals("namespace", "monitoring"),
		//dataset.Equals("container", "statsd-loadbalancer"),
		dataset.Project(
			labels.MetricName,
			"namespace",
		),
	)
	selection, err := scanner.Scan()
	if err != nil {
		log.Fatalln(err)
	}

	for _, rowGroup := range selection {
		fmt.Println(rowGroup.NumRows())
	}
}
