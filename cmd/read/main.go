package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore/providers/filesystem"

	"fpetkovski/tsdb-parquet/dataset"
	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/schema"
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

	//config := storage.GCSConfig{
	//	Bucket: "shopify-o11y-metrics-scratch",
	//}
	//conf, err := yaml.Marshal(config)
	//if err != nil {
	//	log.Fatalln(err)
	//}
	//
	//bucket, err := gcs.NewBucket(context.Background(), nil, conf, "parquet-reader")
	//if err != nil {
	//	log.Fatalln(err)
	//}

	bucket, err := filesystem.NewBucket("./out")
	if err != nil {
		log.Fatalln(err)
	}

	reader, err := db.OpenFileReader("compact-2.7", bucket)
	if err != nil {
		log.Fatalln(err)
	}

	pqFile, err := parquet.OpenFile(reader, reader.FileSize(), parquet.ReadBufferSize(db.ReadBufferSize))
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Println("Scanning...")
	scanStart := time.Now()
	scanner := dataset.NewScanner(pqFile, reader.SectionLoader(),
		//dataset.GreaterThanOrEqual(schema.MinTColumn, parquet.Int64Value(1686873600000)),
		//dataset.LessThanOrEqual(schema.MaxTColumn, parquet.Int64Value(1687046400000)),
		dataset.Equals(labels.MetricName, "nginx_ingress_controller_response_duration_seconds_bucket"),
		//dataset.Equals("namespace", "fbs-production"),
	)
	selections, err := scanner.Scan()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Time taken:", time.Since(scanStart))

	fmt.Println("Reading columns...")
	projectStart := time.Now()
	projectionColumns := []string{schema.MinTColumn, labels.MetricName, "namespace", "pod", "zone"}
	for _, selection := range selections {
		fmt.Println("Projecting", selection.NumRows(), "rows")
		projection := dataset.Projection(selection, reader.SectionLoader(), projectionColumns...)
		columns, err := projection.ReadColumnRanges()
		if err != nil {
			log.Fatalln(err)
		}
		printColumns(columns, io.Discard)
	}
	fmt.Println("Time taken:", time.Since(projectStart))
}

func printColumns(columns [][]parquet.Value, writer io.Writer) {
	if len(columns) == 0 {
		return
	}
	for i := 0; i < len(columns[0]); i++ {
		for _, c := range columns {
			fmt.Fprintf(writer, c[i].String())
			fmt.Fprintf(writer, " ")
		}
		fmt.Fprintln(writer)
	}
}
