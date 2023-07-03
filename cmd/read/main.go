package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"
	"gopkg.in/yaml.v3"

	"Shopify/thanos-parquet-engine/compute"
	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"
	"Shopify/thanos-parquet-engine/storage"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var heapprofile = flag.String("heapprofile", "", "write heap profile to file")
var tracefile = flag.String("trace", "", "write trace to file")

const batchSize = 16 * 1024

func main() {
	flag.Parse()

	if *heapprofile != "" {
		defer func() {
			f, err := os.Create(*heapprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.WriteHeapProfile(f)
		}()
	}

	config := storage.GCSConfig{
		Bucket: "shopify-thanos-storage-staging",
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

	bucket.Iter(context.Background(), "", func(name string) error {
		fmt.Println(name)
		return nil
	}, objstore.WithRecursiveIter)

	reader, err := db.NewFileReader("compact-2.7", bucket)
	if err != nil {
		log.Fatalln(err)
	}
	defer reader.Close()

	fmt.Println("Opening parquet file")
	pqFile, err := parquet.OpenFile(reader, reader.FileSize(), parquet.ReadBufferSize(db.ReadBufferSize))
	if err != nil {
		log.Fatalln(err)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if *tracefile != "" {
		f, err := os.Create(*tracefile)
		if err != nil {
			log.Fatal(err)
		}
		if err := trace.Start(f); err != nil {
			log.Fatal(err)
		}
		defer trace.Stop()
	}

	fmt.Println("Scanning...")
	scanStart := time.Now()
	scanner := compute.NewScanner(pqFile, reader.SectionLoader(),
		//compute.GreaterThanOrEqual(schema.MinTColumn, parquet.Int64Value(1686873600000)),
		//compute.LessThanOrEqual(schema.MaxTColumn, parquet.Int64Value(1687046400000)),
		compute.Equals(labels.MetricName, "container_network_receive_packets_total"),
		//compute.Equals("namespace", "fbs-production"),
	)
	selections, err := scanner.Select()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Time taken:", time.Since(scanStart))

	fmt.Println("Reading columns...")
	projectStart := time.Now()
	projectionColumns := []string{schema.ChunkBytesColumn}
	for _, selection := range selections {
		fmt.Println("Projecting", selection.NumRows(), "rows")
		projection := compute.ProjectColumns(selection, reader.SectionLoader(), batchSize, projectionColumns...)
		defer projection.Close()

		for {
			columns, err := projection.NextBatch()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}
			printColumns(columns, io.Discard)
			projection.Release(columns)
		}
	}
	fmt.Println("Time taken:", time.Since(projectStart))
}

func printColumns(columns [][]parquet.Value, writer io.Writer) {
	if len(columns) == 0 {
		return
	}
	for i := 0; i < len(columns[0]); i++ {
		for _, c := range columns {
			_, _ = fmt.Fprintf(writer, "%s", c[i])
			_, _ = fmt.Fprintf(writer, " ")
		}
		_, _ = fmt.Fprintln(writer)
	}
}
