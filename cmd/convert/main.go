package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"

	_ "net/http/pprof"

	"github.com/googleapis/gax-go/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/schollz/progressbar/v3"
	"google.golang.org/api/iterator"
	"gopkg.in/alecthomas/kingpin.v2"

	iterablebucket "cloud.google.com/go/storage"
)

type Options struct {
	// The path to the directory containing the TSDB blocks.
	TSDBPath string
	// The path to the directory to write the Parquet files to.
	ParquetPath string
	// List of tenants to convert.
	Tenants []string
	// Max age of a block to convert.
	MaxAge time.Duration
	// Metrics address to expose metrics on.
	Debug bool
}

type meta struct {
	Ulid    string `json:"ulid"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
	Thanos  struct {
		Labels     map[string]string `json:"labels"`
		Source     string            `json:"source"`
		Downsample struct {
			Resolution int64 `json:"resolution"`
		}
	}
	Compaction struct {
		Level int `json:"level"`
	}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
		http.Handle("/metrics", promhttp.Handler())
	}()

	//opts := Options{
	//	TSDBPath: "shopify-thanos-storage-staging",
	//	MaxAge:   24 * time.Hour,
	//	//Tenants:  []string{"apps-a-us-east1-24-3"},
	//	Tenants: []string{"mysql-sandbox-us-ea1-vv4"},
	//} //To be deleted

	app := kingpin.New("parquet-convert", "Convert TSDB blocks to Parquet files.")
	opts := Options{}
	err := (&opts).BindFlags(app)

	mints := calculateMinTimestamp(&opts)
	fmt.Println("Min timestamp:", mints)

	var buf bytes.Buffer
	fmt.Println("Connecting to GCS, raw")
	err = listFiles(&buf, opts.TSDBPath)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Connected to GCS, raw")
	m := make(map[string]string)

	for _, line := range strings.Split(buf.String(), "\n") {
		if len(line) > 0 {
			split := strings.Split(line, " ")
			m[split[0]] = split[1]
		}
	}

	var translateblocks []string
	var mu sync.Mutex

	waitChan := make(chan struct{}, 5)
	count := 0
	for file, timestamp := range m {
		timestampint64, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil {
			log.Fatalln(err)
		}
		//skip files that are older than now - maxage
		if timestampint64 < mints {
			continue
		}
		//Skip files that are not ending in meta.json
		if !strings.HasSuffix(file, "meta.json") {
			continue
		}

		waitChan <- struct{}{}
		count++
		go func(count int) {
			data, err := downloadFileIntoMemory(io.Discard, opts.TSDBPath, file)
			if err == iterablebucket.ErrObjectNotExist {
				//continue
				fmt.Println("File not found:", file)
			}
			if err != nil {
				log.Fatalln(err)
			}

			blkinfo := meta{}
			json.Unmarshal(data, &blkinfo)

			fmt.Println("Downloaded file:", file, "k8s_cluster:", blkinfo.Thanos.Labels["k8s_cluster"], "source:", blkinfo.Thanos.Source, "downsample resolution:", blkinfo.Thanos.Downsample.Resolution, "compaction level:", blkinfo.Compaction.Level)

			if blkinfo.Thanos.Labels["k8s_cluster"] == opts.Tenants[0] && blkinfo.Thanos.Source == "compactor" && blkinfo.Thanos.Downsample.Resolution == 0 && blkinfo.Compaction.Level == 2 {
				fmt.Println("Adding file to translateblocks:", blkinfo.Ulid)
				mu.Lock()
				translateblocks = append(translateblocks, blkinfo.Ulid)
				mu.Unlock()
			}

			fmt.Println("Blocks to translate (inloop):")
			fmt.Println(translateblocks)
			<-waitChan
		}(count)
	}

	fmt.Println("Blocks to translate:")
	fmt.Println(translateblocks)

	//Pass translateblocks to the function that will translate them

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

func (o *Options) BindFlags(app *kingpin.Application) error {
	app.Flag("tsdb-path", "The path to the directory containing the TSDB blocks.").
		Default("").StringVar(&o.TSDBPath)
	app.Flag("parquet-path", "The path to the directory to write the Parquet files to.").
		Default("").StringVar(&o.ParquetPath)
	app.Flag("tenant", "List of tenants to convert.").
		Default("").StringsVar(&o.Tenants)
	app.Flag("max-age", "Max age of a block to convert.").
		Default("24h").DurationVar(&o.MaxAge)
	app.Flag("debug", "Enable debug logging.").BoolVar(&o.Debug)

	_, err := app.Parse(os.Args[1:])
	if err != nil {
		return err
	}
	return nil
}

// listFiles lists objects within specified bucket.
func listFiles(w io.Writer, bucket string) error {
	// bucket := "bucket-name"
	ctx := context.Background()
	client, err := iterablebucket.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()

	// https://pkg.go.dev/cloud.google.com/go/storage@v1.28.1#Query.SetAttrSelection
	query := &iterablebucket.Query{}
	query.SetAttrSelection([]string{"Name", "Updated"})

	it := client.Bucket(bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("Bucket(%q).Objects: %w", bucket, err)
		}

		fmt.Fprintln(w, attrs.Name, attrs.Updated.Unix())

	}
	return nil
}

func downloadFileIntoMemory(w io.Writer, bucket, object string) ([]byte, error) {
	// bucket := "bucket-name"
	// object := "object-name"
	ctx := context.Background()
	client, err := iterablebucket.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).Retryer(iterablebucket.WithBackoff(gax.Backoff{
		Initial:    2 * time.Second,
		Max:        300 * time.Second,
		Multiplier: 3,
	})).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("io.ReadAll: %w", err)
	}
	fmt.Fprintf(w, "Blob %v downloaded.\n", object)
	return data, nil
}

// Calculate minimum timestamp file can have to be downloaded
func calculateMinTimestamp(opts *Options) int64 {
	return time.Now().Unix() - int64(opts.MaxAge/time.Second)
}
