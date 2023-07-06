package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/thanos-io/thanos/pkg/block/metadata"

	_ "net/http/pprof"

	"Shopify/thanos-parquet-engine/compact"
	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"

	"github.com/googleapis/gax-go/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/schollz/progressbar/v3"
	"google.golang.org/api/iterator"
	"gopkg.in/alecthomas/kingpin.v2"

	gcsStorage "cloud.google.com/go/storage"
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

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println(http.ListenAndServe("localhost:8080", nil))
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
	if err != nil {
		log.Fatal(err)
	}

	gcsClient, err := gcsStorage.NewClient(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fetcher := compact.NewThanosMetaFetcher(gcsClient, opts.TSDBPath)
	metas, err := fetcher.FetchMetas(context.Background(), opts.minTimestamp(), opts.Tenants[0])
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(metas)

	// Pass translateBlocks to the function that will translate them

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

// listFiles lists meta.json files within specified bucket which are created after a given timestamp.
func listMetas(bucket string, minTS int64) ([]string, error) {
	metaFiles := make([]string, 0)

	ctx := context.Background()
	client, err := gcsStorage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()

	// https://pkg.go.dev/cloud.google.com/go/storage@v1.28.1#Query.SetAttrSelection
	query := &gcsStorage.Query{}
	if err := query.SetAttrSelection([]string{"Name", "Updated"}); err != nil {
		return nil, fmt.Errorf("query.SetAttrSelection: %w", err)
	}

	it := client.Bucket(bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Bucket(%q).Objects: %w", bucket, err)
		}
		if attrs.Updated.Unix() < minTS {
			continue
		}
		if !strings.HasSuffix(attrs.Name, "/meta.json") {
			continue
		}

		fmt.Println("Identified file", attrs.Name, attrs.Updated.Unix())
		metaFiles = append(metaFiles, attrs.Name)
	}
	return metaFiles, nil
}

func readMeta(w io.Writer, bucket, object string) (*metadata.Meta, error) {
	ctx := context.Background()
	client, err := gcsStorage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*600)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).Retryer(gcsStorage.WithBackoff(gax.Backoff{
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
	meta := &metadata.Meta{}
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// Calculate minimum timestamp file can have to be downloaded
func (o *Options) minTimestamp() int64 {
	return time.Now().Unix() - int64(o.MaxAge/time.Second)
}
