package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	kitlog "github.com/go-kit/kit/log"
	"github.com/thanos-io/objstore/providers/gcs"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gopkg.in/alecthomas/kingpin.v2"

	"Shopify/thanos-parquet-engine/compact"

	gcsStorage "cloud.google.com/go/storage"
)

type Options struct {
	// The path to the directory containing the TSDB blocks.
	TSDBPath string
	// The path to the directory to write the Parquet files to.
	ParquetPath string
	// DataDir is the temporary directory used as scratch space by the translator.
	DataDir string
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

	logger := kitlog.NewLogfmtLogger(os.Stdout)
	tsdbBucketConfig := gcs.Config{Bucket: opts.TSDBPath}
	tsdbBucket, err := gcs.NewBucketWithConfig(context.Background(), logger, tsdbBucketConfig, "parquet-translate")
	if err != nil {
		log.Fatal(err)
	}
	parquetBucketConfig := gcs.Config{Bucket: opts.ParquetPath}
	parquetBucket, err := gcs.NewBucketWithConfig(context.Background(), logger, parquetBucketConfig, "parquet-translate")
	if err != nil {
		log.Fatal(err)
	}
	translator := compact.NewTranslator(logger, tsdbBucket, parquetBucket, opts.DataDir)
	if err := translator.TranslateBlock(context.Background(), metas[0]); err != nil {
		log.Fatal(err)
	}
}

func (o *Options) BindFlags(app *kingpin.Application) error {
	app.Flag("tsdb-path", "The path to the directory containing the TSDB blocks.").
		Default("").StringVar(&o.TSDBPath)
	app.Flag("parquet-path", "The path to the directory to write the Parquet files to.").
		Default("").StringVar(&o.ParquetPath)
	app.Flag("data-dir", "A temporary directory used as scratch space.").
		Default("tmp").StringVar(&o.DataDir)
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

// Calculate minimum timestamp file can have to be downloaded
func (o *Options) minTimestamp() int64 {
	return time.Now().Unix() - int64(o.MaxAge/time.Second)
}
