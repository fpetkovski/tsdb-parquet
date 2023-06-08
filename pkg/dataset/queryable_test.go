package dataset

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/apache/arrow/go/v10/arrow/memory"
	arrowpq "github.com/apache/arrow/go/v10/parquet"
	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/metadata"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"

	"fpetkovski/prometheus-parquet/schema"
)

func TestQueryable(t *testing.T) {
	labelsA := labels.FromStrings("pod", "nginx-1")
	labelsB := labels.FromStrings("pod", "nginx-2")

	tests := []struct {
		name         string
		rowsPerGroup int64
		chunks       []*schema.Chunk
		mint         int64
		maxt         int64
	}{
		{
			name:         "two values read",
			rowsPerGroup: 2,
			chunks: []*schema.Chunk{
				schema.NewChunk(0, 20, 30, labelsA, nil),
				schema.NewChunk(0, 30, 50, labelsA, nil),
				schema.NewChunk(1, 10, 20, labelsB, nil),
				schema.NewChunk(1, 20, 40, labelsB, nil),
			},
			mint: 0,
			maxt: 100,
		},
		{
			name:         "four values read",
			rowsPerGroup: 2,
			chunks: []*schema.Chunk{
				schema.NewChunk(0, 20, 30, labelsA, nil),
				schema.NewChunk(1, 10, 20, labelsB, nil),
				schema.NewChunk(0, 30, 50, labelsA, nil),
				schema.NewChunk(1, 20, 40, labelsB, nil),
			},
			mint: 0,
			maxt: 100,
		},
		{
			name:         "one value read",
			rowsPerGroup: 2,
			chunks: []*schema.Chunk{
				schema.NewChunk(0, 20, 30, labelsA, nil),
				schema.NewChunk(0, 30, 50, labelsA, nil),
				schema.NewChunk(1, 10, 20, labelsB, nil),
				schema.NewChunk(1, 20, 40, labelsB, nil),
			},
			mint: 0,
			maxt: 20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var outputBuffer bytes.Buffer
			pqschema := schema.MakeChunkSchema([]string{"pod"})
			writer := parquet.NewWriter(
				&outputBuffer,
				pqschema.ParquetSchema(),
				parquet.BloomFilters(parquet.SplitBlockFilter(10, "pod")),
				parquet.MaxRowsPerRowGroup(test.rowsPerGroup),
			)
			for _, chunk := range test.chunks {
				row := pqschema.MakeChunkRow(*chunk)
				_, err := writer.WriteRows([]parquet.Row{row})
				require.NoError(t, err)
			}
			require.NoError(t, writer.Close())

			pqbytes := outputBuffer.Bytes()
			f, err := parquet.OpenFile(bytes.NewReader(pqbytes), int64(len(pqbytes)))
			require.NoError(t, err)

			arrowReader, err := newArrowReader(bytes.NewReader(pqbytes))

			storage := NewParquetStorage(f, arrowReader)
			querier := storage.Querier(context.Background(), test.mint, test.maxt)
			matchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "pod", "nginx-1"),
			}
			_, err = querier.Select([]string{"pod"}, matchers...)
			require.NoError(t, err)
		})
	}
}

type metaSection struct {
	offset, length int64
	bytes          []byte
}

type objectReader struct {
	name   string
	bucket objstore.Bucket
	cache  []metaSection

	position int64
	size     int64
}

func (o *objectReader) Seek(offset int64, whence int) (int64, error) {
	newPosition := o.position
	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition += offset
	case io.SeekEnd:
		newPosition = o.size - offset
	default:
		return 0, fmt.Errorf("seek: invalid whence")
	}
	if newPosition < 0 {
		return 0, fmt.Errorf("seek: negative offset")
	}

	o.position = newPosition
	return o.position, nil
}

func newObjectReader(name string, bucket objstore.Bucket, size int64) *objectReader {
	return &objectReader{
		name:     name,
		bucket:   bucket,
		cache:    make([]metaSection, 0),
		position: 0,
		size:     size,
	}
}

func (o *objectReader) ReadAt(p []byte, off int64) (n int, err error) {
	fmt.Printf("Reading %d bytes at %d\n", len(p), off)
	rangeReader, err := o.bucket.GetRange(context.Background(), o.name, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	return io.ReadFull(rangeReader, p)
}

func TestReadMetadata(t *testing.T) {
	f, err := os.ReadFile("/Users/fpetkovski/Projects/tsdb-parquet/out/meta.thrift")
	require.NoError(t, err)

	meta, err := metadata.NewFileMetaData(f, nil)
	require.NoError(t, err)

	for _, g := range meta.RowGroups {
		fmt.Println(g.Columns[10].MetaData.BloomFilterOffset)
	}
}

func TestQueryableAgainstFile(t *testing.T) {
	f, arrowReader := readObject(t)
	//f := readFile(t)

	storage := NewParquetStorage(f, arrowReader)
	querier := storage.Querier(context.Background(), 1679790635000, 1679808635000)
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "container_cpu_usage_seconds_total"),
		//labels.MustNewMatcher(labels.MatchEqual, "pod", "prometheus"),
		labels.MustNewMatcher(labels.MatchEqual, "namespace", "monitoring"),
	}
	fmt.Println("Selecting data from parquet file")

	_, err := querier.Select([]string{"namespace", "pod"}, matchers...)
	require.NoError(t, err)

	block, err := tsdb.OpenBlock(nil, "/Users/fpetkovski/Projects/tsdb-parquet/data/01GWF1YNCVC0WWTPRTGDBB3VZM", chunkenc.NewPool())
	require.NoError(t, err)

	tsdbQuerier, err := tsdb.NewBlockChunkQuerier(block, 1679790635000, 1679808635000)
	require.NoError(t, err)

	var numSeries, numChunks int64
	sset := tsdbQuerier.Select(true, nil, matchers...)
	for sset.Next() {
		numSeries++
		chunks := sset.At().Iterator(nil)
		for chunks.Next() {
			numChunks++
		}
	}
	fmt.Println(numSeries, numChunks)
}

func readObject(t *testing.T) (*parquet.File, *pqarrow.FileReader) {
	bucket, err := gcs.NewBucketWithConfig(context.Background(), nil, gcs.Config{Bucket: "shopify-o11y-metrics-scratch"}, "")
	require.NoError(t, err)

	attrs, err := bucket.Attributes(context.Background(), "01GWF1YNCVC0WWTPRTGDBB3VZM.parquet")
	require.NoError(t, err)

	reader := newObjectReader("01GWF1YNCVC0WWTPRTGDBB3VZM.parquet", bucket, attrs.Size)
	fileConfig := &parquet.FileConfig{
		ReadMode:         parquet.ReadModeAsync,
		SkipBloomFilters: true,
	}

	f, err := parquet.OpenFile(reader, attrs.Size, fileConfig)
	require.NoError(t, err)

	freader, err := newArrowReader(reader)
	require.NoError(t, err)

	return f, freader
}

func newArrowReader(reader arrowpq.ReaderAtSeeker) (*pqarrow.FileReader, error) {
	pqreader, err := file.NewParquetReader(reader)
	if err != nil {
		return nil, err
	}

	return pqarrow.NewFileReader(pqreader, pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 4 * 1024 * 1024,
	}, memory.DefaultAllocator)
}

func readFile(t *testing.T) *parquet.File {
	fd, err := os.Open("/Users/fpetkovski/Projects/tsdb-parquet/out/data.parquet")
	require.NoError(t, err)

	stats, err := fd.Stat()
	require.NoError(t, err)

	f, err := parquet.OpenFile(fd, stats.Size())
	require.NoError(t, err)

	return f
}
