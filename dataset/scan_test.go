package dataset

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/schema"
)

var columns = []string{"ColumnA", "ColumnB", "ColumnC", "ColumnD"}

type testRow struct {
	ColumnA string `parquet:",dict"`
	ColumnB string `parquet:",dict"`
	ColumnC string `parquet:",dict"`
}

func makeTestChunk(columnA string, columnB string, columnC string) schema.Chunk {
	return schema.Chunk{
		Labels: labels.FromStrings(
			"ColumnA", columnA,
			"ColumnB", columnB,
			"ColumnC", columnC,
		),
	}
}

func TestScan(t *testing.T) {
	cases := []struct {
		name       string
		rows       []schema.Chunk
		predicates []ScannerOption
		expected   []SelectionResult
	}{
		{
			name: "single row selection",
			rows: []schema.Chunk{
				// Row group 1.
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val1", "val1", "val1"),
				// Row group 2.
				makeTestChunk("val1", "val2", "val3"),
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val5"),
				// Row group 3.
				makeTestChunk("val2", "val3", "val3"),
				makeTestChunk("val2", "val3", "val4"),
				makeTestChunk("val2", "val3", "val5"),
			},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
				Equals("ColumnC", "val4"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			},
			expected: []SelectionResult{
				{},
				{pick(1, 2)},
				{},
			},
		},
		{
			name: "multi row selection",
			rows: []schema.Chunk{
				// Row group 1.
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val1", "val1", "val2"),
				makeTestChunk("val1", "val1", "val3"),
				// Row group 2.
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val5"),
			},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
				Equals("ColumnC", "val4"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			},
			expected: []SelectionResult{
				{},
				{pick(0, 2)},
			},
		},
		{
			name: "multi disjoint rows selection",
			rows: []schema.Chunk{
				// Row group 1.
				makeTestChunk("val1", "val2", "val1"),
				makeTestChunk("val2", "val1", "val1"),
				makeTestChunk("val2", "val2", "val1"),
				// Row group 2.
				makeTestChunk("val3", "val1", "val3"),
				makeTestChunk("val3", "val2", "val3"),
				makeTestChunk("val3", "val3", "val3"),
			},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expected: []SelectionResult{
				{pick(0, 1), pick(2, 3)},
				{pick(1, 2)},
			},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, createDB(dir, tcase.rows))

			bucket, err := filesystem.NewBucket(dir)
			require.NoError(t, err)

			pqFile, err := db.OpenFileReader("part.0", bucket)
			require.NoError(t, err)

			pqreader, err := parquet.OpenFile(pqFile, pqFile.FileSize())
			require.NoError(t, err)

			scanner := NewScanner(pqreader, pqFile, tcase.predicates...)
			rowRanges, err := scanner.Scan()
			require.NoError(t, err)
			require.Equal(t, tcase.expected, rowRanges)
		})
	}
}

func createDB(path string, chunks []schema.Chunk) error {
	chunkSchema := schema.MakeChunkSchema(columns)
	writer := db.NewWriter(path, columns, chunkSchema, db.MaxRowsPerGroup(3))
	defer writer.Close()

	for _, row := range chunks {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return writer.Compact()
}
