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
		parts      [][]schema.Chunk
		predicates []ScannerOption
		expected   SelectionResult
	}{
		{
			name: "single page, single predicate",
			parts: [][]schema.Chunk{{
				makeTestChunk("val1", "val2", "val3"),
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val5"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnC", "val4"),
			},
			expected: SelectionResult{
				pick(1, 2),
			},
		},
		{
			name: "single row selection",
			parts: [][]schema.Chunk{{
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val1", "val1", "val1"),
			}, {
				makeTestChunk("val1", "val2", "val3"),
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val5"),
			}, {
				makeTestChunk("val2", "val3", "val3"),
				makeTestChunk("val2", "val3", "val4"),
				makeTestChunk("val2", "val3", "val5"),
			}},
			predicates: []ScannerOption{
				Project("ColumnA", "ColumnC"),
				Equals("ColumnB", "val2"),
				Equals("ColumnC", "val4"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			},
			expected: SelectionResult{
				pick(4, 5),
			},
		},
		{
			name: "multi row selection",
			parts: [][]schema.Chunk{{
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val1", "val1", "val2"),
				makeTestChunk("val1", "val1", "val3"),
			}, {
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val4"),
				makeTestChunk("val1", "val2", "val5"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
				Equals("ColumnC", "val4"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			},
			expected: SelectionResult{
				pick(3, 5),
			},
		},
		{
			name: "multi disjoint rows selection",
			parts: [][]schema.Chunk{{
				makeTestChunk("val1", "val2", "val1"),
				makeTestChunk("val2", "val1", "val1"),
				makeTestChunk("val2", "val2", "val1"),
			}, {
				makeTestChunk("val3", "val1", "val3"),
				makeTestChunk("val3", "val2", "val3"),
				makeTestChunk("val3", "val3", "val3"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expected: SelectionResult{
				pick(0, 1), pick(2, 3), pick(4, 5),
			},
		},
		{
			name: "different page sizes",
			parts: [][]schema.Chunk{{
				makeTestChunk("val0", "val0", "val0"),
			}, {
				makeTestChunk("val1", "val1", "val1"),
				makeTestChunk("val2", "val1", "val1"),
				makeTestChunk("val3", "val1", "val2"),
			}, {
				makeTestChunk("val4", "val2", "val2"),
				makeTestChunk("val5", "val2", "val1"),
				makeTestChunk("val6", "val2", "val2"),
				makeTestChunk("val7", "val2", "val2"),
			}, {
				makeTestChunk("val8", "val3", "val1"),
				makeTestChunk("val9", "val3", "val2"),
				makeTestChunk("val9", "val3", "val3"),
			}, {
				makeTestChunk("val9", "val4", "val1"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnC", "val2"),
			},
			expected: SelectionResult{
				pick(3, 5), pick(6, 8), pick(9, 10),
			},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, createDB(dir, tcase.parts))

			bucket, err := filesystem.NewBucket(dir)
			require.NoError(t, err)

			pqFile, err := db.OpenFileReader("compact", bucket)
			require.NoError(t, err)

			pqreader, err := parquet.OpenFile(pqFile, pqFile.FileSize())
			require.NoError(t, err)

			scanner := NewScanner(pqreader, pqFile, tcase.predicates...)
			rowRanges, err := scanner.Scan()
			require.NoError(t, err)
			require.Equal(t, tcase.expected, rowRanges[0])
		})
	}
}

func createDB(path string, chunksParts [][]schema.Chunk) error {
	chunkSchema := schema.MakeChunkSchema(columns)
	writer := db.NewWriter(path, columns, chunkSchema, db.PageBufferSize(2))
	defer writer.Close()

	for _, part := range chunksParts {
		for _, chunk := range part {
			if err := writer.Write(chunk); err != nil {
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	return writer.Compact()
}
