package dataset

import (
	"bytes"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

var columns = []string{"ColumnA", "ColumnB", "ColumnC", "ColumnD"}

type testRow struct {
	ColumnA string `parquet:",dict"`
	ColumnB string `parquet:",dict"`
}

func makeTestRow(columnA string, columnB string) testRow {
	return testRow{ColumnA: columnA, ColumnB: columnB}
}

func TestScan(t *testing.T) {
	cases := []struct {
		name       string
		parts      [][]testRow
		predicates []ScannerOption
		expected   SelectionResult
	}{
		{
			//
			//	pages:      |_____|
			//	selection:    |_|
			name: "single page, single predicate",
			parts: [][]testRow{{
				makeTestRow("val1", "val1"),
				makeTestRow("val1", "val2"),
				makeTestRow("val1", "val3"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expected: SelectionResult{
				pick(1, 2),
			},
		},
		{
			//
			//	pages:      |_____||_____||_____|
			//	selection:           |_|
			name: "single row selection",
			parts: [][]testRow{{
				makeTestRow("val1", "val1"),
				makeTestRow("val1", "val2"),
				makeTestRow("val1", "val3"),
			}, {
				makeTestRow("val2", "val4"),
				makeTestRow("val2", "val5"),
				makeTestRow("val2", "val6"),
			}, {
				makeTestRow("val3", "val1"),
				makeTestRow("val3", "val2"),
				makeTestRow("val3", "val3"),
			}},
			predicates: []ScannerOption{
				Project("ColumnA", "ColumnC"),
				Equals("ColumnA", "val2"),
				Equals("ColumnB", "val5"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			},
			expected: SelectionResult{
				pick(4, 5),
			},
		},
		{
			//
			//	pages:      |_____||_____|
			//	selection:      |___|
			name: "multi row selection",
			parts: [][]testRow{{
				makeTestRow("val1", "val1"),
				makeTestRow("val1", "val1"),
				makeTestRow("val1", "val2"),
				makeTestRow("val2", "val2"),
			}, {
				makeTestRow("val2", "val2"),
				makeTestRow("val2", "val2"),
				makeTestRow("val2", "val3"),
			}},
			predicates: []ScannerOption{
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val2"))),
				Equals("ColumnB", "val2"),
			},
			expected: SelectionResult{
				pick(3, 6),
			},
		},
		{
			//
			//	pages:      |_____||_____||_____|
			//	selection:       |___| |___| |__|
			name: "multiple disjoint rows",
			parts: [][]testRow{{
				makeTestRow("val1", "val1"),
				makeTestRow("val2", "val2"),
				makeTestRow("val2", "val2"),
			}, {
				makeTestRow("val3", "val1"),
				makeTestRow("val3", "val3"),
				makeTestRow("val3", "val2"),
			}, {
				makeTestRow("val3", "val2"),
				makeTestRow("val3", "val1"),
				makeTestRow("val3", "val2"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expected: SelectionResult{
				pick(1, 3), pick(5, 7), pick(8, 9),
			},
		},
		{
			//
			//	pages:      |__||______||_______||__|
			//	selection:           |___| |___|  |_|
			name: "different page sizes",
			parts: [][]testRow{{
				makeTestRow("val0", "val0"),
			}, {
				makeTestRow("val1", "val1"),
				makeTestRow("val2", "val1"),
				makeTestRow("val3", "val2"),
			}, {
				makeTestRow("val4", "val2"),
				makeTestRow("val5", "val2"),
				makeTestRow("val6", "val3"),
				makeTestRow("val7", "val3"),
			}, {
				makeTestRow("val8", "val2"),
				makeTestRow("val9", "val2"),
				makeTestRow("val9", "val3"),
			}, {
				makeTestRow("val9", "val2"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expected: SelectionResult{
				pick(3, 6), pick(8, 10), pick(11, 12),
			},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			pqFile, err := createFile(tcase.parts)

			scanner := NewScanner(pqFile, &nopSectionLoader{}, tcase.predicates...)
			rowRanges, err := scanner.Scan()
			require.NoError(t, err)
			require.Equal(t, tcase.expected, rowRanges[0])
		})
	}
}

func createFile(chunksParts [][]testRow) (*parquet.File, error) {
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[testRow](&buffer,
		parquet.PageBufferSize(4),
	)

	for _, parts := range chunksParts {
		_, err := writer.Write(parts)
		if err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	readBuf := bytes.NewReader(buffer.Bytes())
	return parquet.OpenFile(readBuf, int64(len(buffer.Bytes())))
}

type nopSectionLoader struct{}

func (n nopSectionLoader) LoadSection(_, _ int64) error { return nil }
