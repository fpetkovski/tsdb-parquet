package dataset

import (
	"os"
	"path"
	"testing"

	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"fpetkovski/tsdb-parquet/db"
)

type testRow struct {
	ColumnA string `parquet:",dict"`
	ColumnB string `parquet:",dict"`
	ColumnC string `parquet:",dict"`
	ColumnD string `parquet:",dict"`
}

func makeTestRow(columnA string, columnB string, columnC string, columnD string) testRow {
	return testRow{ColumnA: columnA, ColumnB: columnB, ColumnC: columnC, ColumnD: columnD}
}

func TestScan(t *testing.T) {
	cases := []struct {
		name     string
		rows     []testRow
		expected []SelectionResult
	}{
		{
			name: "base_case",
			rows: []testRow{
				// Row group 1.
				makeTestRow("val1", "val1", "val1", "val1"),
				makeTestRow("val1", "val1", "val1", "val2"),
				makeTestRow("val1", "val1", "val1", "val3"),
				// Row group 2.
				makeTestRow("val1", "val1", "val2", "val4"),
				makeTestRow("val1", "val1", "val2", "val1"),
				makeTestRow("val1", "val1", "val2", "val2"),
				// Row group 3.
				makeTestRow("val1", "val1", "val3", "val4"),
				makeTestRow("val1", "val1", "val3", "val4"),
				makeTestRow("val1", "val1", "val3", "val5"),
			},
			expected: []SelectionResult{
				{},
				{pick(0, 1)},
				{},
			},
		},
		{
			name: "base_case",
			rows: []testRow{
				// Row group 1.
				makeTestRow("val1", "val1", "val1", "val1"),
				makeTestRow("val1", "val1", "val1", "val2"),
				makeTestRow("val1", "val1", "val1", "val3"),
				// Row group 2.
				makeTestRow("val1", "val1", "val2", "val4"),
				makeTestRow("val1", "val1", "val2", "val2"),
				makeTestRow("val1", "val1", "val2", "val4"),
			},
			expected: []SelectionResult{
				{},
				{pick(0, 1), pick(2, 3)},
			},
		},
	}
	dir := t.TempDir()
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			filePath := path.Join(dir, tcase.name)
			require.NoError(t, createRows(filePath, tcase.rows))

			bucket, err := filesystem.NewBucket(dir)
			require.NoError(t, err)

			pqFile, err := db.OpenFileReader(tcase.name, bucket)
			require.NoError(t, err)

			pqreader, err := parquet.OpenFile(pqFile, pqFile.FileSize())
			require.NoError(t, err)

			scanner := NewScanner(pqreader, pqFile,
				Equals("ColumnC", "val2"),
				Equals("ColumnD", "val4"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			)
			rowRanges, err := scanner.Scan()
			require.NoError(t, err)
			require.Equal(t, tcase.expected, rowRanges)
		})
	}
}

func createRows(path string, rows []testRow) error {
	if err := createDataFile(path, rows); err != nil {
		return err
	}
	if err := createMetaDataFile(path); err != nil {
		return err
	}
	return nil
}

func createMetaDataFile(path string) error {
	f, err := os.Open(path + ".parquet")
	if err != nil {
		return err
	}
	pqReader, err := file.NewParquetReader(f)
	defer pqReader.Close()

	metaFile, err := os.Create(path + ".metadata")
	if err != nil {
		return err
	}

	_, err = pqReader.MetaData().WriteTo(metaFile, nil)
	if err != nil {
		return err
	}
	return metaFile.Close()
}

func createDataFile(path string, rows []testRow) error {
	f, err := os.Create(path + ".parquet")
	if err != nil {
		return err
	}
	writer := parquet.NewWriter(f,
		parquet.MaxRowsPerRowGroup(3),
		parquet.DataPageStatistics(true),
	)

	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	return writer.Close()
}
