package dataset

import (
	"os"
	"path"
	"sort"
	"strconv"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func BenchmarkScanner_Scan(b *testing.B) {
	b.StopTimer()

	numRows := 1_000_000
	numPages := 5
	numRowsPerPage := numRows / numPages

	rows := make([][]testRow, numPages)
	for page := 0; page < numPages; page++ {
		rows[page] = make([]testRow, numRowsPerPage)

		for row := 0; row < numRowsPerPage; row++ {
			rows[page][row] = testRow{
				ColumnA: strconv.Itoa(row % 4),
				ColumnB: strconv.Itoa(row % 3),
				ColumnC: strconv.Itoa(row % 2),
				ColumnD: strconv.Itoa(row),
			}
		}
	}

	file, err := createSortedFile(b.TempDir(), rows)
	require.NoError(b, err)

	scanner := NewScanner(file, &nopSectionLoader{},
		Equals("ColumnA", "2"),
		Equals("ColumnB", "1"),
	)

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		result, err := scanner.Select()
		require.EqualValues(b, 83330, result[0].NumRows())
		require.NoError(b, err)
	}
}

func createSortedFile(dir string, parts [][]testRow) (*parquet.File, error) {
	buffer := parquet.NewGenericBuffer[testRow](
		parquet.SortingRowGroupConfig(
			parquet.SortingColumns(
				parquet.Ascending("ColumnA"),
				parquet.Ascending("ColumnB"),
				parquet.Ascending("ColumnC"),
				parquet.Ascending("ColumnD"),
			)))

	for _, part := range parts {
		_, err := buffer.Write(part)
		if err != nil {
			return nil, err
		}
	}
	sort.Sort(buffer)

	filePath := path.Join(dir, "sorted_file.parquet")
	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	writer := parquet.NewGenericWriter[testRow](f)
	if _, err := parquet.CopyRows(writer, buffer.Rows()); err != nil {
		return nil, err
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	f, err = os.Open(filePath)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return parquet.OpenFile(f, stat.Size())
}
