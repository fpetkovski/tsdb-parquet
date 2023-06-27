package compute

import (
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"fpetkovski/tsdb-parquet/dataset"
)

func BenchmarkDistinct(b *testing.B) {
	b.StopTimer()

	numRows := 1_000_000
	numPages := 20
	numRowsPerPage := numRows / numPages

	rows := make([][]testRow, numPages)
	for page := 0; page < numPages; page++ {
		rows[page] = make([]testRow, numRowsPerPage)
		for row := 0; row < numRowsPerPage; row++ {
			rows[page][row] = testRow{
				ColumnA: "value-" + strconv.Itoa(row%4),
				ColumnB: "value-" + strconv.Itoa(row%3),
				ColumnC: "value-" + strconv.Itoa(row%2),
				ColumnD: "value-" + strconv.Itoa(row),
			}
		}
	}

	file, err := createSortedFile(b.TempDir(), rows)
	require.NoError(b, err)

	var batchSize int64 = 32 * 1024
	cols := []string{"ColumnA", "ColumnB"}
	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		selection := dataset.SelectRows(file.RowGroups()[0], dataset.SelectAll())
		projection := ProjectColumns(selection, &nopSectionLoader{}, batchSize, cols...)
		distinct := UniqueByColumn(0, projection)
		defer distinct.Close()

		b.StartTimer()

		var numRead int
		for {
			batch, err := distinct.NextBatch()
			if err == io.EOF {
				break
			}
			require.NoError(b, err)
			for i := 1; i < len(batch); i++ {
				require.Equal(b, len(batch[0]), len(batch[i]))
			}
			numRead += len(batch[0])

			require.Len(b, batch, len(cols))
			require.LessOrEqual(b, int64(len(batch[0])), batchSize)
			distinct.Release(batch)
		}
		require.EqualValues(b, 4, numRead)
	}
}
