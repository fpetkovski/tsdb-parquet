package dataset

import (
	"strconv"
	"testing"

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

	file, err := createFile(rows)
	require.NoError(b, err)

	scanner := NewScanner(file, &nopSectionLoader{},
		Equals("ColumnA", "2"),
		Equals("ColumnB", "1"))

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err = scanner.Scan()
		require.NoError(b, err)
	}
}
