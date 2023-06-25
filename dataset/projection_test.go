package dataset

import (
	"io"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestProjectColumns(t *testing.T) {
	cases := []struct {
		name      string
		rows      [][]testRow
		selection []pickRange

		project  []string
		expected [][][]parquet.Value
	}{
		{
			name: "two column projection",
			rows: [][]testRow{{
				twoColumnRow("val1", "val1"),
				twoColumnRow("val1", "val2"),
				twoColumnRow("val1", "val3"),
				twoColumnRow("val1", "val4"),
			}, {
				twoColumnRow("val2", "val5"),
				twoColumnRow("val1", "val6"),
				twoColumnRow("val2", "val7"),
				twoColumnRow("val2", "val8"),
			}},
			project:   []string{"ColumnA", "ColumnB"},
			selection: []pickRange{pick(2, 6)},

			expected: [][][]parquet.Value{{
				{pqVal("val1", 0), pqVal("val1", 0)},
				{pqVal("val3", 1), pqVal("val4", 1)},
			}, {
				{pqVal("val2", 0), pqVal("val1", 0)},
				{pqVal("val5", 1), pqVal("val6", 1)},
			}},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			file, err := createFile(tcase.rows)
			require.NoError(t, err)

			selection := SelectionResult{
				rowGroup: file.RowGroups()[0],
				ranges:   tcase.selection,
			}
			projections := ProjectColumns(selection, &nopSectionLoader{}, 2, "ColumnA", "ColumnB")
			defer projections.Close()
			for {
				values, err := projections.NextBatch()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				require.Equal(t, tcase.expected[0], values)
				projections.Release(values)

				tcase.expected = tcase.expected[1:]
			}
		})
	}
}

func pqVal(val any, columnIndex int) parquet.Value {
	return parquet.ValueOf(val).Level(0, 0, columnIndex)
}
