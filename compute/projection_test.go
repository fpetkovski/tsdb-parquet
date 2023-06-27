package compute

import (
	"io"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"fpetkovski/tsdb-parquet/dataset"
)

func TestProjectColumns(t *testing.T) {
	cases := []struct {
		name      string
		rows      [][]testRow
		selection []dataset.PickRange

		columns   []string
		chunkSize int64
		expected  [][][]parquet.Value
	}{
		{
			name: "single column projection",
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
			columns:   []string{"ColumnB"},
			chunkSize: 3,
			selection: []dataset.PickRange{dataset.Pick(2, 6)},

			expected: [][][]parquet.Value{{
				{pqVal("val3", 1), pqVal("val4", 1), pqVal("val5", 1)},
			}, {
				{pqVal("val6", 1)},
			}},
		},
		{
			name: "disjoint selection within single page",
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
			columns:   []string{"ColumnB"},
			chunkSize: 3,
			selection: []dataset.PickRange{dataset.Pick(1, 2), dataset.Pick(3, 4), dataset.Pick(4, 5), dataset.Pick(6, 7)},

			expected: [][][]parquet.Value{{
				{pqVal("val2", 1), pqVal("val4", 1), pqVal("val5", 1)},
			}, {
				{pqVal("val7", 1)},
			}},
		},
		{
			name: "disjoint selection",
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
				twoColumnRow("val2", "val9"),
			}},
			columns:   []string{"ColumnB"},
			chunkSize: 3,
			selection: []dataset.PickRange{dataset.Pick(0, 2), dataset.Pick(3, 5), dataset.Pick(6, 9)},

			expected: [][][]parquet.Value{{
				{pqVal("val1", 1), pqVal("val2", 1), pqVal("val4", 1)},
			}, {
				{pqVal("val5", 1), pqVal("val7", 1), pqVal("val8", 1)},
			}, {
				{pqVal("val9", 1)},
			}},
		},
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
			columns:   []string{"ColumnA", "ColumnB"},
			chunkSize: 2,
			selection: []dataset.PickRange{dataset.Pick(2, 6)},

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

			selection := dataset.NewSelectionResult(
				file.RowGroups()[0], tcase.selection,
			)
			projections := ProjectColumns(selection, &nopSectionLoader{}, tcase.chunkSize, tcase.columns...)
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
