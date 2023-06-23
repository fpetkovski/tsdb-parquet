package dataset

import (
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProjectColumns(t *testing.T) {
	cases := []struct {
		name      string
		rows      [][]testRow
		selection []pickRange
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
			selection: []pickRange{pick(2, 6)},
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
			projections := ProjectColumns(selection, &nopSectionLoader{}, 2, "ColumnB", "ColumnA")
			defer projections.Close()
			for {
				values, err := projections.NextBatch()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				for _, array := range values {
					for _, value := range array {
						fmt.Println(value)
					}
				}
				projections.Release(values)
			}
		})
	}
}
