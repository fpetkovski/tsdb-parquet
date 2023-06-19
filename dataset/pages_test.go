package dataset

import (
	"bytes"
	"io"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestPages(t *testing.T) {
	cases := []struct {
		name           string
		pageValues     [][]string
		selection      SelectionResult
		expectedValues [][]string
		expectedCursor []int64
	}{
		{
			/*
			  pages:     |_______|
			  selection:   |___|
			*/
			name: "single page",
			pageValues: [][]string{
				{"val-0", "val-1", "val-2", "val-3"},
			},
			selection: SelectionResult{pick(1, 3)},
			expectedValues: [][]string{
				{"val-1", "val-2"},
			},
			expectedCursor: []int64{1},
		},
		{
			/*
			  pages:     |__|____|____|_____|
			  selection:        |__| |__| |_|
			*/
			name: "multiple pages",
			pageValues: [][]string{
				{"val-0"},
				{"val-1", "val-2", "val-3"},
				{"val-4", "val-5", "val-6", "val-7"},
				{"val-8", "val-9", "val-10"},
			},
			selection: SelectionResult{pick(3, 5), pick(6, 8), pick(9, 10)},
			expectedValues: [][]string{
				{"val-3"},
				{"val-4"},
				{"val-6", "val-7"},
				{"val-9"},
			},
			expectedCursor: []int64{3, 6, 9},
		},
		{
			/*
			  pages:     |__|__________|_____|
			  selection:      |__| |__| |_|
			*/
			name: "multiple ranges inside a page",
			pageValues: [][]string{
				{"val-0"},
				{"val-1", "val-2", "val-3", "val-4", "val-5", "val-6", "val-7"},
				{"val-8", "val-9", "val-10"},
			},
			selection: SelectionResult{pick(2, 4), pick(6, 8), pick(9, 10)},
			expectedValues: [][]string{
				{"val-2", "val-3"},
				{"val-6", "val-7"},
				{"val-9"},
			},
			expectedCursor: []int64{3, 6, 9},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			var buffer bytes.Buffer
			writer := parquet.NewGenericWriter[testRow](&buffer,
				parquet.WriteBufferSize(4),
				parquet.PageBufferSize(4),
			)

			for _, pageValues := range tcase.pageValues {
				rows := make([]testRow, len(pageValues))
				for i, val := range pageValues {
					rows[i] = testRow{ColumnA: val}
				}
				n, err := writer.Write(rows)
				require.NoError(t, err)
				require.Equal(t, len(rows), n)
			}

			require.NoError(t, writer.Close())

			readBuf := bytes.NewReader(buffer.Bytes())
			file, err := parquet.OpenFile(readBuf, int64(len(buffer.Bytes())))
			require.NoError(t, err)

			pages := SelectPages(file.RowGroups()[0].ColumnChunks()[0], tcase.selection)
			actualValues := make([][]string, 0, len(tcase.expectedValues))
			for {
				page, _, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				values := make([]parquet.Value, page.NumRows())
				n, err := page.Values().ReadValues(values)
				if err != io.EOF {
					require.NoError(t, err)
				}
				pageVals := make([]string, n)
				for i, val := range values {
					pageVals[i] = val.String()
				}
				actualValues = append(actualValues, pageVals)
			}
			require.Equal(t, tcase.expectedValues, actualValues)
		})
	}
}
