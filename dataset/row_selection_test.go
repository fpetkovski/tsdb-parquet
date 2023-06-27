package dataset

import (
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestRowSelection(t *testing.T) {
	const numRows = 40
	rows := make([]testRow, numRows)
	rowGroup := parquet.NewBuffer(parquet.SchemaOf(&testRow{}))
	for _, row := range rows {
		require.NoError(t, rowGroup.Write(row))
	}

	cases := []struct {
		name       string
		selections []RowSelection
		expected   []PickRange
	}{
		{
			name:       "empty selections",
			selections: nil,
			expected:   []PickRange{Pick(0, numRows)},
		},
		{
			name:       "single skip",
			selections: []RowSelection{{skip(0, 10)}},
			expected:   []PickRange{Pick(10, numRows)},
		},
		{
			name:       "multiple skips",
			selections: []RowSelection{{skip(0, 10), skip(25, 32)}},
			expected:   []PickRange{Pick(10, 25), Pick(32, numRows)},
		},
		{
			name: "two equal selections",
			selections: []RowSelection{
				{skip(5, 10), skip(35, numRows)},
				{skip(5, 10), skip(35, numRows)},
			},
			expected: []PickRange{Pick(0, 5), Pick(10, 35)},
		},
		{
			name: "one selection is a subset of another",
			selections: []RowSelection{
				{skip(0, 10), skip(28, 37)},
				{skip(5, 8), skip(30, 35)},
			},
			expected: []PickRange{Pick(10, 28), Pick(37, numRows)},
		},
		{
			name: "two different selections",
			selections: []RowSelection{
				{skip(10, 20), skip(25, 30)},
				{skip(0, 5), skip(15, 23), skip(26, 28)},
			},
			expected: []PickRange{
				Pick(5, 10), Pick(23, 25), Pick(30, numRows),
			},
		},
		{
			name: "multiple different selections",
			selections: []RowSelection{
				{skip(0, 10), skip(20, 30)},
				{skip(0, 5), skip(15, 23), skip(23, 28), skip(28, 30)},
				{skip(0, 3), skip(21, 24), skip(24, 30)},
			},
			expected: []PickRange{
				Pick(10, 15), Pick(30, numRows),
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := SelectRows(rowGroup, testCase.selections...)
			if len(result.ranges) != len(testCase.expected) {
				t.Fatalf("expected %d ranges, got %d", len(testCase.expected), len(result.ranges))
			}
			for i, expected := range testCase.expected {
				if result.ranges[i] != expected {
					t.Errorf("expected range %d to be %v, got %v", i, expected, result.ranges[i])
				}
			}
		})
	}
}

type testRow struct {
	ColumnA string `parquet:",dict"`
	ColumnB string `parquet:",dict"`
	ColumnC string `parquet:",dict"`
	ColumnD string `parquet:",dict"`
}
