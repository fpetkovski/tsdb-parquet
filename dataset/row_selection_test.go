package dataset

import "testing"

func TestRowSelection(t *testing.T) {
	const numRows = 40
	cases := []struct {
		name       string
		selections []RowSelection
		expected   SelectionResult
	}{
		{
			name:       "empty selections",
			selections: nil,
			expected:   SelectionResult{pick(0, numRows)},
		},
		{
			name:       "single skip",
			selections: []RowSelection{{skip(0, 10)}},
			expected:   SelectionResult{pick(10, numRows)},
		},
		{
			name:       "multiple skips",
			selections: []RowSelection{{skip(0, 10), skip(25, 32)}},
			expected:   SelectionResult{pick(10, 25), pick(32, numRows)},
		},
		{
			name: "two equal selections",
			selections: []RowSelection{
				{skip(5, 10), skip(35, numRows)},
				{skip(5, 10), skip(35, numRows)},
			},
			expected: SelectionResult{pick(0, 5), pick(10, 35)},
		},
		{
			name: "one selection is a subset of another",
			selections: []RowSelection{
				{skip(0, 10), skip(28, 37)},
				{skip(5, 8), skip(30, 35)},
			},
			expected: SelectionResult{pick(10, 28), pick(37, numRows)},
		},
		{
			name: "two different selections",
			selections: []RowSelection{
				{skip(10, 20), skip(25, 30)},
				{skip(0, 5), skip(15, 23), skip(26, 28)},
			},
			expected: SelectionResult{
				pick(5, 10), pick(23, 25), pick(30, numRows),
			},
		},
		{
			name: "multiple different selections",
			selections: []RowSelection{
				{skip(0, 10), skip(20, 30)},
				{skip(0, 5), skip(15, 23), skip(23, 28), skip(28, 30)},
				{skip(0, 3), skip(21, 24), skip(24, 30)},
			},
			expected: SelectionResult{
				pick(10, 15), pick(30, numRows),
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := pickRanges(numRows, testCase.selections...)
			if len(result) != len(testCase.expected) {
				t.Fatalf("expected %d ranges, got %d", len(testCase.expected), len(result))
			}
			for i, expected := range testCase.expected {
				if result[i] != expected {
					t.Errorf("expected range %d to be %v, got %v", i, expected, result[i])
				}
			}
		})
	}
}
