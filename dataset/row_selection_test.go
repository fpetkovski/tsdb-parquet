package dataset

import "testing"

func TestRowSelection(t *testing.T) {
	const numRows = 40
	cases := []struct {
		name       string
		selections []predicateResult
		expected   selectionResult
	}{
		{
			name:       "empty selections",
			selections: nil,
			expected:   selectionResult{pickRows(0, numRows)},
		},
		{
			name:       "single skip",
			selections: []predicateResult{{skipRows(0, 10)}},
			expected:   selectionResult{pickRows(10, numRows)},
		},
		{
			name:       "multiple skips",
			selections: []predicateResult{{skipRows(0, 10), skipRows(25, 32)}},
			expected:   selectionResult{pickRows(10, 25), pickRows(32, numRows)},
		},
		{
			name: "two equal selections",
			selections: []predicateResult{
				{skipRows(5, 10), skipRows(35, numRows)},
				{skipRows(5, 10), skipRows(35, numRows)},
			},
			expected: selectionResult{pickRows(0, 5), pickRows(10, 35)},
		},
		{
			name: "two different selections",
			selections: []predicateResult{
				{skipRows(10, 20), skipRows(25, 30)},
				{skipRows(0, 5), skipRows(15, 23), skipRows(26, 28)},
			},
			expected: selectionResult{
				pickRows(5, 10), pickRows(23, 25), pickRows(30, numRows),
			},
		},
		{
			name: "multiple different selections",
			selections: []predicateResult{
				{skipRows(0, 10), skipRows(20, 30)},
				{skipRows(0, 5), skipRows(15, 23), skipRows(23, 28), skipRows(28, 30)},
				{skipRows(0, 3), skipRows(21, 24), skipRows(24, 30)},
			},
			expected: selectionResult{
				pickRows(10, 15), pickRows(30, numRows),
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := intersectSelections(numRows, testCase.selections...)
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
