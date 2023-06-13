package dataset

import "testing"

func TestRowSelection(t *testing.T) {
	cases := []struct {
		name       string
		selections []rowSelection
		expected   []rowRange
	}{
		{
			name:       "empty selections",
			selections: nil,
		},
		{
			name:       "single pick selection",
			selections: []rowSelection{{pickRows(0, 10)}},
			expected:   []rowRange{pickRows(0, 10)},
		},
		{
			name:       "single skip selection",
			selections: []rowSelection{{skipRows(0, 10)}},
			expected:   []rowRange{skipRows(0, 10)},
		},
		{
			name: "two selections",
			selections: []rowSelection{
				{skipRows(0, 10), pickRows(10, 20), pickRows(20, 30)},
				{skipRows(0, 5), pickRows(5, 15), skipRows(15, 23), pickRows(23, 28), skipRows(28, 30)},
			},
			expected: []rowRange{
				skipRows(0, 10), pickRows(10, 15), skipRows(15, 23), pickRows(23, 28), skipRows(28, 30),
			},
		},
		{
			name: "multiple selections",
			selections: []rowSelection{
				{skipRows(0, 10), pickRows(10, 20), pickRows(20, 30)},
				{skipRows(0, 5), pickRows(5, 15), skipRows(15, 23), pickRows(23, 28), skipRows(28, 30)},
				{skipRows(0, 3), pickRows(3, 12), skipRows(12, 21), pickRows(21, 24), skipRows(24, 30)},
			},
			expected: []rowRange{
				skipRows(0, 10), pickRows(10, 12), skipRows(12, 23), pickRows(23, 24), skipRows(24, 30),
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			result := intersectSelections(testCase.selections...)
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
