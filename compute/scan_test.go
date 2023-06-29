package compute

import (
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"

	"fpetkovski/tsdb-parquet/dataset"
	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/pqtest"
)

func TestScan(t *testing.T) {
	cases := []struct {
		name           string
		parts          [][]pqtest.Row
		predicates     []ScannerOption
		expectedRanges []dataset.PickRange
	}{
		{
			//
			//	pages:      |_____|
			//	selection:    |_|
			name: "single page, single predicate",
			parts: [][]pqtest.Row{{
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val1", "val2"),
				pqtest.TwoColumnRow("val1", "val3"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expectedRanges: []dataset.PickRange{dataset.Pick(1, 2)},
		},
		{
			//
			//	pages:      |_____||_____||_____|
			//	selection:           |_|
			name: "single row selection",
			parts: [][]pqtest.Row{{
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val1", "val2"),
				pqtest.TwoColumnRow("val1", "val3"),
			}, {
				pqtest.TwoColumnRow("val2", "val4"),
				pqtest.TwoColumnRow("val2", "val5"),
				pqtest.TwoColumnRow("val2", "val6"),
			}, {
				pqtest.TwoColumnRow("val3", "val1"),
				pqtest.TwoColumnRow("val3", "val2"),
				pqtest.TwoColumnRow("val3", "val3"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnA", "val2"),
				Equals("ColumnB", "val5"),
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val1"))),
			},
			expectedRanges: []dataset.PickRange{dataset.Pick(4, 5)},
		},
		{
			//
			//	pages:      |_____|
			//	selection:  |_| |_|
			name: "multiple selections within a single page",
			parts: [][]pqtest.Row{{
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val1", "val2"),
				pqtest.TwoColumnRow("val2", "val1"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val1"),
			},
			expectedRanges: []dataset.PickRange{dataset.Pick(0, 1), dataset.Pick(2, 3)},
		},
		{
			//
			//	pages:      |_____||_____|
			//	selection:      |___|
			name: "multi row selection",
			parts: [][]pqtest.Row{{
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val1", "val2"),
				pqtest.TwoColumnRow("val2", "val2"),
			}, {
				pqtest.TwoColumnRow("val2", "val2"),
				pqtest.TwoColumnRow("val2", "val2"),
				pqtest.TwoColumnRow("val2", "val3"),
			}},
			predicates: []ScannerOption{
				GreaterThanOrEqual("ColumnA", parquet.ByteArrayValue([]byte("val2"))),
				Equals("ColumnB", "val2"),
			},
			expectedRanges: []dataset.PickRange{dataset.Pick(3, 6)},
		},
		{
			//
			//	pages:      |_____||_____||_____|
			//	selection:       |___| |___| |__|
			name: "multiple disjoint rows",
			parts: [][]pqtest.Row{{
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val2", "val2"),
				pqtest.TwoColumnRow("val2", "val2"),
			}, {
				pqtest.TwoColumnRow("val3", "val1"),
				pqtest.TwoColumnRow("val3", "val3"),
				pqtest.TwoColumnRow("val3", "val2"),
			}, {
				pqtest.TwoColumnRow("val3", "val2"),
				pqtest.TwoColumnRow("val3", "val1"),
				pqtest.TwoColumnRow("val3", "val2"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expectedRanges: []dataset.PickRange{dataset.Pick(1, 3), dataset.Pick(5, 7), dataset.Pick(8, 9)},
		},
		{
			//
			//	pages:      |__||______||_______||__|
			//	selection:           |___| |___|  |_|
			name: "different page sizes",
			parts: [][]pqtest.Row{{
				pqtest.TwoColumnRow("val0", "val0"),
			}, {
				pqtest.TwoColumnRow("val1", "val1"),
				pqtest.TwoColumnRow("val2", "val1"),
				pqtest.TwoColumnRow("val3", "val2"),
			}, {
				pqtest.TwoColumnRow("val4", "val2"),
				pqtest.TwoColumnRow("val5", "val2"),
				pqtest.TwoColumnRow("val6", "val3"),
				pqtest.TwoColumnRow("val7", "val3"),
			}, {
				pqtest.TwoColumnRow("val8", "val2"),
				pqtest.TwoColumnRow("val9", "val2"),
				pqtest.TwoColumnRow("val9", "val3"),
			}, {
				pqtest.TwoColumnRow("val9", "val2"),
			}},
			predicates: []ScannerOption{
				Equals("ColumnB", "val2"),
			},
			expectedRanges: []dataset.PickRange{dataset.Pick(3, 6), dataset.Pick(8, 10), dataset.Pick(11, 12)},
		},
	}

	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			pqFile, err := pqtest.CreateFile(tcase.parts)

			scanner := NewScanner(pqFile, &nopSectionLoader{}, tcase.predicates...)
			rowRanges, err := scanner.Select()
			require.NoError(t, err)

			for _, rowGroup := range pqFile.RowGroups() {
				expected := dataset.NewSelectionResult(rowGroup, tcase.expectedRanges)
				require.Equal(t, expected, rowRanges[0])
			}
		})
	}
}

type nopSectionLoader struct{}

func (n nopSectionLoader) NewSectionSize(_, _, _ int64) (db.Section, error) {
	return emptySection{}, nil
}

func (n nopSectionLoader) NewSection(_, _ int64) (db.Section, error) {
	return emptySection{}, nil
}

type emptySection struct{}

func (n emptySection) LoadNext() error { return nil }
func (n emptySection) LoadAll() error  { return nil }
func (n emptySection) Close() error    { return nil }
