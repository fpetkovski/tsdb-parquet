package dataset

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/segmentio/parquet-go"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/stretchr/testify/require"
)

var stringType = parquet.String().Type()

func TestRowIndexedPages(t *testing.T) {
	cases := []struct {
		name      string
		vals      []string
		pageSizes []int
		selection SelectionResult
	}{
		{
			name:      "single page",
			vals:      []string{"val1", "val2", "val3"},
			pageSizes: []int{3},
			selection: SelectionResult{pick(1, 2)},
		},
		{
			name: "multiple pages",
			vals: []string{"val1", "val2",
				"val3",
				"val4", "val5", "val6", "val7",
				"val8", "val9", "val10",
			},
			pageSizes: []int{2, 1, 4, 3},
			selection: SelectionResult{pick(2, 4), pick(6, 8), pick(9, 10)},
		},
	}
	for _, tcase := range cases {
		t.Run(tcase.name, func(t *testing.T) {
			chunk := newMemChunk(tcase.vals, tcase.pageSizes)
			pages := SelectPages(chunk.OffsetIndex(), chunk.NumValues(), chunk.Pages(), tcase.selection)
			for {
				page, cursor, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				values := make([]parquet.Value, page.NumValues())
				_, err = page.Values().ReadValues(values)
				if err != io.EOF {
					require.NoError(t, err)
				}
				fmt.Println(cursor)
				for _, v := range values {
					fmt.Println(v.String())
				}
			}
		})
	}
}

type memPages struct {
	values    []string
	pageSizes []int
	rowIndex  int64
}

func newMemPages(values []string, pageSizes []int) *memPages {
	return &memPages{
		values:    values,
		pageSizes: pageSizes,
	}
}

func (m *memPages) ReadPage() (parquet.Page, error) {
	if len(m.pageSizes) == 0 {
		return nil, io.EOF
	}
	pageSize := m.pageSizes[0]
	m.pageSizes = m.pageSizes[1:]

	values := encodeStrings(m.values[m.rowIndex : int(m.rowIndex)+pageSize])
	m.rowIndex += int64(pageSize)

	return stringType.NewPage(0, pageSize, values), nil
}

func (m *memPages) SeekToRow(i int64) error {
	m.rowIndex = i
	if m.rowIndex >= int64(len(m.values)) {
		return io.EOF
	}

	return nil
}

func (m *memPages) Close() error { return nil }

func encodeStrings(stringVals []string) encoding.Values {
	buffer := bytes.NewBuffer(nil)
	offsets := make([]uint32, len(stringVals)+1)
	for i, v := range stringVals {
		offsets[i] = uint32(buffer.Len())
		buffer.Write([]byte(v))
	}
	offsets[len(stringVals)] = uint32(buffer.Len())

	return stringType.NewValues(buffer.Bytes(), offsets)
}

type memChunk struct {
	values    []string
	pageSizes []int
}

func newMemChunk(values []string, pageSizes []int) *memChunk {
	return &memChunk{values: values, pageSizes: pageSizes}
}

func (m memChunk) Type() parquet.Type {
	return stringType
}

func (m memChunk) Column() int {
	return 0
}

func (m memChunk) Pages() parquet.Pages {
	return newMemPages(m.values, m.pageSizes)
}

func (m memChunk) BloomFilter() parquet.BloomFilter {
	return nil
}

func (m memChunk) NumValues() int64 {
	return int64(len(m.values))
}

func (m memChunk) ColumnIndex() parquet.ColumnIndex {
	panic("not implemented")
}

func (m memChunk) OffsetIndex() parquet.OffsetIndex {
	return memOffsetIndex{
		values:    m.values,
		pageSizes: m.pageSizes,
	}
}

type memOffsetIndex struct {
	values    []string
	pageSizes []int
}

func (m memOffsetIndex) NumPages() int {
	return len(m.pageSizes)
}

func (m memOffsetIndex) Offset(i int) int64 {
	return 0
}

func (m memOffsetIndex) CompressedPageSize(i int) int64 {
	return int64(m.pageSizes[i])
}

func (m memOffsetIndex) FirstRowIndex(i int) int64 {
	size := 0
	for j := 0; j < i; j++ {
		size += m.pageSizes[j]
	}

	return int64(size)
}
