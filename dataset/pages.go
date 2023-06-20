package dataset

import (
	"io"

	"github.com/segmentio/parquet-go"
)

type RowIndexedPages interface {
	ReadPage() (parquet.Page, int64, error)
	OffsetRange() (int64, int64)
	io.Closer
}

type pageSelection struct {
	offset   int64
	rowRange rowRange
}

type selectedPages struct {
	currentRowIndex int64
	index           []pageSelection
	pages           parquet.Pages
}

func SelectPages(chunk parquet.ColumnChunk, ranges SelectionResult) RowIndexedPages {
	if len(ranges) == 0 {
		return &emptyPageSelection{}
	}

	iRange := 0
	iPages := 0
	index := make([]pageSelection, 0)
	for iPages < chunk.OffsetIndex().NumPages() && iRange < len(ranges) {
		pageRange := getPageSelection(iPages, chunk.OffsetIndex(), chunk.NumValues())
		if ranges[iRange].overlaps(pageRange.rowRange) {
			index = append(index, pageSelection{
				offset: pageRange.offset,
				rowRange: rowRange{
					from: maxInt64(ranges[iRange].from, pageRange.rowRange.from),
					to:   minInt64(ranges[iRange].to, pageRange.rowRange.to),
				},
			})
		}

		if ranges[iRange].before(pageRange.rowRange) {
			iRange++
		} else {
			iPages++
		}
	}
	if len(index) == 0 {
		return &emptyPageSelection{}
	}

	return &selectedPages{
		pages: chunk.Pages(),
		index: index,
	}
}

func (p *selectedPages) ReadPage() (parquet.Page, int64, error) {
	if len(p.index) == 0 {
		return nil, 0, io.EOF
	}
	pageRows := p.index[0].rowRange
	p.index = p.index[1:]

	if pageRows.from > p.currentRowIndex {
		err := p.pages.SeekToRow(pageRows.from)
		if err != nil {
			return nil, 0, err
		}
	}

	page, err := p.pages.ReadPage()
	if err != nil {
		return nil, 0, err
	}
	p.currentRowIndex += pageRows.length()

	return newPageSlice(page, 0, pageRows.length()), pageRows.from, nil
}

func (p *selectedPages) OffsetRange() (int64, int64) {
	return p.index[0].offset, p.index[len(p.index)-1].offset
}

func (p *selectedPages) Close() error {
	return p.pages.Close()
}

type pageSlice struct {
	parquet.Page
	original parquet.Page
}

func newPageSlice(page parquet.Page, from, to int64) pageSlice {
	return pageSlice{
		original: page,
		Page:     page.Slice(from, to),
	}
}

func (s pageSlice) Release() {
	parquet.Release(s.original)
	parquet.Release(s.Page)
}

func getPageSelection(iPages int, offsetIndex parquet.OffsetIndex, numRows int64) pageSelection {
	firstRowIndex := offsetIndex.FirstRowIndex(iPages)
	var lastRowIndex int64
	if iPages < offsetIndex.NumPages()-1 {
		lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
	} else {
		lastRowIndex = numRows
	}
	return pageSelection{
		offset:   offsetIndex.Offset(iPages),
		rowRange: rowRange{from: firstRowIndex, to: lastRowIndex},
	}
}

type emptyPageSelection struct{}

func (e emptyPageSelection) ReadPage() (parquet.Page, int64, error) { return nil, 0, io.EOF }

func (e emptyPageSelection) SeekToRow(i int64) error     { return io.EOF }
func (e emptyPageSelection) OffsetRange() (int64, int64) { return 0, 0 }
func (e emptyPageSelection) Close() error                { return nil }
