package dataset

import (
	"io"

	"github.com/segmentio/parquet-go"
)

type RowIndexedPages interface {
	io.Closer
	parquet.PageReader
	CurrentRowIndex() int64
	PageOffset(i int64) int64
	NumPages() int64
}

type pageSelection struct {
	pageOffset int64
	rowRange   rowRange
}

type selectedPages struct {
	currentRowIndex       int64
	currentSelectionIndex int64
	selected              []pageSelection
	pages                 parquet.Pages
}

func SelectPages(chunk parquet.ColumnChunk, selection SelectionResult) RowIndexedPages {
	ranges := selection.ranges
	if len(ranges) == 0 {
		return &emptyPageSelection{}
	}

	iRange := 0
	iPages := 0
	selected := make([]pageSelection, 0)
	for iPages < chunk.OffsetIndex().NumPages() && iRange < len(ranges) {

		currentPage := getCurrentPage(iPages, chunk.OffsetIndex(), chunk.NumValues())
		currentRange := ranges[iRange]
		if currentRange.overlaps(currentPage.rowRange) {
			selected = append(selected, pageSelection{
				pageOffset: currentPage.pageOffset,
				rowRange:   currentPage.rowRange.intersect(currentRange.rowRange),
			})
		}

		if ranges[iRange].before(currentPage.rowRange) {
			iRange++
		} else {
			iPages++
		}
	}
	if len(selected) == 0 {
		return &emptyPageSelection{}
	}

	return &selectedPages{
		pages:    chunk.Pages(),
		selected: selected,
	}
}

func (p *selectedPages) ReadPage() (parquet.Page, error) {
	if p.currentSelectionIndex == int64(len(p.selected)) {
		return nil, io.EOF
	}
	var pageRows rowRange
	pageRows = p.selected[p.currentSelectionIndex].rowRange
	if pageRows.from > p.currentRowIndex {
		err := p.pages.SeekToRow(pageRows.from)
		if err != nil {
			return nil, err
		}
	}

	page, err := p.pages.ReadPage()
	if err != nil {
		return nil, err
	}
	p.currentRowIndex = pageRows.from
	p.currentSelectionIndex++

	tail := page.Slice(0, pageRows.length())
	parquet.Release(page)

	return tail, nil
}

func (p *selectedPages) CurrentRowIndex() int64 {
	return p.currentRowIndex
}

func (p *selectedPages) PageOffset(i int64) int64 {
	return p.selected[i].pageOffset
}

func (p *selectedPages) NumPages() int64 {
	return int64(len(p.selected))
}

func (p *selectedPages) Close() error {
	return p.pages.Close()
}

func getCurrentPage(iPages int, offsetIndex parquet.OffsetIndex, numRows int64) pageSelection {
	firstRowIndex := offsetIndex.FirstRowIndex(iPages)
	var lastRowIndex int64
	if iPages < offsetIndex.NumPages()-1 {
		lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
	} else {
		lastRowIndex = numRows
	}
	return pageSelection{
		pageOffset: offsetIndex.Offset(iPages),
		rowRange:   rowRange{from: firstRowIndex, to: lastRowIndex},
	}
}

type emptyPageSelection struct{}

func (e emptyPageSelection) ReadPage() (parquet.Page, error) { return nil, io.EOF }

func (e emptyPageSelection) NumPages() int64          { return 0 }
func (e emptyPageSelection) CurrentRowIndex() int64   { return 0 }
func (e emptyPageSelection) PageOffset(_ int64) int64 { return 0 }
func (e emptyPageSelection) Close() error             { return nil }
