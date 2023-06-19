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

type pageIndex struct {
	offset   int64
	rowRange rowRange
}

type selectedPages struct {
	currentRowIndex int64
	index           []pageIndex
	pages           parquet.Pages
}

func SelectPages(offsetIndex parquet.OffsetIndex, numRows int64, pages parquet.Pages, ranges SelectionResult) RowIndexedPages {
	if len(ranges) == 0 {
		return &emptyPageSelection{}
	}

	iRange := 0
	iPages := 0
	index := make([]pageIndex, 0)
	for iPages < offsetIndex.NumPages() && iRange < len(ranges) {
		pageRange := getPageIndex(offsetIndex, numRows, iPages)
		if ranges[iRange].overlaps(pageRange.rowRange) {
			index = append(index, pageRange)
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
		pages: pages,
		index: index,
	}
}

func (p *selectedPages) ReadPage() (parquet.Page, int64, error) {
	if len(p.index) == 0 {
		return nil, 0, io.EOF
	}
	rowInPage := p.index[0].rowRange.from
	p.index = p.index[1:]

	if rowInPage > p.currentRowIndex {
		err := p.pages.SeekToRow(rowInPage)
		if err != nil {
			return nil, 0, err
		}
	}

	page, err := p.pages.ReadPage()
	if err != nil {
		return nil, 0, err
	}
	p.currentRowIndex += page.NumValues()

	return page, rowInPage, nil
}

func (p *selectedPages) OffsetRange() (int64, int64) {
	return p.index[0].offset, p.index[len(p.index)-1].offset
}

func (p *selectedPages) Close() error {
	return p.pages.Close()
}

func getPageIndex(offsetIndex parquet.OffsetIndex, numRows int64, iPages int) pageIndex {
	firstRowIndex := offsetIndex.FirstRowIndex(iPages)
	var lastRowIndex int64
	if iPages < offsetIndex.NumPages()-1 {
		lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
	} else {
		lastRowIndex = numRows
	}
	return pageIndex{
		offset:   offsetIndex.Offset(iPages),
		rowRange: rowRange{from: firstRowIndex, to: lastRowIndex},
	}
}

type emptyPageSelection struct{}

func (e emptyPageSelection) ReadPage() (parquet.Page, int64, error) { return nil, 0, io.EOF }

func (e emptyPageSelection) SeekToRow(i int64) error     { return io.EOF }
func (e emptyPageSelection) OffsetRange() (int64, int64) { return 0, 0 }
func (e emptyPageSelection) Close() error                { return nil }
