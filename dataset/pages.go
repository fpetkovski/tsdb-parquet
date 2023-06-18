package dataset

import (
	"io"

	"github.com/segmentio/parquet-go"
)

type RowIndexedPages interface {
	ReadPage() (parquet.Page, int64, error)
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

func SelectPages(chunk parquet.ColumnChunk, ranges SelectionResult) RowIndexedPages {
	if len(ranges) == 0 {
		return &emptyPageSelection{}
	}
	offsetIndex := chunk.OffsetIndex()

	iRange := 0
	iPages := 0
	index := make([]pageIndex, 0)
	for iPages < offsetIndex.NumPages() && iRange < len(ranges) {
		pageRange := getPageIndex(chunk, offsetIndex, iPages)
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
		pages: chunk.Pages(),
		index: index,
	}
}

func (p *selectedPages) ReadPage() (parquet.Page, int64, error) {
	if len(p.index) == 0 {
		return nil, 0, io.EOF
	}
	nextRow := p.index[0].rowRange.from
	p.index = p.index[1:]

	if nextRow > p.currentRowIndex {
		err := p.pages.SeekToRow(nextRow)
		if err != nil {
			return nil, 0, err
		}
	}

	page, err := p.pages.ReadPage()
	if err != nil {
		return nil, 0, err
	}
	p.currentRowIndex += page.NumValues()

	return page, nextRow, nil
}

func (p *selectedPages) Close() error {
	return p.pages.Close()
}

func getPageIndex(chunk parquet.ColumnChunk, offsetIndex parquet.OffsetIndex, iPages int) pageIndex {
	firstRowIndex := offsetIndex.FirstRowIndex(iPages)
	var lastRowIndex int64
	if iPages < offsetIndex.NumPages()-1 {
		lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
	} else {
		lastRowIndex = chunk.NumValues()
	}
	return pageIndex{
		offset:   offsetIndex.Offset(iPages),
		rowRange: rowRange{from: firstRowIndex, to: lastRowIndex},
	}
}

type emptyPageSelection struct{}

func (e emptyPageSelection) ReadPage() (parquet.Page, int64, error) { return nil, 0, io.EOF }
func (e emptyPageSelection) SeekToRow(i int64) error                { return io.EOF }
func (e emptyPageSelection) Close() error                           { return nil }

//// selectPagesRange selects the range of page offsets that contain rows in the given ranges.
//func selectPagesRange(chunk parquet.ColumnChunk, ranges SelectionResult) (rowRange, []int64) {
//	if len(ranges) == 0 {
//		return emptyRange(), nil
//	}
//
//	iRange := 0
//	iPages := 0
//	pageOffsets := make([]int64, 0)
//	offsetIndex := chunk.OffsetIndex()
//	var rows []int64
//	for iPages < offsetIndex.NumPages() && iRange < len(ranges) {
//		firstRowIndex := offsetIndex.FirstRowIndex(iPages)
//		var lastRowIndex int64
//		if iPages < offsetIndex.NumPages()-1 {
//			lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
//		} else {
//			lastRowIndex = chunk.NumValues()
//		}
//		pageRange := rowRange{from: firstRowIndex, to: lastRowIndex}
//		if ranges[iRange].overlaps(pageRange) {
//			pageOffsets = append(pageOffsets, offsetIndex.Offset(iPages))
//			rows = append(rows, firstRowIndex)
//		}
//
//		if ranges[iRange].before(pageRange) {
//			iRange++
//		} else {
//			iPages++
//		}
//	}
//
//	if len(pageOffsets) == 0 {
//		return emptyRange(), nil
//	}
//	return rowRange{from: pageOffsets[0], to: pageOffsets[len(pageOffsets)-1]}, rows
//}
