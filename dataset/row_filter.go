package dataset

import (
	"io"

	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

type RowFilter interface {
	FilterRows(parquet.ColumnChunk, SelectionResult) (RowSelection, error)
}

type matchFunc func(parquet.Value) bool

type rowFilter struct {
	reader  *db.FileReader
	matches func(parquet.Value) bool
}

func NewRowFilter(reader *db.FileReader, matches matchFunc) RowFilter {
	return &rowFilter{
		reader:  reader,
		matches: matches,
	}
}

func (r rowFilter) FilterRows(chunk parquet.ColumnChunk, ranges SelectionResult) (RowSelection, error) {
	pages := chunk.Pages()
	defer pages.Close()

	pageRange := selectPageOffsets(chunk, ranges)
	if err := r.reader.LoadSection(pageRange.from, pageRange.to); err != nil {
		return nil, err
	}

	var numMatches int64
	var selection RowSelection
	for _, rows := range ranges {
		cursor := rows.from
		for cursor < rows.to {
			if err := pages.SeekToRow(cursor); err != nil {
				return nil, err
			}
			page, err := pages.ReadPage()
			if err != nil {
				return nil, err
			}

			numValues := rows.to - cursor
			if numValues > page.NumValues() {
				numValues = page.NumValues()
			}

			values := make([]parquet.Value, numValues)
			n, err := page.Values().ReadValues(values)
			if err != nil && err != io.EOF {
				return nil, err
			}
			skipFrom, skipTo := cursor, cursor
			for i := 0; i < n; i++ {
				skipTo++
				matches := r.matches(values[i])
				if matches {
					numMatches++
					selection = selection.Skip(skipFrom, skipTo-1)
					skipFrom = skipTo
				}
			}
			selection = selection.Skip(skipFrom, skipTo)
			cursor += numValues
		}
	}
	return selection, nil
}

func selectPageOffsets(chunk parquet.ColumnChunk, ranges SelectionResult) rowRange {
	offsetIndex := chunk.OffsetIndex()
	if len(ranges) == 0 {
		return emptyRange()
	}

	pageOffsets := make([]int64, 0)
	iRange := 0
	iPages := 0
	for iPages < offsetIndex.NumPages() && iRange < len(ranges) {
		firstRowIndex := offsetIndex.FirstRowIndex(iPages)
		var lastRowIndex int64
		if iPages < offsetIndex.NumPages()-1 {
			lastRowIndex = offsetIndex.FirstRowIndex(iPages + 1)
		} else {
			lastRowIndex = chunk.NumValues()
		}
		pageRange := rowRange{from: firstRowIndex, to: lastRowIndex}
		if ranges[iRange].overlaps(pageRange) {
			pageOffsets = append(pageOffsets, offsetIndex.Offset(iPages))
		}

		if ranges[iRange].before(pageRange) {
			iRange++
		} else {
			iPages++
		}
	}

	if len(pageOffsets) == 0 {
		return emptyRange()
	}
	return rowRange{from: pageOffsets[0], to: pageOffsets[len(pageOffsets)-1]}
}
