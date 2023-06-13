package dataset

import (
	"golang.org/x/exp/slices"
)

type rowRange struct {
	from int64
	to   int64
}

func (r rowRange) less(other rowRange) bool {
	if r.from == other.from {
		return r.to < other.to
	}
	return r.from < other.from
}

func (r rowRange) overlaps(other rowRange) bool {
	return r.to > other.from && r.from < other.to
}

type skipRange struct {
	rowRange
}

func skip(from, to int64) skipRange {
	return skipRange{rowRange{from: from, to: to}}
}

func (s skipRange) union(other skipRange) skipRange {
	return skip(
		minInt64(s.from, other.from),
		maxInt64(s.to, other.to),
	)
}

func (s skipRange) intersection(other skipRange) RowSelection {
	if s.overlaps(other.rowRange) {
		return RowSelection{s.union(other)}
	}

	return RowSelection{s, other}
}

type RowSelection []skipRange

type pickRange struct {
	rowRange
}

func pick(from, to int64) pickRange {
	return pickRange{rowRange{from: from, to: to}}
}

type selectionResult []pickRange

func pickRanges(numRows int64, skips ...RowSelection) selectionResult {
	if len(skips) == 0 {
		return selectionResult{pick(0, numRows)}
	}

	allRanges := make(RowSelection, 0, len(skips))
	for _, selection := range skips {
		allRanges = append(allRanges, selection...)
	}
	if len(allRanges) == 0 {
		return selectionResult{pick(0, numRows)}
	}
	slices.SortFunc(allRanges, func(a, b skipRange) bool {
		return a.less(b.rowRange)
	})

	merged := mergeOverlappingRanges(allRanges)
	return invertSkips(numRows, merged)
}

func mergeOverlappingRanges(allRanges []skipRange) RowSelection {
	merged := RowSelection{allRanges[0]}
	allRanges = allRanges[1:]

	for _, r := range allRanges {
		last := merged[len(merged)-1]
		merged = merged[:len(merged)-1]
		merged = append(merged, last.intersection(r)...)
	}
	return merged
}

func invertSkips(numRows int64, skips RowSelection) selectionResult {
	result := make(selectionResult, 0, len(skips))
	fromRow := int64(0)
	for _, s := range skips {
		if s.from > fromRow {
			result = append(result, pick(fromRow, s.from))
		}
		fromRow = s.to
	}
	if fromRow < numRows {
		result = append(result, pick(fromRow, numRows))
	}
	return result
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
