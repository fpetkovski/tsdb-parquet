package dataset

import "golang.org/x/exp/slices"

type rowRange struct {
	from int64
	to   int64
}

func (r rowRange) length() int64 {
	return r.to - r.from
}

func (r rowRange) before(other rowRange) bool {
	if r.to == other.to {
		return r.from < other.from
	}
	return r.to < other.to
}

func (r rowRange) overlaps(other rowRange) bool {
	disjoint := r.to <= other.from || other.to <= r.from
	return !disjoint
}

type skipRange struct {
	rowRange
}

func skip(from, to int64) skipRange {
	return skipRange{rowRange{from: from, to: to}}
}

func (s skipRange) merge(other skipRange) RowSelection {
	if s.overlaps(other.rowRange) {
		return RowSelection{s.union(other)}
	}

	return RowSelection{s, other}
}

func (s skipRange) union(other skipRange) skipRange {
	return skip(
		minInt64(s.from, other.from),
		maxInt64(s.to, other.to),
	)
}

type RowSelection []skipRange

func SelectAll() RowSelection {
	return RowSelection{}
}

func (r RowSelection) Skip(from, to int64) RowSelection {
	if from == to {
		return r
	}
	return append(r, skip(from, to))
}

func SelectRows(numRows int64, skips ...RowSelection) SelectionResult {
	if len(skips) == 0 {
		return SelectionResult{pick(0, numRows)}
	}

	allRanges := make(RowSelection, 0, len(skips))
	for _, selection := range skips {
		allRanges = append(allRanges, selection...)
	}
	if len(allRanges) == 0 {
		return SelectionResult{pick(0, numRows)}
	}
	slices.SortFunc(allRanges, func(a, b skipRange) bool {
		return a.before(b.rowRange)
	})

	merged := mergeOverlappingRanges(allRanges)
	return invertSkips(numRows, merged)
}

type pickRange struct {
	rowRange
}

func pick(from, to int64) pickRange {
	return pickRange{rowRange{from: from, to: to}}
}

type SelectionResult []pickRange

func (s SelectionResult) NumRows() int64 {
	var numRows int64
	for _, r := range s {
		numRows += r.length()
	}
	return numRows
}

func mergeOverlappingRanges(allRanges []skipRange) RowSelection {
	merged := RowSelection{allRanges[0]}
	allRanges = allRanges[1:]

	for _, r := range allRanges {
		last := merged[len(merged)-1]
		merged = merged[:len(merged)-1]
		merged = append(merged, last.merge(r)...)
	}
	return merged
}

func invertSkips(numRows int64, skips RowSelection) SelectionResult {
	result := make(SelectionResult, 0, len(skips))
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
