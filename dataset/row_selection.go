package dataset

import (
	"fmt"

	"golang.org/x/exp/slices"
)

type rowRange struct {
	from int64
	to   int64
}

func lessSelectionResult(a, b rowRange) bool {
	if a.from == b.from {
		return a.to < b.to
	}
	return a.from < b.from
}

func (p rowRange) overlaps(other rowRange) bool {
	isDisjoint := p.to <= other.from || p.from >= other.to
	return !isDisjoint
}

type skipRange struct {
	rowRange
}

func skipRows(from, to int64) skipRange {
	return skipRange{rowRange{from: from, to: to}}
}

func (s skipRange) union(other rowRange) rowRange {
	return rowRange{
		from: minInt64(s.from, other.from),
		to:   maxInt64(s.to, other.to),
	}
}

func (s skipRange) String() string {
	return fmt.Sprintf("skip(%d, %d)", s.from, s.to)
}

type predicateResult []skipRange

func (s skipRange) intersect(other skipRange) predicateResult {
	if s.overlaps(other.rowRange) {
		overlap := s.union(other.rowRange)
		return predicateResult{skipRange{overlap}}
	}

	return predicateResult{s, other}
}

type pickRange struct {
	rowRange
}

func pickRows(from, to int64) pickRange {
	return pickRange{rowRange{from: from, to: to}}
}

type selectionResult []pickRange

func intersectSelections(numRows int64, skips ...predicateResult) selectionResult {
	if len(skips) == 0 {
		return selectionResult{pickRows(0, numRows)}
	}

	allRanges := make(predicateResult, 0, len(skips))
	for _, selection := range skips {
		allRanges = append(allRanges, selection...)
	}
	if len(allRanges) == 0 {
		return selectionResult{pickRows(0, numRows)}
	}
	slices.SortFunc(allRanges, func(a, b skipRange) bool {
		return lessSelectionResult(a.rowRange, b.rowRange)
	})

	merged := mergeOverlappingRanges(allRanges)
	return invertSkips(numRows, merged)
}

func invertSkips(numRows int64, skips predicateResult) selectionResult {
	result := make(selectionResult, 0, len(skips))
	fromRow := int64(0)
	for i := 0; i < len(skips); i++ {
		skip := skips[i]
		if skip.from > fromRow {
			result = append(result, pickRows(fromRow, skip.from))
		}
		fromRow = skip.to
	}
	if fromRow < numRows {
		result = append(result, pickRows(fromRow, numRows))
	}
	return result
}

func mergeOverlappingRanges(allRanges []skipRange) predicateResult {
	merged := predicateResult{allRanges[0]}
	allRanges = allRanges[1:]

	for _, r := range allRanges {
		last := merged[len(merged)-1]
		merged = merged[:len(merged)-1]
		merged = append(merged, last.intersect(r)...)
	}
	return merged
}
