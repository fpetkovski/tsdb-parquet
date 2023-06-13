package dataset

import "golang.org/x/exp/slices"

type matchType int

const (
	matchTypeSkip matchType = iota
	matchTypePick
)

type rowRange interface {
	matchType() matchType
	fromRow() int64
	toRow() int64
	intersect(rowRange) []rowRange
}

func lessRowRange(a, b rowRange) bool {
	if a.fromRow() == b.fromRow() {
		return a.toRow() < b.toRow()
	}

	return a.fromRow() < b.fromRow()
}

type pickRange struct {
	from int64
	to   int64
}

func pickRows(from, to int64) pickRange {
	return pickRange{from: from, to: to}
}
func (p pickRange) matchType() matchType { return matchTypePick }

func (p pickRange) fromRow() int64 { return p.from }

func (p pickRange) toRow() int64 { return p.to }

func (p pickRange) intersect(other rowRange) []rowRange {
	if other.matchType() == matchTypePick {
		from := maxInt64(p.from, other.fromRow())
		to := minInt64(p.to, other.toRow())
		return []rowRange{pickRows(from, to)}
	}

	return []rowRange{pickRows(p.from, other.fromRow()), other}
}

type skipRange struct {
	from int64
	to   int64
}

func (s skipRange) fromRow() int64 { return s.from }

func (s skipRange) toRow() int64 { return s.to }

func (s skipRange) matchType() matchType { return matchTypeSkip }

func (s skipRange) intersect(other rowRange) []rowRange {
	if other.matchType() == matchTypeSkip {
		from := minInt64(s.from, other.fromRow())
		to := maxInt64(s.to, other.toRow())
		return []rowRange{skipRows(from, to)}
	}

	return []rowRange{s, pickRows(s.to, other.toRow())}
}

func skipRows(from, to int64) skipRange {
	return skipRange{from: from, to: to}
}

type rowSelection []rowRange

func intersectSelections(selections ...rowSelection) rowSelection {
	if len(selections) == 0 {
		return nil
	}

	allRanges := make([]rowRange, 0, len(selections))
	for _, selection := range selections {
		allRanges = append(allRanges, selection...)
	}
	if len(allRanges) == 0 {
		return nil
	}
	slices.SortFunc(allRanges, lessRowRange)

	return mergeOverlappingRanges(allRanges)
}

func mergeOverlappingRanges(allRanges []rowRange) rowSelection {
	merged := []rowRange{allRanges[0]}
	allRanges = allRanges[1:]

	for _, r := range allRanges {
		last := merged[len(merged)-1]
		merged = merged[:len(merged)-1]
		merged = append(merged, last.intersect(r)...)
	}
	return merged
}
