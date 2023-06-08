package dataset

import (
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"

	"fpetkovski/prometheus-parquet/schema"
)

type matcher struct {
	columnIndex int
	Value       parquet.Value
}

type rowFilter struct {
	matchers     []matcher
	maxt         int64
	mint         int64
	columnsIndex map[string]int
}

func newRowFilter(columnsIndex map[string]int, mint int64, maxt int64, labelMatchers []*labels.Matcher) *rowFilter {
	matchers := make([]matcher, 0, len(labelMatchers))
	for _, m := range labelMatchers {
		columnIndex := columnsIndex[m.Name]
		matchers = append(matchers, matcher{
			columnIndex: columnIndex,
			Value:       parquet.ByteArrayValue([]byte(m.Value)),
		})
	}

	return &rowFilter{
		mint:         mint,
		maxt:         maxt,
		columnsIndex: columnsIndex,
		matchers:     matchers,
	}
}

func (p *rowFilter) Matches(group parquet.RowGroup) (bool, error) {
	chunks := group.ColumnChunks()

	mintColumn := p.columnsIndex[schema.MinTColumn]
	mintColumnIndex := group.ColumnChunks()[mintColumn].ColumnIndex()
	maxtColumn := p.columnsIndex[schema.MaxTColumn]
	maxtColumnIndex := group.ColumnChunks()[maxtColumn].ColumnIndex()

	isInBounds := inBoundsInt64(mintColumnIndex, maxtColumnIndex, p.mint, p.maxt)
	if !isInBounds {
		return false, nil
	}

	for _, filter := range p.matchers {
		chunk := chunks[filter.columnIndex]
		//ok, err := chunk.BloomFilter().Check(filter.Value)
		//if err != nil {
		//	return false, err
		//}
		//if !ok {
		//	return false, nil
		//}

		inPage, err := p.pageContains(chunk, filter.Value)
		if err != nil {
			return false, err
		}
		if !inPage {
			return false, nil
		}
	}
	return true, nil
}

func (p *rowFilter) pageContains(chunk parquet.ColumnChunk, value parquet.Value) (bool, error) {
	for i := 0; i < chunk.ColumnIndex().NumPages(); i++ {
		min := chunk.ColumnIndex().MinValue(i).String()
		max := chunk.ColumnIndex().MaxValue(i).String()
		if min <= value.String() && value.String() <= max {
			return true, nil
		}
	}
	return false, nil
}

func inBoundsInt64(minColumn, maxColumn parquet.ColumnIndex, minValue, maxValue int64) bool {
	var minBound int64 = math.MaxInt64
	var maxBound int64 = math.MinInt

	for i := 0; i < minColumn.NumPages(); i++ {
		min := minColumn.MinValue(i).Int64()
		if min < minBound {
			minBound = min
		}
	}
	for i := 0; i < maxColumn.NumPages(); i++ {
		max := maxColumn.MaxValue(i).Int64()
		if max > maxBound {
			maxBound = max
		}
	}
	//     |-----|      |-----|
	//  *                 *
	//             *        *
	//        *    *
	// * *
	//                           *    *
	outOfBounds := minValue > maxBound || maxValue < minBound
	return !outOfBounds
}
