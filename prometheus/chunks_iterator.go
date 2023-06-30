package prometheus

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type constValIterator struct {
	ts int64
}

func (c *constValIterator) Next() chunkenc.ValueType {
	c.ts += 30
	return chunkenc.ValFloat
}

func (c *constValIterator) Seek(t int64) chunkenc.ValueType {
	c.ts = t
	return chunkenc.ValFloat
}

func (c *constValIterator) At() (int64, float64) {
	return c.ts, 1
}

func (c *constValIterator) AtHistogram() (int64, *histogram.Histogram) {
	return 0, nil
}

func (c *constValIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (c *constValIterator) AtT() int64 {
	return c.ts
}

func (c *constValIterator) Err() error {
	return nil
}
