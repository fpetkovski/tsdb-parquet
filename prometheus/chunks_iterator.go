package prometheus

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type chunksIterator struct {
	current chunkenc.Iterator
	chunks  chan chunkenc.Chunk
}

func (c *chunksIterator) Next() chunkenc.ValueType {
	nextVal := c.current.Next()
	if nextVal != chunkenc.ValNone {
		return nextVal
	}
	select {
	case nextChunk, ok := <-c.chunks:
		if !ok {
			return chunkenc.ValNone
		}
		c.current = nextChunk.Iterator(c.current)
	}
	return c.current.Next()
}

func (c *chunksIterator) Seek(t int64) chunkenc.ValueType {
	for {
		valType := c.current.Seek(t)
		if valType != chunkenc.ValNone {
			return valType
		}
		select {
		case nextChunk, ok := <-c.chunks:
			if !ok {
				return chunkenc.ValNone
			}
			c.current = nextChunk.Iterator(c.current)
		}
	}
}

func (c *chunksIterator) At() (int64, float64) {
	return c.current.At()
}

func (c *chunksIterator) AtHistogram() (int64, *histogram.Histogram) {
	return c.current.AtHistogram()
}

func (c *chunksIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return c.current.AtFloatHistogram()
}

func (c *chunksIterator) AtT() int64 {
	return c.current.AtT()
}

func (c *chunksIterator) Err() error {
	return c.current.Err()
}
