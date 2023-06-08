package dataset

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type chunkSeries struct {
	i      int
	labels labels.Labels
	chunks []chunks.Meta
}

func (c chunkSeries) Labels() labels.Labels {
	//TODO implement me
	panic("implement me")
}

func (c chunkSeries) Iterator(iterator chunks.Iterator) chunks.Iterator {
	//TODO implement me
	panic("implement me")
}

type chunkSet struct {
	i      int
	series []chunkSeries
}

func newChunkSet() *chunkSet {
	return &chunkSet{}
}

func (c chunkSet) Next() bool {
	//TODO implement me
	panic("implement me")
}

func (c chunkSet) At() storage.ChunkSeries {
	//TODO implement me
	panic("implement me")
}

func (c chunkSet) Err() error {
	//TODO implement me
	panic("implement me")
}

func (c chunkSet) Warnings() storage.Warnings {
	//TODO implement me
	panic("implement me")
}
