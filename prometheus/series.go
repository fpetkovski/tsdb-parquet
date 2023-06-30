package prometheus

import (
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"Shopify/thanos-parquet-engine/compute"
)

const chunksChannelSize = 5

type seriesSet struct {
	chunksPlan compute.Fragment
	labelsPlan compute.Fragment

	currentBatch  compute.Batch
	currentRow    int
	currentLabels labels.Labels
	err           error

	chunkChannels map[int64]chan chunkenc.Chunk
}

func newSeriesSet(labelNames []string, labelsProjection compute.Fragment, chunksProjection compute.Fragment) *seriesSet {
	lbls := make(labels.Labels, len(labelNames))
	for i, name := range labelNames {
		lbls[i].Name = name
	}
	return &seriesSet{
		currentLabels: lbls,
		labelsPlan:    labelsProjection,
		chunksPlan:    chunksProjection,

		chunkChannels: make(map[int64]chan chunkenc.Chunk),
	}
}

func (s *seriesSet) Next() bool {
	s.currentRow++
	if s.currentBatch == nil || s.currentRow == len(s.currentBatch[0]) {
		s.err = s.nextBatch()
		if s.err == io.EOF {
			return false
		}
		if s.err != nil {
			return false
		}
	}

	return s.currentRow < len(s.currentBatch[0])
}

func (s *seriesSet) nextBatch() error {
	var err error
	s.currentBatch, err = s.labelsPlan.NextBatch()
	if err != nil {
		return err
	}

	for _, seriesVal := range s.currentBatch[0] {
		seriesID := seriesVal.Int64()
		_, channelExists := s.chunkChannels[seriesID]
		if !channelExists {
			s.chunkChannels[seriesID] = make(chan chunkenc.Chunk, chunksChannelSize)
		}
	}

	s.currentRow = 0
	return nil
}

func (s *seriesSet) At() storage.Series {
	for i, col := range s.currentBatch {
		s.currentLabels[i].Value = col[s.currentRow].String()
	}
	return &series{
		labels: s.currentLabels,
	}
}

func (s *seriesSet) Err() error {
	return s.err
}

func (s *seriesSet) Warnings() storage.Warnings { return nil }

type series struct {
	labels labels.Labels
	chunks chan chunkenc.Chunk
}

func (s series) Labels() labels.Labels {
	return s.labels
}

func (s series) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return &chunksIterator{
		chunks: s.chunks,
	}
}
