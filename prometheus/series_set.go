package prometheus

import (
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"Shopify/thanos-parquet-engine/compute"
)

type seriesSet struct {
	labelsPlan compute.Fragment

	currentBatch  compute.Batch
	currentRow    int
	currentLabels labels.Labels
	err           error
}

func newSeriesSet(labelNames []string, labelsProjection compute.Fragment) *seriesSet {
	lbls := make(labels.Labels, len(labelNames))
	for i, name := range labelNames {
		lbls[i].Name = name
	}
	return &seriesSet{
		currentLabels: lbls,
		labelsPlan:    labelsProjection,
	}
}

func (s *seriesSet) Next() bool {
	s.currentRow++
	if s.currentBatch == nil || s.currentRow == len(s.currentBatch[0]) {
		err := s.nextBatch()
		if err == io.EOF {
			return false
		}
		if s.err != nil {
			s.err = err
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

	s.currentRow = 0
	return nil
}

func (s *seriesSet) At() storage.Series {
	numCols := len(s.currentBatch)
	for iCol := 0; iCol < numCols; iCol++ {
		s.currentLabels[iCol].Value = s.currentBatch[iCol][s.currentRow].String()
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
	return &constValIterator{}
}
