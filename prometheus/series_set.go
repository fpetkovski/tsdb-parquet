package prometheus

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"Shopify/thanos-parquet-engine/compute"
)

type seriesSet struct {
	plan compute.Fragment

	currentBatch  compute.Batch
	currentRow    int
	currentLabels labels.Labels

	err error
}

func newSeriesSet(plan compute.Fragment, labelNames []string) *seriesSet {
	lbls := make(labels.Labels, len(labelNames))
	for i, name := range labelNames {
		lbls[i].Name = name
	}
	return &seriesSet{
		plan:          plan,
		currentLabels: lbls,
	}
}

func (s *seriesSet) Next() bool {
	if s.currentBatch == nil || len(s.currentBatch[0]) == 0 {
		s.err = s.nextBatch()
		if s.err != nil {
			return false
		}
	}

	s.currentRow++
	return s.currentRow < len(s.currentBatch[0])
}

func (s *seriesSet) nextBatch() error {
	var err error
	s.currentBatch, err = s.plan.NextBatch()
	if err != nil {
		return err
	}
	s.currentRow = -1
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
}

func (s series) Labels() labels.Labels {
	return s.labels
}

func (s series) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	//TODO implement me
	panic("implement me")
}
