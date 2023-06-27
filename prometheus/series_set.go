package prometheus

import (
	"github.com/prometheus/prometheus/storage"

	"fpetkovski/tsdb-parquet/dataset"
)

type seriesSet struct {
	plan dataset.ExecFragment
}

func newSeriesSet(scanner *dataset.Scanner) *seriesSet {
	return &seriesSet{}
}

func (s seriesSet) Next() bool {
	//TODO implement me
	panic("implement me")
}

func (s seriesSet) At() storage.Series {
	//TODO implement me
	panic("implement me")
}

func (s seriesSet) Err() error {
	//TODO implement me
	panic("implement me")
}

func (s seriesSet) Warnings() storage.Warnings { return nil }
