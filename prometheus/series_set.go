package prometheus

import (
	"github.com/prometheus/prometheus/storage"

	"fpetkovski/tsdb-parquet/compute"
)

type seriesSet struct {
	plan compute.Fragment
}

func newSeriesSet(scanner *compute.Scanner) *seriesSet {
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
