package prometheus

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type series struct {

}

func (s series) Labels() labels.Labels {
	//TODO implement me
	panic("implement me")
}

func (s series) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	//TODO implement me
	panic("implement me")
}
