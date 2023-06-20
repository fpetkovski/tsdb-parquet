package prometheus

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func NewQuerier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &querier{ctx: ctx, mint: mint, maxt: maxt}, nil
	})(ctx, mint, maxt)
}

type querier struct {
	ctx context.Context

	mint int64
	maxt int64
}

func (q querier) Select(_ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	//TODO implement me
	panic("implement me")
}

func (q querier) Close() error { return nil }

func (q querier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	//TODO implement me
	panic("implement me")
}

func (q querier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	//TODO implement me
	panic("implement me")
}
