package prometheus

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/compute"
	"fpetkovski/tsdb-parquet/db"
	"fpetkovski/tsdb-parquet/schema"
)

const seriesBatchSize = 32 * 1024

func NewQuerier(ctx context.Context, file *parquet.File, sectionLoader db.SectionLoader, mint, maxt int64) (storage.Querier, error) {
	return storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
		return &querier{
			ctx:  ctx,
			mint: mint,
			maxt: maxt,

			file:          file,
			sectionLoader: sectionLoader,
		}, nil
	})(ctx, mint, maxt)
}

type querier struct {
	ctx  context.Context
	mint int64
	maxt int64

	file          *parquet.File
	sectionLoader db.SectionLoader
}

func (q querier) Select(_ bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	opts := []compute.ScannerOption{
		compute.GreaterThanOrEqual(schema.MinTColumn, parquet.Int64Value(q.mint)),
		compute.LessThanOrEqual(schema.MaxTColumn, parquet.Int64Value(q.maxt)),
	}
	for _, m := range matchers {
		opts = append(opts, compute.Equals(m.Name, m.Value))
	}

	scanner := compute.NewScanner(q.file, q.sectionLoader, opts...)
	selection, err := scanner.Select()
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	projectionColumns := append([]string{schema.SeriesIDColumn}, hints.Grouping...)
	plan := compute.UniqueByColumn(0, compute.ProjectColumns(
		selection[0],
		q.sectionLoader,
		seriesBatchSize,
		projectionColumns...),
	)

	return newSeriesSet(plan, projectionColumns)
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
