package prometheus

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/segmentio/parquet-go"

	"fpetkovski/tsdb-parquet/db"
)

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
	//opts := []dataset.ScannerOption{
	//	dataset.GreaterThanOrEqual(schema.MinTColumn, parquet.Int64Value(q.mint)),
	//	dataset.LessThanOrEqual(schema.MinTColumn, parquet.Int64Value(q.mint)),
	//}
	//for _, m := range matchers {
	//	opts = append(opts, dataset.Equals(m.Name, m.Value))
	//}
	//
	//scanner := dataset.NewScanner(q.file, q.sectionLoader, opts...)
	//selection, err := scanner.Select()
	//if err != nil {
	//	return storage.ErrSeriesSet(err)
	//}
	//projectionColumns := append([]string{schema.SeriesIDColumn}, hints.Grouping...)
	//projection := dataset.ProjectColumns(selection[0], q.sectionLoader, projectionColumns...)
	//labelValues, err := projection.ReadColumnRanges()
	//if err != nil {
	//	return storage.ErrSeriesSet(err)
	//
	//}
	//return &seriesSet{
	//	scanner: scanner,
	//}
	return nil
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
