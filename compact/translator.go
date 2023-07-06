package compact

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"golang.org/x/exp/maps"

	"Shopify/thanos-parquet-engine/db"
	"Shopify/thanos-parquet-engine/schema"
)

const ParquetDir = "parquet"

type Translator struct {
	dataDir       string
	tsdbBucket    objstore.Bucket
	parquetBucket objstore.Bucket
	logger        log.Logger
}

func NewTranslator(logger log.Logger, tsdbBucket, parquetBucket objstore.Bucket, dataDir string) *Translator {
	return &Translator{
		logger:        logger,
		tsdbBucket:    tsdbBucket,
		parquetBucket: parquetBucket,
		dataDir:       dataDir,
	}
}

func (t *Translator) TranslateBlock(ctx context.Context, meta metadata.Meta) (translateErr error) {
	srcDir := path.Join(t.dataDir, meta.ULID.String())
	defer func() {
		if translateErr == nil {
			os.RemoveAll(srcDir)
		}
	}()

	level.Info(t.logger).Log("msg", "downloading block", "block", meta.ULID)
	if err := block.Download(ctx, t.logger, t.tsdbBucket, meta.ULID, srcDir, objstore.WithFetchConcurrency(10)); err != nil {
		return err
	}

	level.Info(t.logger).Log("msg", "converting block", "block", meta.ULID)
	if err := t.convertToParquet(meta); err != nil {
		return err
	}

	level.Info(t.logger).Log("msg", "uploading parquet files", "block", meta.ULID)
	if err := t.uploadParquetBlock(ctx, meta); err != nil {
		return err
	}
	return nil
}

func (t *Translator) convertToParquet(meta metadata.Meta) error {
	dstDir := path.Join(t.dataDir, meta.ULID.String(), ParquetDir)
	if err := os.Mkdir(dstDir, 0750); err != nil {
		return err
	}
	tsdbBlock, block, err := openBlock(t.dataDir, meta.ULID.String())
	if err != nil {
		return err
	}
	defer tsdbBlock.Close()

	blockQuerier, err := tsdb.NewBlockChunkQuerier(block, math.MinInt64, math.MaxInt64)
	defer blockQuerier.Close()
	if err != nil {
		return err
	}

	allLabels, _, err := blockQuerier.LabelNames()
	if err != nil {
		return err
	}
	labelSet := make(map[string]struct{})
	for _, lbl := range allLabels {
		labelSet[strings.ToLower(lbl)] = struct{}{}
	}
	allLabels = maps.Keys(labelSet)

	chunkReader, err := block.Chunks()
	if err != nil {
		return err
	}
	defer chunkReader.Close()

	ir, err := block.Index()
	if err != nil {
		return err
	}
	defer ir.Close()

	writer := db.NewWriter(dstDir, allLabels)
	defer writer.Close()

	ps, err := ir.Postings(index.AllPostingsKey())
	if err != nil {
		return err
	}
	ps = ir.SortedPostings(ps)
	var (
		lblBuilder labels.ScratchBuilder
		chks       []chunks.Meta
		seriesID   int64 = -1
	)
	for ps.Next() {
		seriesID++
		lblBuilder.Reset()
		if err := ir.Series(ps.At(), &lblBuilder, &chks); err != nil {
			return err
		}

		lbls := lblBuilder.Labels()
		lblsMap := make(map[string]string, lbls.Len())
		lbls.Range(func(lbl labels.Label) {
			lblsMap[strings.ToLower(lbl.Name)] = lbl.Value
		})
		for _, chunkMeta := range chks {
			chk, err := chunkReader.Chunk(chunkMeta)
			if err != nil {
				return err
			}

			chunk := schema.Chunk{
				SeriesID:   seriesID,
				Labels:     lblsMap,
				MinT:       chunkMeta.MinTime,
				MaxT:       chunkMeta.MaxTime,
				ChunkBytes: chk.Bytes(),
			}
			if err := writer.Write(chunk); err != nil {
				return err
			}
		}
	}

	if err := writer.Flush(); err != nil {
		return err
	}

	if err := writer.Compact(); err != nil {
		return err
	}

	return nil
}

func (t *Translator) uploadParquetBlock(ctx context.Context, dir metadata.Meta) error {
	parquetDir := path.Join(t.dataDir, dir.ULID.String(), ParquetDir)
	files := []string{
		"compact.parquet",
		"compact.metadata",
	}
	for _, file := range files {
		localPath := path.Join(parquetDir, file)
		f, err := os.Open(localPath)
		if err != nil {
			return err
		}
		defer f.Close()

		remotePath := path.Join(dir.ULID.String(), file)
		if err := t.parquetBucket.Upload(ctx, remotePath, f); err != nil {
			return err
		}
	}
	return nil
}

func openBlock(path string, blockID string) (*tsdb.DBReadOnly, tsdb.BlockReader, error) {
	db, err := tsdb.OpenDBReadOnly(path, nil)
	if err != nil {
		return nil, nil, err
	}
	blocks, err := db.Blocks()
	if err != nil {
		return nil, nil, err
	}
	var block tsdb.BlockReader
	if blockID != "" {
		for _, b := range blocks {
			if b.Meta().ULID.String() == blockID {
				block = b
				break
			}
		}
	} else if len(blocks) > 0 {
		block = blocks[len(blocks)-1]
	}
	if block == nil {
		return nil, nil, fmt.Errorf("block %s not found", blockID)
	}
	return db, block, nil
}
