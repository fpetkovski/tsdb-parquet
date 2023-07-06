package compact

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	gcsStorage "cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

type ThanosMetaFetcher struct {
	bucket string
	client *gcsStorage.Client
}

func NewThanosMetaFetcher(client *gcsStorage.Client, bucket string) *ThanosMetaFetcher {
	return &ThanosMetaFetcher{
		bucket: bucket,
		client: client,
	}
}

func (f *ThanosMetaFetcher) FetchMetas(ctx context.Context, minTimestamp int64, tenant string) ([]metadata.Meta, error) {
	metaPaths, err := f.listMetas(ctx, minTimestamp)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	var matchingMetas []metadata.Meta

	var errGroup errgroup.Group
	errGroup.SetLimit(50)
	for _, metaPath := range metaPaths {
		metaPath := metaPath
		errGroup.Go(func() error {
			meta, err := f.readMeta(ctx, f.bucket, metaPath)
			if err == gcsStorage.ErrObjectNotExist {
				fmt.Println("File not found:", metaPath)
				return nil
			}
			if err != nil {
				return err
			}

			if meta.Thanos.Labels["k8s_cluster"] == tenant && meta.Thanos.Source == "compactor" && meta.Thanos.Downsample.Resolution == 0 && meta.Compaction.Level == 2 {
				mu.Lock()
				matchingMetas = append(matchingMetas, meta)
				mu.Unlock()
			}
			return nil
		})
	}

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}
	return matchingMetas, nil
}

func (f *ThanosMetaFetcher) listMetas(ctx context.Context, minTimestamp int64) ([]string, error) {
	metaFiles := make([]string, 0)

	// https://pkg.go.dev/cloud.google.com/go/storage@v1.28.1#Query.SetAttrSelection
	query := &gcsStorage.Query{}
	if err := query.SetAttrSelection([]string{"Name", "Updated"}); err != nil {
		return nil, err
	}

	it := f.client.Bucket(f.bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if attrs.Updated.Unix() < minTimestamp {
			continue
		}
		if !strings.HasSuffix(attrs.Name, "/meta.json") {
			continue
		}

		metaFiles = append(metaFiles, attrs.Name)
	}
	return metaFiles, nil
}

func (f *ThanosMetaFetcher) readMeta(ctx context.Context, bucket, object string) (metadata.Meta, error) {
	readClient, err := f.client.Bucket(bucket).Object(object).Retryer(gcsStorage.WithBackoff(gax.Backoff{
		Initial:    2 * time.Second,
		Max:        300 * time.Second,
		Multiplier: 3,
	})).NewReader(ctx)
	if err != nil {
		return metadata.Meta{}, fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer readClient.Close()

	data, err := io.ReadAll(readClient)
	if err != nil {
		return metadata.Meta{}, fmt.Errorf("io.ReadAll: %w", err)
	}
	meta := metadata.Meta{}
	if err := json.Unmarshal(data, &meta); err != nil {
		return metadata.Meta{}, err
	}

	return meta, nil
}
