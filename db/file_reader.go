package db

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v10/parquet/metadata"
	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"Shopify/thanos-parquet-engine/storage"
)

const (
	ReadBufferSize         = 4 * 1024
	defaultSectionCacheDir = "./cache"
)

type fileReaderOpts struct {
	sectionCacheDir string
}

type FileReaderOpt func(*fileReaderOpts)

func WithSectionCacheDir(dir string) FileReaderOpt {
	return func(opts *fileReaderOpts) {
		opts.sectionCacheDir = dir
	}
}

type FileReader struct {
	size       int64
	file       *parquet.File
	dataReader io.ReaderAt

	sectionLoader *sections
}

func NewFileReader(partName string, bucket objstore.Bucket, opts ...FileReaderOpt) (*FileReader, error) {
	partMetadata, err := readMetadata(partName+metadataFileSuffix, bucket)
	if err != nil {
		return nil, errors.Wrap(err, "error reading file metadata")
	}

	dataFile := partName + dataFileSuffix
	dataReader := storage.NewBucketReader(dataFile, bucket)

	dataFileAtts, err := bucket.Attributes(context.Background(), dataFile)
	if err != nil {
		return nil, errors.Wrap(err, "error reading file attributes")
	}

	readerOpts := applyOpts(opts)
	fsSectionLoader, err := newFilesystemLoader(dataReader, dataFileAtts.Size, readerOpts.sectionCacheDir)
	if err != nil {
		return nil, errors.Wrap(err, "error creating section reader")
	}

	fmt.Println("Loading bloom filter section")
	if err := loadBloomFilters(fsSectionLoader, partMetadata); err != nil {
		return nil, errors.Wrap(err, "error reading column bloom filters")
	}

	//fmt.Println("Loading dictionary sections")
	//if err := loadDictionaryPages(fsSectionLoader, partMetadata); err != nil {
	//	return nil, errors.Wrap(err, "error reading column dictionaries")
	//}

	reader := &FileReader{
		size:          dataFileAtts.Size,
		dataReader:    dataReader,
		sectionLoader: fsSectionLoader,
	}

	return reader, nil
}

func applyOpts(opts []FileReaderOpt) fileReaderOpts {
	readerOpts := fileReaderOpts{
		sectionCacheDir: defaultSectionCacheDir,
	}
	for _, opt := range opts {
		opt(&readerOpts)
	}
	return readerOpts
}

func (r *FileReader) SectionLoader() SectionLoader {
	return r.sectionLoader
}

func (r *FileReader) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = r.sectionLoader.ReadAt(p, off)
	if err == errSectionNotFound {
		return r.dataReader.ReadAt(p, off)
	}
	return n, err
}

func (r *FileReader) FileSize() int64 {
	return r.size
}

func (r *FileReader) Close() error {
	return r.sectionLoader.Close()
}

func readMetadata(metadataFile string, bucket objstore.Bucket) (*metadata.FileMetaData, error) {
	metaFileAttrs, err := bucket.Attributes(context.Background(), metadataFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get attributes for metadata file "+metadataFile)
	}

	metaReader, err := bucket.Get(context.Background(), metadataFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata file "+metadataFile)
	}
	defer metaReader.Close()

	metadataBytes := make([]byte, metaFileAttrs.Size)
	if _, err := io.ReadFull(metaReader, metadataBytes); err != nil {
		return nil, err
	}

	return metadata.NewFileMetaData(metadataBytes, nil)
}

func loadDictionaryPages(loader SectionLoader, metadata *metadata.FileMetaData) error {
	var errGroup errgroup.Group
	errGroup.SetLimit(32)
	for _, rowGroup := range metadata.RowGroups {
		for _, column := range rowGroup.Columns {
			dataPageOffset := column.MetaData.DataPageOffset
			dictionaryPageOffset := column.MetaData.DictionaryPageOffset
			if dictionaryPageOffset == nil {
				continue
			}
			errGroup.Go(func() error {
				sec, err := loader.NewSection(*dictionaryPageOffset, dataPageOffset)
				if err != nil {
					return err
				}
				return sec.LoadAll()
			})
		}
	}
	if err := errGroup.Wait(); err != nil {
		return err
	}

	return nil
}

func loadBloomFilters(loader SectionLoader, metadata *metadata.FileMetaData) error {
	var bloomFilterOffsets []int64
	for _, rg := range metadata.RowGroups {
		for _, c := range rg.Columns {
			if c.MetaData.BloomFilterOffset != nil {
				bloomFilterOffsets = append(bloomFilterOffsets, *c.MetaData.BloomFilterOffset)
			}
		}
	}
	slices.SortFunc(bloomFilterOffsets, func(a, b int64) bool {
		return a < b
	})

	if len(bloomFilterOffsets) == 0 {
		return nil
	}

	from := bloomFilterOffsets[0]
	to := bloomFilterOffsets[len(bloomFilterOffsets)-1] + ReadBufferSize

	sec, err := loader.NewSection(from, to)
	if err != nil {
		return err
	}
	return sec.LoadAll()
}
