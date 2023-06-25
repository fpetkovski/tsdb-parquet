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

	"fpetkovski/tsdb-parquet/storage"
)

const ReadBufferSize = 4 * 1024

type FileReader struct {
	size       int64
	file       *parquet.File
	dataReader io.ReaderAt

	sectionLoader *sectionLoader
}

func OpenFileReader(partName string, bucket objstore.Bucket) (*FileReader, error) {
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

	fsSectionLoader := newFilesystemReader(dataReader, dataFileAtts.Size)

	fmt.Println("Loading bloom filter section")
	if err := loadBloomFilters(fsSectionLoader, partMetadata); err != nil {
		return nil, errors.Wrap(err, "error reading column bloom filters")
	}

	fmt.Println("Loading dictionary sections")
	if err := loadDictionaryPages(fsSectionLoader, partMetadata); err != nil {
		return nil, errors.Wrap(err, "error reading column dictionaries")
	}

	reader := &FileReader{
		size:          dataFileAtts.Size,
		dataReader:    dataReader,
		sectionLoader: fsSectionLoader,
	}

	return reader, nil
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
	errGroup.SetLimit(16)
	for _, rowGroup := range metadata.RowGroups {
		for _, column := range rowGroup.Columns {
			dataPageOffset := column.MetaData.DataPageOffset
			dictionaryPageOffset := column.MetaData.DictionaryPageOffset
			if dictionaryPageOffset == nil {
				continue
			}
			errGroup.Go(func() error {
				return loader.LoadSection(*dictionaryPageOffset, dataPageOffset)
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
	to := bloomFilterOffsets[len(bloomFilterOffsets)-1] + 4*1024

	return loader.LoadSection(from, to)
}

func readSection(reader io.ReaderAt, from int64, to int64, fileSize int64) (section, error) {
	to += ReadBufferSize
	if to > fileSize {
		to = fileSize
	}
	buffer := make([]byte, to-from)
	_, err := reader.ReadAt(buffer, from)
	if err != nil {
		return section{}, err
	}

	return section{
		from:  from,
		to:    to,
		bytes: buffer,
	}, nil
}