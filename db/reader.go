package db

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v10/parquet/metadata"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"

	"fpetkovski/tsdb-parquet/storage"
)

type section struct {
	from  int64
	to    int64
	bytes []byte
}

type Reader struct {
	partName       string
	metadata       *metadata.FileMetaData
	dataFileSize   int64
	dataFileReader *storage.BucketReader

	loadedSections []section
}

func (r *Reader) FileSize() int64 {
	return r.dataFileSize
}

func (r *Reader) ReadAt(p []byte, off int64) (n int, err error) {
	for _, s := range r.loadedSections {
		if off >= s.from && off+int64(len(p)) <= s.to {
			// We have the data in memory
			// Copy it to p
			copy(p, s.bytes[off-s.from:off-s.from+int64(len(p))])
			return len(p), nil
		}
	}

	return r.dataFileReader.ReadAt(p, off)
}

func OpenReader(partName string, bucket objstore.Bucket) (*Reader, error) {
	metadataFile := partName + metadataFileSuffix
	metaFileAttrs, err := bucket.Attributes(context.Background(), metadataFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata file attributes for part "+partName)
	}

	dataFile := partName + dataFileSuffix
	dataFileReader := storage.NewBucketReader(dataFile, bucket)

	metaReader, err := dataFileReader.Get(context.Background(), metadataFile)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get metadata for metadata file "+metadataFile)
	}
	defer metaReader.Close()

	metadataBytes := make([]byte, metaFileAttrs.Size)
	if _, err := io.ReadFull(metaReader, metadataBytes); err != nil {
		return nil, err
	}

	partMetadata, err := metadata.NewFileMetaData(metadataBytes, nil)
	if err != nil {
		return nil, err
	}

	dataFileAtts, err := bucket.Attributes(context.Background(), dataFile)
	if err != nil {
		return nil, err
	}

	reader := &Reader{
		partName:       partName,
		metadata:       partMetadata,
		dataFileSize:   dataFileAtts.Size,
		dataFileReader: dataFileReader,
	}
	if err := reader.loadBloomFilters(); err != nil {
		return nil, err
	}

	return reader, nil
}

func (r *Reader) loadBloomFilters() error {
	var bloomFilterOffsets []int64
	for _, rg := range r.metadata.RowGroups {
		for _, c := range rg.Columns {
			if c.MetaData.BloomFilterOffset != nil {
				bloomFilterOffsets = append(bloomFilterOffsets, *c.MetaData.BloomFilterOffset)
			}
		}
	}
	slices.SortFunc(bloomFilterOffsets, func(a, b int64) bool {
		return a < b
	})

	from := bloomFilterOffsets[0]
	to := bloomFilterOffsets[len(bloomFilterOffsets)-1] + 4*1024
	return r.loadSection(from, to)
}

func (r *Reader) loadSection(from int64, to int64) error {
	buffer := make([]byte, to-from)
	_, err := r.dataFileReader.ReadAt(buffer, from)
	if err != nil {
		return err
	}

	r.loadedSections = append(r.loadedSections, section{
		from:  from,
		to:    to,
		bytes: buffer,
	})
	return nil
}
