package db

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/apache/arrow/go/v10/parquet/metadata"
	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"

	"fpetkovski/tsdb-parquet/storage"
)

const ReadBufferSize = 4 * 1024

type section struct {
	from  int64
	to    int64
	bytes []byte
}

type FileReader struct {
	partName       string
	file           *parquet.File
	metadata       *metadata.FileMetaData
	dataFileSize   int64
	dataFileReader *storage.BucketReader
	loadedSections []section
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

	fmt.Println("Loading bloom filter section")
	bloomFiltersSection, err := loadBloomFilters(dataReader, partMetadata, dataFileAtts.Size)
	if err != nil {
		return nil, errors.Wrap(err, "error reading column bloom filters")
	}

	fmt.Println("Loading dictionary sections")
	dictionarySections, err := loadDictionaryPages(dataReader, partMetadata, dataFileAtts.Size)
	if err != nil {
		return nil, errors.Wrap(err, "error reading column dictionaries")
	}

	reader := &FileReader{
		partName:       partName,
		metadata:       partMetadata,
		dataFileSize:   dataFileAtts.Size,
		dataFileReader: dataReader,
		loadedSections: append([]section{bloomFiltersSection}, dictionarySections...),
	}

	return reader, nil
}

func (r *FileReader) MetaData() *metadata.FileMetaData {
	return r.metadata
}

func (r *FileReader) ReadAt(p []byte, off int64) (n int, err error) {
	for _, s := range r.loadedSections {
		if off >= s.from && off+int64(len(p)) <= s.to {
			// We have the data in memory, copy it to p.
			copy(p, s.bytes[off-s.from:off-s.from+int64(len(p))])
			return len(p), nil
		}
	}

	return r.dataFileReader.ReadAt(p, off)
}

func (r *FileReader) LoadSection(from, to int64) error {
	if from == to {
		return nil
	}
	s, err := readSection(r.dataFileReader, from, to, r.FileSize())
	if err != nil {
		return err
	}
	r.loadedSections = append(r.loadedSections, s)
	return nil
}

func (r *FileReader) FileSize() int64 {
	return r.dataFileSize
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

func loadDictionaryPages(dataReader io.ReaderAt, metadata *metadata.FileMetaData, fileSize int64) ([]section, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var sections []section
	for _, rowGroup := range metadata.RowGroups {
		for _, column := range rowGroup.Columns {
			dataPageOffset := column.MetaData.DataPageOffset
			dictionaryPageOffset := column.MetaData.DictionaryPageOffset
			if dictionaryPageOffset == nil {
				continue
			}
			if dataPageOffset-*dictionaryPageOffset < 4*1024 {
				dataPageOffset = *dictionaryPageOffset + 4*1024
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				dictionarySection, err := readSection(dataReader, *dictionaryPageOffset, dataPageOffset, fileSize)
				if err != nil {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				sections = append(sections, dictionarySection)
			}()
		}
	}
	wg.Wait()
	slices.SortFunc(sections, func(a, b section) bool {
		return a.from < b.from
	})

	return sections, nil
}

func loadBloomFilters(dataReader io.ReaderAt, metadata *metadata.FileMetaData, fileSize int64) (section, error) {
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
		return section{}, nil
	}

	from := bloomFilterOffsets[0]
	to := bloomFilterOffsets[len(bloomFilterOffsets)-1] + 4*1024
	return readSection(dataReader, from, to, fileSize)
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
