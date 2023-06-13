package db

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v10/parquet/metadata"
	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"

	"fpetkovski/tsdb-parquet/storage"
)

type section struct {
	from  int64
	to    int64
	bytes []byte
}

type dictionaryPage struct {
	header    parquet.DictionaryPageHeader
	pageBytes []byte
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
		return nil, err
	}

	dataFile := partName + dataFileSuffix
	dataReader := storage.NewBucketReader(dataFile, bucket)

	dataFileAtts, err := bucket.Attributes(context.Background(), dataFile)
	if err != nil {
		return nil, err
	}

	bloomFiltersSection, err := readBloomFilters(dataReader, partMetadata)
	if err != nil {
		return nil, err
	}

	dictionaryPages, err := readDictionaryPages(dataReader, partMetadata)
	if err != nil {
		return nil, err
	}

	reader := &FileReader{
		partName:       partName,
		metadata:       partMetadata,
		dataFileSize:   dataFileAtts.Size,
		dataFileReader: dataReader,
		loadedSections: []section{
			bloomFiltersSection,
			dictionaryPages,
		},
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
	s, err := readSection(r.dataFileReader, from, to)
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

func readDictionaryPages(dataReader io.ReaderAt, metadata *metadata.FileMetaData) (section, error) {
	numRowGroups := len(metadata.RowGroups)
	var i, j int
loopColumns:
	for i = 0; i < numRowGroups; i++ {
		for j = 0; j < len(metadata.RowGroups[i].Columns); j++ {
			dictionaryPageOffset := metadata.RowGroups[i].Columns[j].MetaData.DictionaryPageOffset
			if dictionaryPageOffset != nil {
				break loopColumns
			}
		}
	}

	firstColumn := metadata.RowGroups[i].Columns[j]
	firstDictPageOffset := *firstColumn.MetaData.DictionaryPageOffset

	lastRowGroup := metadata.RowGroups[numRowGroups-1]
	lastColumn := lastRowGroup.Columns[len(lastRowGroup.Columns)-1]
	lastDictPageOffset := *lastColumn.MetaData.DictionaryPageOffset + 4*1024

	bytes := make([]byte, lastDictPageOffset-firstDictPageOffset)
	_, err := dataReader.ReadAt(bytes, firstDictPageOffset)
	if err != nil {
		return section{}, err
	}

	return section{
		from:  firstDictPageOffset,
		to:    lastDictPageOffset,
		bytes: bytes,
	}, nil
}

func readBloomFilters(dataReader io.ReaderAt, metadata *metadata.FileMetaData) (section, error) {
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

	from := bloomFilterOffsets[0]
	to := bloomFilterOffsets[len(bloomFilterOffsets)-1] + 4*1024
	return readSection(dataReader, from, to)
}

func readSection(reader io.ReaderAt, from int64, to int64) (section, error) {
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
