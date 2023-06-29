package db

import (
	"errors"
	"sync"

	"Shopify/thanos-parquet-engine/storage"
)

var errSectionNotFound = errors.New("section not found")

type SectionLoader interface {
	NewSectionSize(from, to, size int64) (Section, error)
	NewSection(from, to int64) (Section, error)
}

type sections struct {
	reader *storage.BucketReader

	fileSize int64
	cacheDir string

	mu             sync.RWMutex
	loadedSections []section
}

func newFilesystemLoader(reader *storage.BucketReader, fileSize int64, cacheDir string) (*sections, error) {
	return &sections{
		reader:         reader,
		cacheDir:       cacheDir,
		fileSize:       fileSize,
		loadedSections: make([]section, 0),
	}, nil
}

func (fs *sections) NewSection(from, to int64) (Section, error) {
	return fs.NewSectionSize(from, to, prefetchBufferSize)
}

func (fs *sections) NewSectionSize(from, to, size int64) (Section, error) {
	fs.mu.RLock()
	sec, ok := fs.find(from, to)
	if ok {
		fs.mu.RUnlock()
		return sec, nil
	}
	fs.mu.RUnlock()

	to = to + ReadBufferSize
	if to > fs.fileSize {
		to = fs.fileSize
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	sec, ok = fs.find(from, to)
	if ok {
		return sec, nil
	}

	sectionReader, err := fs.reader.ReaderAt(from, to-from)
	if err != nil {
		return nil, err
	}

	sec, err = fs.newDiskSection(from, to, size, fs.cacheDir, sectionReader)
	if err != nil {
		return nil, err
	}

	fs.loadedSections = append(fs.loadedSections, sec)
	return sec, nil
}

func (fs *sections) ReadAt(p []byte, absOffset int64) (int, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	s, ok := fs.find(absOffset, absOffset+int64(len(p)))
	if ok {
		relOffset := absOffset - s.from
		return s.bytes.ReadAt(p, relOffset)
	}

	return 0, errSectionNotFound
}

func (fs *sections) find(from, to int64) (section, bool) {
	for _, sec := range fs.loadedSections {
		if sec.from <= from && to <= sec.to {
			return sec, true
		}
	}

	return section{}, false
}

func (fs *sections) release(s section) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for i := 0; i < len(fs.loadedSections); i++ {
		if fs.loadedSections[i].from == s.from && fs.loadedSections[i].to == s.to {
			fs.loadedSections = append(fs.loadedSections[:i], fs.loadedSections[i+1:]...)
			break
		}
	}

	return s.bytes.Close()
}

func (fs *sections) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	for _, sec := range fs.loadedSections {
		if err := sec.bytes.Close(); err != nil {
			return err
		}
	}
	fs.loadedSections = fs.loadedSections[:0]
	return nil
}
