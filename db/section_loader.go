package db

import (
	"errors"
	"io"
	"sync"

	"fpetkovski/tsdb-parquet/storage"
)

var errSectionNotFound = errors.New("section not found")

type SectionLoader interface {
	LoadSection(from, to int64) (io.Closer, error)
}

type sectionLoader struct {
	reader *storage.BucketReader

	fileSize int64
	cacheDir string

	mu             sync.RWMutex
	loadedSections []section
}

func newFilesystemLoader(reader *storage.BucketReader, fileSize int64, cacheDir string) (*sectionLoader, error) {
	return &sectionLoader{
		reader:         reader,
		cacheDir:       cacheDir,
		fileSize:       fileSize,
		loadedSections: make([]section, 0),
	}, nil
}

func (f *sectionLoader) LoadSection(from, to int64) (io.Closer, error) {
	f.mu.RLock()
	sec, ok := f.findSection(from, to)
	if ok {
		f.mu.RUnlock()
		return sectionCloser{
			loader:  f,
			section: sec,
		}, nil
	}
	f.mu.RUnlock()

	readTo := to + ReadBufferSize
	if readTo > f.fileSize {
		readTo = f.fileSize
	}
	sec, err := newDiskSection(from, readTo, f.cacheDir)
	if err != nil {
		return nil, err
	}

	if err := readSection(f.reader, from, readTo, sec); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadedSections = append(f.loadedSections, sec)
	return sectionCloser{
		loader:  f,
		section: sec,
	}, nil
}

func (f *sectionLoader) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	s, ok := f.findSection(off, off+int64(len(p)))
	if ok {
		return s.bytes.ReadAt(p, off-s.from)
	}

	return 0, errSectionNotFound
}

func (f *sectionLoader) findSection(from, to int64) (section, bool) {
	for _, sec := range f.loadedSections {
		if sec.from <= from && to <= sec.to {
			return sec, true
		}
	}

	return section{}, false
}

func (f *sectionLoader) releaseSection(s section) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := 0; i < len(f.loadedSections); i++ {
		if f.loadedSections[i].from == s.from && f.loadedSections[i].to == s.to {
			f.loadedSections = append(f.loadedSections[:i], f.loadedSections[i+1:]...)
			break
		}
	}

	return s.bytes.Close()
}

func (f *sectionLoader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, sec := range f.loadedSections {
		if err := sec.bytes.Close(); err != nil {
			return err
		}
	}
	f.loadedSections = f.loadedSections[:0]
	return nil
}

func readSection(bucket *storage.BucketReader, from int64, to int64, into section) error {
	buffer := make([]byte, to-from)
	reader, err := bucket.ReaderAt(buffer, from)
	if err != nil {
		return err
	}

	if _, err := io.Copy(into.bytes, reader); err != nil {
		return err
	}

	return nil
}

type sectionCloser struct {
	loader  *sectionLoader
	section section
}

func (s sectionCloser) Close() error {
	return s.loader.releaseSection(s.section)
}
