package db

import (
	"errors"
	"io"
	"sync"
)

var errSectionNotFound = errors.New("section not found")

type filesystemLoader struct {
	fileSize int64
	reader   io.ReaderAt

	mu             sync.RWMutex
	loadedSections []section
}

func newFilesystemReader(reader io.ReaderAt, fileSize int64) *filesystemLoader {
	return &filesystemLoader{
		fileSize:       fileSize,
		reader:         reader,
		loadedSections: make([]section, 0),
	}
}

func (f *filesystemLoader) LoadSection(from, to int64) error {
	s, err := readSection(f.reader, from, to, f.fileSize)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadedSections = append(f.loadedSections, s)
	return nil
}

func (f *filesystemLoader) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for _, s := range f.loadedSections {
		if off >= s.from && off+int64(len(p)) <= s.to {
			// We have the data in memory, copy it to p.
			copy(p, s.bytes[off-s.from:off-s.from+int64(len(p))])
			return len(p), nil
		}
	}

	return 0, errSectionNotFound
}
