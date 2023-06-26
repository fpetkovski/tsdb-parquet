package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"fpetkovski/tsdb-parquet/storage"
)

var errSectionNotFound = errors.New("section not found")

type SectionLoader interface {
	LoadSection(from, to int64) (io.Closer, error)
}

type section struct {
	from  int64
	to    int64
	bytes []byte
}

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

type fileSection struct {
	from  int64
	to    int64
	bytes readerAtCloser
}

type sectionCloser struct {
	loader  *sectionLoader
	section fileSection
}

func (s sectionCloser) Close() error {
	return s.loader.releaseSection(s.section)
}

type sectionLoader struct {
	reader *storage.BucketReader

	fileSize int64
	cacheDir string

	mu             sync.RWMutex
	loadedSections []fileSection
}

func newFilesystemLoader(reader *storage.BucketReader, fileSize int64, cacheDir string) (*sectionLoader, error) {
	cachedFiles, err := os.ReadDir(cacheDir)
	if err != nil {
		return nil, err
	}

	sections := make([]fileSection, 0)
	for _, file := range cachedFiles {
		if file.IsDir() {
			continue
		}
		f, err := os.Open(path.Join(cacheDir, file.Name()))
		if err != nil {
			return nil, err
		}
		parts := strings.Split(file.Name(), "-")
		from, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, err
		}
		to, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
		sections = append(sections, fileSection{
			from:  int64(from),
			to:    int64(to),
			bytes: f,
		})
	}

	return &sectionLoader{
		reader:         reader,
		cacheDir:       cacheDir,
		fileSize:       fileSize,
		loadedSections: sections,
	}, nil
}

func (f *sectionLoader) LoadSection(from, to int64) (io.Closer, error) {
	f.mu.RLock()
	s, ok := f.findSection(from, to)
	if ok {
		f.mu.RUnlock()
		return sectionCloser{
			loader:  f,
			section: s,
		}, nil
	}
	f.mu.RUnlock()

	s, err := readSection(f.reader, from, to, f.fileSize, f.cacheDir)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadedSections = append(f.loadedSections, s)
	return sectionCloser{
		loader:  f,
		section: s,
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

func (f *sectionLoader) findSection(from, to int64) (fileSection, bool) {
	for _, section := range f.loadedSections {
		if section.from <= from && to <= section.to {
			return section, true
		}
	}

	return fileSection{}, false
}

func (f *sectionLoader) releaseSection(s fileSection) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := 0; i < len(f.loadedSections); i++ {
		if f.loadedSections[i].from == s.from && f.loadedSections[i].to == s.to {
			f.loadedSections = append(f.loadedSections[:i], f.loadedSections[i+1:]...)
			break
		}
	}

	if err := s.bytes.Close(); err != nil {
		return err
	}

	sectionFile := path.Join(f.cacheDir, fmt.Sprintf("%d-%d", s.from, s.to))
	return os.Remove(sectionFile)
}

func readSection(bucket *storage.BucketReader, from int64, to int64, fileSize int64, cacheDir string) (fileSection, error) {
	to += ReadBufferSize
	if to > fileSize {
		to = fileSize
	}
	buffer := make([]byte, to-from)
	reader, err := bucket.ReaderAt(buffer, from)
	if err != nil {
		return fileSection{}, err
	}
	sectionName := fmt.Sprintf("%d-%d", from, to)
	writer, err := os.Create(path.Join(cacheDir, sectionName))
	if err != nil {
		return fileSection{}, err
	}
	if _, err := io.Copy(writer, reader); err != nil {
		return fileSection{}, err
	}

	return fileSection{
		from:  from,
		to:    to,
		bytes: writer,
	}, nil
}
