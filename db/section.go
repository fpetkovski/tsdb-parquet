package db

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

const prefetchBufferSize = 4 * 1024 * 1024

type Section interface {
	io.Closer
	LoadNext() error
	LoadAll() error
}

type section struct {
	sections *sections

	from       int64
	to         int64
	readBuffer []byte

	reader io.Reader
	bytes  sectionBytes
}

func (fs *sections) newDiskSection(from, to, readBatchSize int64, dir string, reader io.Reader) (section, error) {
	filePath := path.Join(dir, fmt.Sprintf("%d-%d.section", from, to))
	f, err := os.Create(filePath)
	if err != nil {
		return section{}, err
	}

	return section{
		sections:   fs,
		from:       from,
		to:         to,
		readBuffer: make([]byte, readBatchSize),
		reader:     reader,
		bytes:      fileBytes{path: filePath, File: f},
	}, nil
}

func (fs *sections) newMemorySection(from, to, readBatchSize int64, reader io.Reader) (section, error) {
	buffer := make([]byte, 0, to-from)

	return section{
		sections:   fs,
		from:       from,
		to:         to,
		readBuffer: make([]byte, readBatchSize),
		reader:     reader,
		bytes:      &memoryBytes{bytes: buffer},
	}, nil
}

func (s section) LoadNext() error {
	start := time.Now()
	bufferReader := io.LimitReader(s.reader, int64(len(s.readBuffer)))
	n, err := io.CopyBuffer(s.bytes, bufferReader, s.readBuffer)
	if err != nil {
		return err
	}
	if n == 0 {
		return io.EOF
	}

	fmt.Printf("Read %dKB in %s. Estimated throughput: %f MB/s\n", n, time.Since(start), float64(n)/1024/1024/time.Since(start).Seconds())
	return nil
}

func (s section) LoadAll() error {
	_, err := io.Copy(s.bytes, s.reader)
	return err
}

func (s section) Close() error {
	return s.sections.release(s)
}

type asyncSection struct {
	section Section
	buffer  chan error

	cancel func()
	ctx    context.Context
}

func AsyncSection(s Section, bufSize int64) Section {
	a := asyncSection{
		section: s,
		buffer:  make(chan error, bufSize),
	}

	a.ctx, a.cancel = context.WithCancel(context.Background())
	go a.loadNextAsync()

	return a
}

func (a asyncSection) loadNextAsync() {
	defer close(a.buffer)
	for {
		select {
		case <-a.ctx.Done():
			return
		default:
			err := a.section.LoadNext()
			a.buffer <- err
			if err == io.EOF {
				return
			}
		}
	}
}

func (a asyncSection) Close() error {
	a.cancel()
	for range a.buffer {
	}
	return a.section.Close()
}

func (a asyncSection) LoadNext() error {
	return <-a.buffer
}

func (a asyncSection) LoadAll() error {
	return a.section.LoadAll()
}

type sectionBytes interface {
	io.ReaderAt
	io.WriteCloser
}

type fileBytes struct {
	*os.File
	path string
}

func (d fileBytes) Close() error {
	if err := d.File.Close(); err != nil {
		return err
	}
	return os.Remove(d.path)
}

type memoryBytes struct {
	bytes []byte
}

func (m *memoryBytes) ReadAt(p []byte, off int64) (n int, err error) {
	copy(p, m.bytes[off:off+int64(len(p))])
	return len(p), nil
}

func (m *memoryBytes) Write(p []byte) (n int, err error) {
	m.bytes = append(m.bytes, p...)
	return len(p), nil
}

func (m *memoryBytes) Close() error { return nil }
