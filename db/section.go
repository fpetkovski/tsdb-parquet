package db

import (
	"context"
	"fmt"
	"io"
	"os"
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

	from          int64
	to            int64
	readBatchSize int64

	reader io.Reader
	bytes  sectionBytes
}

func (s section) LoadNext() error {
	start := time.Now()
	n, err := io.CopyN(s.bytes, s.reader, s.readBatchSize)
	if err != nil {
		return err
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
