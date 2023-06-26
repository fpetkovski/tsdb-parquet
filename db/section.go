package db

import (
	"fmt"
	"io"
	"os"
	"path"
)

type sectionBytes interface {
	io.ReaderAt
	io.WriteCloser
}

type section struct {
	from  int64
	to    int64
	bytes sectionBytes
}

func newDiskSection(from, to int64, dir string) (section, error) {
	filePath := path.Join(dir, fmt.Sprintf("%d-%d.section", from, to))
	f, err := os.Create(filePath)
	if err != nil {
		return section{}, err
	}

	return section{
		from:  from,
		to:    to,
		bytes: diskReader{path: filePath, f: f},
	}, nil
}

type diskReader struct {
	path string
	f    *os.File
}

func (d diskReader) ReadAt(p []byte, off int64) (int, error) {
	return d.f.ReadAt(p, off)
}

func (d diskReader) Write(p []byte) (n int, err error) {
	return d.f.Write(p)
}

func (d diskReader) Close() error {
	if err := d.f.Close(); err != nil {
		return err
	}
	return os.Remove(d.path)
}
