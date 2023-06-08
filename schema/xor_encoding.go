package schema

import (
	"bytes"

	"github.com/keisku/gorilla"
	"github.com/segmentio/parquet-go/encoding"
	"github.com/segmentio/parquet-go/format"
)

const XOREncoding = 99

type XorEncoding struct {
	encoding.NotSupported
}

func (x XorEncoding) String() string { return "xor" }

func (x XorEncoding) Encoding() format.Encoding {
	return XOREncoding
}

func (x XorEncoding) EncodeDouble(dst []byte, src []float64) ([]byte, error) {
	if src == nil {
		return nil, nil
	}

	dst = dst[:0]
	buf := bytes.NewBuffer(dst)
	header := uint32(0)
	c, finish, err := gorilla.NewCompressor(buf, header)
	if err != nil {
		return nil, err
	}

	for _, f := range src {
		if err := c.Compress(1, f); err != nil {
			return nil, err
		}
	}
	err = finish()
	return buf.Bytes(), err
}

func (x XorEncoding) DecodeDouble(dst []float64, src []byte) ([]float64, error) {
	if src == nil {
		return nil, nil
	}

	d, _, err := gorilla.NewDecompressor(bytes.NewBuffer(src))
	if err != nil {
		return nil, err
	}

	dst = dst[:0]
	iter := d.Iterator()
	for iter.Next() {
		_, v := iter.At()
		dst = append(dst, v)
	}

	return dst, iter.Err()
}

func (x XorEncoding) CanDecodeInPlace() bool { return false }

func resize(buf []byte, size int) []byte {
	if cap(buf) < size {
		buf = make([]byte, size, 2*size)
	} else {
		buf = buf[:size]
	}
	return buf
}
