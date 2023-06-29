package pqtest

import (
	"bytes"

	"github.com/segmentio/parquet-go"
)

var columns = []string{"ColumnA", "ColumnB", "ColumnC", "ColumnD"}

type Row struct {
	ColumnA string `parquet:",dict"`
	ColumnB string `parquet:",dict"`
	ColumnC string `parquet:",dict"`
	ColumnD string `parquet:",dict"`
}

func TwoColumnRow(columnA string, columnB string) Row {
	return Row{ColumnA: columnA, ColumnB: columnB}
}

func CreateFile(parts [][]Row) (*parquet.File, error) {
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[Row](&buffer,
		parquet.PageBufferSize(4),
	)

	for _, parts := range parts {
		_, err := writer.Write(parts)
		if err != nil {
			return nil, err
		}
	}

	if err := writer.Close(); err != nil {
		return nil, err
	}

	readBuf := bytes.NewReader(buffer.Bytes())
	return parquet.OpenFile(readBuf, int64(len(buffer.Bytes())))
}
