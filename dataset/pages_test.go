package dataset

import (
	"bytes"
	"fmt"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

var strType = parquet.String().Type()

func TestPages(t *testing.T) {
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[testRow](&buffer,
		parquet.WriteBufferSize(4),
		parquet.PageBufferSize(4),
	)

	var valID int64
	pageSizes := []int{1, 3, 4, 3}
	for _, size := range pageSizes {
		rows := make([]testRow, size)
		for i := 0; i < size; i++ {
			rows[i] = testRow{ColumnA: fmt.Sprintf("val-%d", valID)}
			valID++
		}
		n, err := writer.Write(rows)
		require.NoError(t, err)
		require.Equal(t, len(rows), n)
	}
	require.NoError(t, writer.Close())

	readBuf := bytes.NewReader(buffer.Bytes())
	file, err := parquet.OpenFile(readBuf, int64(len(buffer.Bytes())))
	require.NoError(t, err)

	pages := file.RowGroups()[0].ColumnChunks()[0].Pages()
	defer pages.Close()
	for {
		page, err := pages.ReadPage()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		fmt.Println(page.Data())
	}
}
