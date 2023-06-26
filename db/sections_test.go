package db

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSection(t *testing.T)  {
	size := 8 * 1024 * 1024
	buffer := make([]byte, size)
	reader := bytes.NewReader(buffer)

	dir := t.TempDir()
	loader, err := newFilesystemLoader(reader, int64(size), dir)
	require.NoError(t, err)

	closer, err := loader.LoadSection(0, 1024)
	require.NoError(t, err)
	require.Nil(t, closer)
}