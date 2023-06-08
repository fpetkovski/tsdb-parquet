package schema

import (
	"os"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

type chunk struct {
	MinT       int64
	MaxT       int64
	ChunkBytes []byte
}

func TestChunkSchema(t *testing.T) {
	schemaLabels := []string{"node", "container", "id"}
	schema := MakeChunkSchema(schemaLabels)

	file, err := os.Create("file.parquet")
	require.NoError(t, err)

	writer := parquet.NewWriter(file, schema.ParquetSchema())
	defer writer.Close()

	rows := []parquet.Row{
		schema.MakeChunkRow(Chunk{
			Labels:     labels.FromStrings("node", "gke-1", "id", "5"),
			MinT:       0,
			MaxT:       10,
			ChunkBytes: []byte{2, 3, 5},
		}),
		schema.MakeChunkRow(Chunk{
			Labels:     labels.FromStrings("container", "nginx", "id", "5"),
			MinT:       0,
			MaxT:       10,
			ChunkBytes: []byte{2, 3, 5},
		}),
		schema.MakeChunkRow(Chunk{
			Labels:     labels.FromStrings("container", "nginx", "node", "1"),
			MinT:       0,
			MaxT:       10,
			ChunkBytes: []byte{2, 3, 5},
		}),
	}
	_, err = writer.WriteRows(rows)

	require.NoError(t, err)
}
