package compute

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrent(t *testing.T) {
	numBatches := 2
	fragment := &testFragment{
		numBatches: numBatches,
		batch: Batch{
			{pqVal("val1", 0), pqVal("val1", 1)},
			{pqVal("val2", 0), pqVal("val2", 1)},
		},
	}

	var batchesSent int
	c := NewConcurrent(fragment, 3)
	for {
		batch, err := c.NextBatch()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, batch, fragment.batch)

		c.Release(batch)
		batchesSent++
	}

	require.NoError(t, c.Close())
	require.Equal(t, numBatches, batchesSent)
}

func TestConcurrentCancellation(t *testing.T) {
	numBatches := 2
	fragment := &testFragment{
		numBatches: numBatches,
		delay:      100 * time.Millisecond,
		batch: Batch{
			{pqVal("val1", 0), pqVal("val1", 1)},
			{pqVal("val2", 0), pqVal("val2", 1)},
		},
	}

	c := NewConcurrent(fragment, 0)
	batch, err := c.NextBatch()
	require.NoError(t, err)
	require.Equal(t, batch, fragment.batch)
	c.Release(batch)

	require.NoError(t, c.Close())
}

type testFragment struct {
	numBatches int
	batch      Batch
	delay      time.Duration
}

func (t *testFragment) MaxBatchSize() int64 {
	return int64(len(t.batch[0]))
}

func (t *testFragment) NextBatch() (Batch, error) {
	if t.numBatches == 0 {
		return nil, io.EOF
	}
	<-time.After(t.delay)

	t.numBatches--
	return t.batch, nil
}
func (t *testFragment) Release(_ Batch) {}
func (t *testFragment) Close() error    { return nil }
