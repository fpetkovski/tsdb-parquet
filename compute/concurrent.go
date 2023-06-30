package compute

import (
	"context"
	"sync"
)

type maybeBatch struct {
	batch Batch
	err   error
}

type Concurrent struct {
	fragment Fragment

	once   sync.Once
	buffer chan maybeBatch

	ctx    context.Context
	cancel context.CancelFunc
}

func NewConcurrent(fragment Fragment, bufferSize int64) *Concurrent {
	c := &Concurrent{
		fragment: fragment,
		buffer:   make(chan maybeBatch, bufferSize),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	go c.pullNextBatch()

	return c
}

func (c *Concurrent) NextBatch() (Batch, error) {
	nextBatch := <-c.buffer
	return nextBatch.batch, nextBatch.err
}

func (c *Concurrent) pullNextBatch() {
	defer close(c.buffer)
	for {
		select {
		case <-c.ctx.Done():
			c.buffer <- maybeBatch{err: c.ctx.Err()}
			return
		default:
			batch, err := c.fragment.NextBatch()
			c.buffer <- maybeBatch{batch: batch, err: err}
		}
	}
}

func (c *Concurrent) MaxBatchSize() int64 {
	return c.fragment.MaxBatchSize()
}

func (c *Concurrent) Release(batch Batch) {
	c.fragment.Release(batch)
}

func (c *Concurrent) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	for range c.buffer {
	}
	return c.fragment.Close()
}
