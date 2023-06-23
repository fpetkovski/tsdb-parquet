package dataset

import (
	"sync"

	"github.com/segmentio/parquet-go"
)

type valuesPool struct {
	mu sync.Mutex

	peak   int
	size   int
	values [][]parquet.Value
}

func newValuesPool(size int) *valuesPool {
	return &valuesPool{
		size:   size,
		values: make([][]parquet.Value, 0),
	}
}

func (p *valuesPool) get() []parquet.Value {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peak == len(p.values) {
		p.values = append(p.values, make([]parquet.Value, p.size))
	}
	values := p.values[p.peak]
	p.peak++
	return values
}

func (p *valuesPool) put(values []parquet.Value) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.peak--
	p.values[p.peak] = values
}
