package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestXOREncoding(t *testing.T) {
	enc := &XorEncoding{}

	numVals := 5000
	vals := make([]float64, 0, numVals)
	for i := 0; i < numVals; i++ {
		vals = append(vals, 1)
	}

	dst, err := enc.EncodeDouble(nil, vals)
	require.NoError(t, err)

	dec, err := enc.DecodeDouble(nil, dst)
	require.NoError(t, err)
	fmt.Println(len(dec))
}
