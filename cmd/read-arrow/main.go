package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer f.Close()

	pqreader, err := file.NewParquetReader(f)
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer pqreader.Close()

	schema := pqreader.MetaData().Schema
	for i := 0; i < schema.NumColumns(); i++ {
		fmt.Println(schema.Column(i).Name())
	}

	freader, err := pqarrow.NewFileReader(pqreader, pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 4 * 1024,
	}, memory.DefaultAllocator)
	if err != nil {
		log.Fatalln(err.Error())
	}

	rrs, _, err := freader.GetFieldReaders(context.Background(), []int{15, 20, 11, 31, 90}, []int{10})
	if err != nil {
		log.Fatalln(err)
	}

	for _, rr := range rrs {
		batch, err := rr.NextBatch(1000)
		if err != nil {
			log.Fatal(err)
		}
		for _, chunk := range batch.Chunks() {
			fmt.Println(chunk)
		}
	}
}
