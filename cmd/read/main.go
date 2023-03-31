package main

import (
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

	fmt.Println(pqreader.MetaData().RowGroups[0].Columns[4])

	_, err = pqarrow.NewFileReader(pqreader, pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 1000,
	}, memory.DefaultAllocator)
	if err != nil {
		log.Fatalln(err.Error())
	}
}
