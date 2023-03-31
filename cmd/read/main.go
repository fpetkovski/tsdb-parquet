package main

import (
	"log"
	"os"

	"github.com/apache/arrow/go/v10/parquet/file"
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
	pqreader.Close()
}
