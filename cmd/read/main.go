package main

import (
	"errors"
	"fmt"
	"github.com/segmentio/parquet-go"
	"io"
	"log"
	"os"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer f.Close()

	fstats, err := f.Stat()
	if err != nil {
		log.Fatalln(err)
	}
	pqreader, err := parquet.OpenFile(f, fstats.Size())
	if err != nil {
		log.Fatalln(err)
	}

	meta := pqreader.Metadata()
	fmt.Println(meta.NumRows)
	schema := pqreader.Schema()
	columns := schema.Columns()
	fmt.Println(columns)

	groups := pqreader.RowGroups()
	for _, group := range groups {
		chunks := group.ColumnChunks()
		for _, chunk := range chunks {
			//bloom := chunk.BloomFilter()
			columnName := columns[chunk.Column()][0]
			pages := chunk.Pages()

			index := chunk.ColumnIndex()
			fmt.Println(index)

			p, err := pages.ReadPage()
			if err != nil {
				panic(err)
			}

			dict := p.Dictionary()
			if p.Type().String() == "STRING" {
				values := make([]parquet.Value, 1)
				indexes := []int32{0}
				dict.Lookup(indexes, values)
				fmt.Println(columnName, values)
			}

			switch page := p.Values().(type) {
			case parquet.ByteArrayReader:
				values := make([]byte, 10000*p.NumValues())
				_, err := page.ReadByteArrays(values)
				if err != nil && !errors.Is(err, io.EOF) {
					panic(err)
				}

				if columnName == "SPECIAL_METRIC_NAME" {
					fmt.Println(string(values))
				}
			case parquet.Int64Reader:
				fmt.Println(p.Bounds())
			default:
				fmt.Println(columnName)
			}
		}
	}
}
