package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/segmentio/parquet-go"
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

	columnIndex := make(map[int]struct{})
	columns := pqreader.Schema().Columns()
	for i, col := range columns {
		if col[0] == "namespace" {
			columnIndex[i] = struct{}{}
		}
	}

	namespace := parquet.ByteArrayValue([]byte("monitoring"))

	groups := pqreader.RowGroups()
	for _, group := range groups {
		chunks := group.ColumnChunks()
		for _, chunk := range chunks {
			if _, ok := columnIndex[chunk.Column()]; !ok {
				continue
			}

			hasValue, err := chunk.BloomFilter().Check(namespace)
			if err != nil {
				log.Fatalln(err)
			}
			if !hasValue {
				continue
			}
			fmt.Println(hasValue)
			pages := chunk.Pages()
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				values := make([]parquet.Value, page.NumValues())

				_, err = page.Values().ReadValues(values)
				if err != nil && !errors.Is(err, io.EOF) {
					panic(err)
				}
				fmt.Println(values)
			}

			//columnName := columns[chunk.Column()][0]
			//pages := chunk.Pages()
			//
			//index := chunk.ColumnIndex()
			//fmt.Println(index)
			//
			//p, err := pages.ReadPage()
			//if err != nil {
			//	panic(err)
			//}
			//
			//dict := p.Dictionary()
			//if p.Type().String() == "STRING" {
			//	values := make([]parquet.Value, 1)
			//	indexes := []int32{0}
			//	dict.Lookup(indexes, values)
			//	fmt.Println(columnName, values)
			//}
			//
			//switch page := p.Values().(type) {
			//case parquet.ByteArrayReader:
			//	values := make([]byte, 10000*p.NumValues())
			//	_, err := page.ReadByteArrays(values)
			//	if err != nil && !errors.Is(err, io.EOF) {
			//		panic(err)
			//	}
			//case parquet.Int64Reader:
			//	fmt.Println(p.Bounds())
			//default:
			//	fmt.Println(columnName)
			//}
		}
	}
}
