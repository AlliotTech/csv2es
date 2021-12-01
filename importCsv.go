package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ctessum/macreader"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/olivere/elastic/v7"
)

func importCsv()  {
	// 解析命令行输入
	host := flag.String("host", "http://localhost:9200", "host, e.g. http://localhost:9200")
	file := flag.String("file", "", "file path")
	esIndex := flag.String("index", "", "elastic search index")
	//esType := flag.String("type", "keyword", "elastic search type")
	bulkSize := flag.Int("bulksize", 1000, "elastic search bulk size(int)")
	threadNum := flag.Int("thread",runtime.GOMAXPROCS(0),"thread num")
	debugState := flag.String("debug", "no", "open debug? yes or no")

	flag.Parse()
	//必填参数
	if *file == "" {
		fmt.Println("please set which csv file you want to import")
		return
	}
	if *esIndex == "" {
		fmt.Println("please set es index")
		return
	}


	fmt.Println("start processing at", time.Now(), "path:", *file)

	//创建es连接
	esClient, err := elastic.NewClient(elastic.SetSniff(false),
		elastic.SetURL(*host),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	//创建es bulk processor
	esService, err := esClient.BulkProcessor().
		BulkActions(*bulkSize).  //bulk大小
		Workers(*threadNum).Do(context.Background()) //并行数
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer esService.Close()

	var lineCount uint64 = 0
	var validCount uint64 = 0
	go func(lineCount *uint64, validCount *uint64) {
		timer := time.Tick(time.Second * 5)
		for {
			select {
			case <-timer:
				get, err := esClient.CatCount().Index(*esIndex).Do(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}
				fmt.Println("around", *lineCount, "lines scanned,", *validCount, "valid records,", get[0].Count, "in database")
			}
		}
	}(&lineCount, &validCount)


	// 打开文件
	fd, err := os.OpenFile(*file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Open file error")
		return
	}
	defer fd.Close()
	//读取csv  兼容mac文件结束符
	reader := csv.NewReader(macreader.New(bufio.NewReader(fd)))
	//reader := csv.NewReader(bufio.NewReader(fd))

	//读取csv第一行的keys
	keys, err := reader.Read()
	//从第二行开始遍历csv 序列化加入es bulk
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		//序列化
		m := make(map[string]string)
		for i, key := range keys {
			m[key] = record[i]
		}
		jsonStr, err := json.Marshal(m)
		if err != nil {
			//fmt.Println(err.Error(), lineCount, line)
			lineCount++
			continue
		}
		if *debugState == "yes"{
			fmt.Println(string(jsonStr))
		}
		esService.Add(elastic.NewBulkIndexRequest().Index(*esIndex).Doc(string(jsonStr)))
		lineCount++
		validCount++
		//if lineCount>200 {
		//	break
		//}
	}
}
