### csv2es  
Golang 实现的一个将 csv 导入到 elasticsearch 的小工具  
环境： elasticsearch 7

```  
# 构建
GOOS=linux GOARCH=amd64 go build .

# 使用
./go2es -host "http://x.x.x.x:9200" -index "test_index" -file "1.csv"  -bulksize 1000

# 帮助  
./go2es -h
```