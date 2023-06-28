
# 编译

git clone https://github.com/luky116/pika-benchmmark.git
cd pika-benchmmark
sh build.sh

# 参数介绍
| 参数      | 含义                                                 |
| --------- |----------------------------------------------------|
| -h        | 服务的IP                                              |
| -p        | 服务的端口                                              |
| -a        | 服务的密码                                              |
| -prefix   | 生成数据的前缀,会有默认值，可以不填                                 |
| -c        | 造数据的命令，可以是 SET、HSET、LPUSH、SADD、ZADD，多个用逗号隔开，默认为SET |
| -parallel | 并发连接数                                              |
| -n        | 每个连接造数据的条数                                         |
| -len      | 造数据的value的长度                                       |
| -key-from | 造数据的key的起始递位置                                      |

例如：

~~~
./benchmark-data -h xx.xx.xx -p 19000 -a 123456 -c=SET,HSET,LPUSH,SADD,ZADD -parallel 20 -n 10000 -len 512 -key-from 1
~~~


